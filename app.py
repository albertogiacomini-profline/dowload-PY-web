import os
import json
import threading
import time
import requests
import socket
from queue import Queue, Empty
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, render_template_string, redirect, url_for, session

from smb.SMBConnection import SMBConnection

app = Flask(__name__)
app.secret_key = os.environ.get("DOWNLOADER_SECRET_KEY", "change-me")

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DOWNLOADER_DATA_DIR", os.path.join(REPO_ROOT, "data"))
BASE_PATH = os.environ.get("DOWNLOADER_BASE_PATH", "/volume1/ANIME")
DATA_FILE = os.environ.get("DOWNLOADER_DATA_FILE", os.path.join(DATA_DIR, "lista.json"))
CONFIG_FILE = os.environ.get("DOWNLOADER_CONFIG_FILE", os.path.join(DATA_DIR, "config.json"))
CLEANUP_FILE = os.environ.get(
    "DOWNLOADER_CLEANUP_FILE", os.path.join(DATA_DIR, "cleanup.timestamp")
)

os.makedirs(DATA_DIR, exist_ok=True)

download_status = {}
downloading = False
next_run = None
MAX_LOG_RECORDS = 10
LOG_RETENTION_DAYS = 7
state_lock = threading.Lock()
data_lock = threading.Lock()
config = {
    "interval_minutes": 30,
    "max_threads": 3,
    "smb_host": "",
    "smb_username": "",
    "smb_password": "",
    "smb_share": "",
    "login_username": "admin",
    "login_password": "admin",
}


# ------------------------- UTILITÀ -------------------------

def load_json(path, default):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def load_config():
    global config
    if os.path.exists(CONFIG_FILE):
        cfg = load_json(CONFIG_FILE, config)
        config.update(cfg)
    else:
        save_json(CONFIG_FILE, config)


def save_config():
    save_json(CONFIG_FILE, config)


def prune_download_status():
    now = time.time()
    retention_seconds = LOG_RETENTION_DAYS * 24 * 60 * 60
    filtered_items = []
    with state_lock:
        for url, meta in download_status.items():
            finished_at = meta.get("finished_at")
            if finished_at and now - finished_at > retention_seconds:
                continue
            last_seen = finished_at or meta.get("updated_at", now)
            filtered_items.append((url, meta, last_seen))

        filtered_items.sort(key=lambda item: item[2], reverse=True)
        if len(filtered_items) > MAX_LOG_RECORDS:
            filtered_items = filtered_items[:MAX_LOG_RECORDS]

        download_status.clear()
        for url, meta, _ in filtered_items:
            download_status[url] = meta


# ------------------------- CARTELLE -------------------------

def list_subfolders(base):
    print(f"[FOLDERS] Scansione cartelle locali: base={base}")
    folders = []
    for root, dirs, files in os.walk(base):
        # Filtra cartelle nascoste o di sistema
        dirs[:] = [d for d in dirs if not d.startswith(".") and "@eaDir" not in d]
        for d in dirs:
            full_path = os.path.relpath(os.path.join(root, d), base)
            if not any(x in full_path for x in ["@eaDir", "/."]):
                folders.append(full_path)
    unique = sorted(list(set(folders)))
    print(f"[FOLDERS] Trovate {len(unique)} cartelle locali.")
    return unique


def create_smb_connection(host, username, password):
    if not host or not username:
        return None, "Config SMB incompleta."
    try:
        my_name = socket.gethostname()
        conn = SMBConnection(
            username,
            password,
            my_name,
            host,
            use_ntlm_v2=True,
            is_direct_tcp=True,
        )
        if not conn.connect(host, 445, timeout=10):
            return None, "Connessione SMB fallita."
        return conn, None
    except Exception as exc:
        return None, str(exc)


def list_smb_shares(host, username, password):
    conn, err = create_smb_connection(host, username, password)
    if err or not conn:
        return [], err
    try:
        shares = []
        for share in conn.listShares():
            if share.isSpecial:
                continue
            if share.type != 0:
                continue
            shares.append(share.name)
        return sorted(shares), None
    finally:
        conn.close()


def list_smb_subfolders(host, username, password, share, max_depth=5):
    if not share:
        return []
    print(
        "[FOLDERS] Scansione cartelle SMB:",
        f"host={host or '-'} share={share} max_depth={max_depth}",
    )
    conn, err = create_smb_connection(host, username, password)
    if err or not conn:
        print(f"[FOLDERS] Errore connessione SMB: {err}")
        return []
    folders = []
    queue = [("", 0)]
    try:
        while queue:
            current, depth = queue.pop(0)
            if current:
                print(f"[FOLDERS] SMB scan depth={depth} current='{current}'")
            if depth > max_depth:
                continue
            path = f"/{current}" if current else "/"
            try:
                entries = conn.listPath(share, path)
            except Exception as exc:
                print(
                    "[FOLDERS] Errore listPath SMB:",
                    f"path={path} err={exc}",
                )
                continue
            if current == "":
                print(f"[FOLDERS] SMB listPath root entries={len(entries)}")
            for entry in entries:
                name = entry.filename
                if name in (".", "..") or name.startswith("."):
                    continue
                if not entry.isDirectory:
                    continue
                rel = f"{current}/{name}" if current else name
                folders.append(rel)
                queue.append((rel, depth + 1))
        unique = sorted(set(folders))
        print(f"[FOLDERS] Trovate {len(unique)} cartelle SMB.")
        if not unique:
            print("[FOLDERS] Nessuna cartella SMB trovata.")
        return unique
    except Exception as exc:
        print(f"[FOLDERS] Errore durante la scansione SMB: {exc}")
        return []
    finally:
        conn.close()


def ensure_smb_dirs(conn, share, directory):
    if not directory:
        return
    parts = [p for p in directory.split("/") if p]
    current = ""
    for part in parts:
        current = f"{current}/{part}" if current else part
        try:
            conn.createDirectory(share, current)
        except Exception:
            continue


def get_smb_config():
    return {
        "host": config.get("smb_host", "").strip(),
        "username": config.get("smb_username", "").strip(),
        "password": config.get("smb_password", ""),
        "share": config.get("smb_share", "").strip(),
    }


def is_login_configured():
    return bool(config.get("login_username") or config.get("login_password"))


def is_authenticated():
    if not is_login_configured():
        return True
    return bool(session.get("logged_in"))


# ------------------------- DOWNLOAD -------------------------

def download_file(url, dest_folder, completed_list):
    try:
        smb_cfg = get_smb_config()
        use_smb = bool(smb_cfg["host"] and smb_cfg["username"] and smb_cfg["share"])
        local_filename = os.path.join(dest_folder, url.split("/")[-1])
        start_time = time.time()

        with requests.get(url, stream=True, verify=False, timeout=30) as r:
            if r.status_code == 404:
                with state_lock:
                    download_status[url] = {
                        "speed": "404 Not Found",
                        "percent": "-",
                        "updated_at": time.time(),
                        "finished_at": time.time(),
                    }
                return
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            downloaded = 0
            if use_smb:
                conn, err = create_smb_connection(
                    smb_cfg["host"],
                    smb_cfg["username"],
                    smb_cfg["password"],
                )
                if err or not conn:
                    raise RuntimeError(err or "Connessione SMB non disponibile.")
                with state_lock:
                    download_status[url] = {
                        "speed": "In corso",
                        "percent": "-",
                        "updated_at": time.time(),
                    }
                remote_dir = dest_folder.strip("/")
                remote_name = url.split("/")[-1]
                remote_path = (
                    f"{remote_dir}/{remote_name}" if remote_dir else remote_name
                )
                ensure_smb_dirs(conn, smb_cfg["share"], remote_dir)
                r.raw.decode_content = True
                conn.storeFile(smb_cfg["share"], remote_path, r.raw)
                conn.close()
            else:
                os.makedirs(dest_folder, exist_ok=True)
                with open(local_filename, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            elapsed = time.time() - start_time
                            speed = downloaded / elapsed / 1024 if elapsed > 0 else 0
                            percent = (downloaded / total * 100) if total > 0 else 0
                            with state_lock:
                                download_status[url] = {
                                    "speed": f"{speed:.1f} KB/s",
                                    "percent": f"{percent:.1f}%",
                                    "updated_at": time.time(),
                                }

        with state_lock:
            download_status[url] = {
                "speed": "Completato",
                "percent": "100%",
                "updated_at": time.time(),
                "finished_at": time.time(),
            }
        completed_list.append(url)
    except Exception as e:
        with state_lock:
            download_status[url] = {
                "speed": f"Errore: {str(e)}",
                "percent": "-",
                "updated_at": time.time(),
                "finished_at": time.time(),
            }


def worker(queue, completed_list):
    while True:
        try:
            url, dest = queue.get_nowait()
        except Empty:
            return
        try:
            download_file(url, dest, completed_list)
        finally:
            queue.task_done()


def run_downloads(force=False):
    global downloading, next_run
    with state_lock:
        if downloading:
            return
        downloading = True
    print(f"[INFO] Avvio ciclo di download {'manuale' if force else 'pianificato'}")

    with data_lock:
        data = load_json(DATA_FILE, [])
    queue = Queue()
    completed = []

    for item in data:
        url = item.get("url")
        subfolder = item.get("subfolder", "")
        if config.get("smb_share"):
            folder_path = subfolder
        else:
            folder_path = os.path.join(BASE_PATH, subfolder)
        queue.put((url, folder_path))

    threads = []
    for _ in range(config["max_threads"]):
        t = threading.Thread(target=worker, args=(queue, completed))
        threads.append(t)
        t.start()

    queue.join()
    for t in threads:
        t.join()

    if completed:
        print(f"[CLEANUP] Rimuovo {len(completed)} link completati.")
        new_data = [x for x in data if x.get("url") not in completed]
        with data_lock:
            save_json(DATA_FILE, new_data)

    with state_lock:
        downloading = False
        next_run = time.time() + config["interval_minutes"] * 60
    print("[INFO] Tutti i download completati.")


# ------------------------- PULIZIA GIORNALIERA -------------------------

def daily_cleanup():
    """Rimuove record completati da oltre 90 giorni (una volta al giorno)."""
    now = datetime.now()
    if os.path.exists(CLEANUP_FILE):
        try:
            with open(CLEANUP_FILE, "r") as f:
                last_run = datetime.fromtimestamp(float(f.read().strip()))
            if (now - last_run).days < 1:
                return
        except Exception:
            pass

    with data_lock:
        data = load_json(DATA_FILE, [])
    threshold = now - timedelta(days=90)
    cleaned = []
    for item in data:
        try:
            date_str = item.get("date", "")
            date_obj = datetime.strptime(date_str, "%b/%y")
            status = item.get("status", "").lower()
            if date_obj < threshold and ("100%" in status or "completato" in status):
                continue
            cleaned.append(item)
        except Exception:
            cleaned.append(item)

    if len(cleaned) != len(data):
        print(
            f"[CLEANUP] Rimossi {len(data) - len(cleaned)} record vecchi completati (>90 giorni)."
        )
        with data_lock:
            save_json(DATA_FILE, cleaned)

    with open(CLEANUP_FILE, "w") as f:
        f.write(str(now.timestamp()))


# ------------------------- BACKGROUND -------------------------

def background_scheduler():
    global next_run
    time.sleep(5)
    if not next_run:
        with state_lock:
            next_run = time.time() + config["interval_minutes"] * 60
    while True:
        daily_cleanup()
        with state_lock:
            should_run = not downloading and next_run and time.time() >= next_run
        if should_run:
            run_downloads(force=False)
        time.sleep(60)


threading.Thread(target=background_scheduler, daemon=True).start()


@app.before_request
def require_login():
    if request.path in ("/login", "/logout"):
        return None
    if request.path.startswith("/static"):
        return None
    if not is_authenticated():
        return redirect(url_for("login", next=request.path))
    return None


# ------------------------- API -------------------------

@app.route("/api/status")
def api_status():
    with state_lock:
        remaining = max(0, int(next_run - time.time())) if next_run else 0
        active = dict(download_status)
        is_downloading = downloading
    prune_download_status()
    return jsonify(
        {
            "downloading": is_downloading,
            "next_run": remaining,
            "active": active,
        }
    )


@app.route("/api/list")
def api_list():
    with data_lock:
        return jsonify(load_json(DATA_FILE, []))


@app.route("/api/add", methods=["POST"])
def api_add():
    with data_lock:
        data = load_json(DATA_FILE, [])
    new_item = request.json
    new_item["date"] = datetime.now().strftime("%b/%y")
    data.append(new_item)
    with data_lock:
        save_json(DATA_FILE, data)
    return jsonify({"ok": True})


@app.route("/api/delete", methods=["POST"])
def api_delete():
    index = request.json.get("index")
    with data_lock:
        data = load_json(DATA_FILE, [])
    if 0 <= index < len(data):
        del data[index]
    with data_lock:
        save_json(DATA_FILE, data)
    return jsonify({"ok": True})


@app.route("/api/update", methods=["POST"])
def api_update():
    i = request.json.get("index")
    with data_lock:
        data = load_json(DATA_FILE, [])
    if 0 <= i < len(data):
        data[i]["url"] = request.json.get("url")
        data[i]["subfolder"] = request.json.get("subfolder")
    with data_lock:
        save_json(DATA_FILE, data)
    return jsonify({"ok": True})


@app.route("/api/force", methods=["POST"])
def api_force():
    threading.Thread(target=run_downloads, args=(True,), daemon=True).start()
    return jsonify({"ok": True})


@app.route("/api/config", methods=["GET", "POST"])
def api_config():
    if request.method == "POST":
        body = request.json
        config["interval_minutes"] = body.get("interval_minutes", config["interval_minutes"])
        config["max_threads"] = body.get("max_threads", config["max_threads"])
        config["smb_host"] = body.get("smb_host", config["smb_host"])
        config["smb_username"] = body.get("smb_username", config["smb_username"])
        config["smb_password"] = body.get("smb_password", config["smb_password"])
        config["smb_share"] = body.get("smb_share", config["smb_share"])
        config["login_username"] = body.get("login_username", config["login_username"])
        config["login_password"] = body.get("login_password", config["login_password"])
        save_config()
        return jsonify({"ok": True})
    return jsonify(config)


@app.route("/api/smb/shares", methods=["POST"])
def api_smb_shares():
    body = request.json or {}
    host = body.get("smb_host", config.get("smb_host", ""))
    username = body.get("smb_username", config.get("smb_username", ""))
    password = body.get("smb_password", config.get("smb_password", ""))
    shares, err = list_smb_shares(host, username, password)
    return jsonify({"shares": shares, "error": err})


@app.route("/api/folders")
def api_folders():
    smb_cfg = get_smb_config()
    if smb_cfg["host"] and smb_cfg["username"] and smb_cfg["share"]:
        print("[FOLDERS] Richiesta cartelle via SMB.")
        folders = list_smb_subfolders(
            smb_cfg["host"],
            smb_cfg["username"],
            smb_cfg["password"],
            smb_cfg["share"],
        )
    else:
        print("[FOLDERS] Richiesta cartelle locali.")
        folders = list_subfolders(BASE_PATH)
    preview = ", ".join(folders[:5])
    print(f"[FOLDERS] Restituite {len(folders)} cartelle. Prime: {preview}")
    return jsonify({"folders": folders})


# ------------------------- UI -------------------------

@app.route("/")
def home():
    data = load_json(DATA_FILE, [])
    base_label = config.get("smb_share") or BASE_PATH
    html = """
    <html><head>
    <title>Downloader UI</title>
    <style>
      body { font-family: Arial, Helvetica, sans-serif; background: #111; color: #ddd; margin:0; padding:20px; }
      .top { display: flex; justify-content: space-between; align-items: center; background:#1d1d1d; padding:12px 16px; border-radius:10px; position:sticky; top:0; z-index:5; }
      .btn { background:#3a3a3a; color:#fff; border:1px solid #555; padding:8px 14px; border-radius:6px; cursor:pointer; }
      .btn:hover { background:#4a4a4a; }
      .btn-sm { padding:4px 10px; font-size: 13px; }
      .btn-row { display:inline-flex; gap:8px; align-items:center; }
      .section { margin-top:18px; }
      table { width:100%; border-collapse:collapse; margin-top:10px; }
      td,th { padding:10px; border-bottom:1px solid #2a2a2a; vertical-align:top; }
      th { text-align:left; color:#bbb; font-weight:600; }
      input,select { background:#1b1b1b; color:#fff; border:1px solid #444; padding:6px 8px; border-radius:6px; }
      input:focus { outline: none; border-color:#666; }
      #overlay,#configOverlay { position:fixed; top:0; left:0; width:100%; height:100%; background:rgba(0,0,0,0.7); display:none; align-items:center; justify-content:center; z-index:100; }
      .modal { background:#1f1f1f; padding:20px; border-radius:10px; min-width: 520px; max-width: 90vw; }
      .row { display:flex; gap:10px; align-items:center; flex-wrap: wrap; }
      #statusBox { line-height:1.5; }
      .muted { color:#999; }
      .suggestion-wrap { position: relative; display: inline-block; min-width:240px; }
      .suggestions { position:absolute; top:100%; left:0; right:0; background:#1f1f1f; border:1px solid #333; border-radius:6px; margin-top:4px; max-height:220px; overflow:auto; z-index:200; display:none; box-shadow:0 6px 14px rgba(0,0,0,0.45); }
      .suggestions button { display:block; width:100%; text-align:left; background:none; border:0; color:#ddd; padding:6px 10px; cursor:pointer; font-size:13px; }
      .suggestions button:hover { background:#2a2a2a; }
    </style>
    </head><body>

    <div class="top">
      <div id="statusBox">Stato: ...</div>
      <div class="btn-row">
        <button class="btn" onclick="force()">Forza download</button>
        <button class="btn" onclick="openConfig()">Impostazioni</button>
        <a class="btn" href="/logout">Esci</a>
      </div>
    </div>

    <div class="section">
      <h3>Aggiungi download</h3>
      <div class="row" style="gap:12px;">
        <label>URL base:</label>
        <input id="url" size="60" placeholder="...Ep_01_SUB_ITA.mp4">
        <label>Cartella:</label>
        <span class="suggestion-wrap">
          <input id="subfolder" list="subfolderOptions" style="min-width:240px;">
          <datalist id="subfolderOptions"></datalist>
          <div id="subfolderSuggest" class="suggestions"></div>
        </span>
        <label>Numero episodi:</label>
        <input id="episodes" type="number" min="1" value="1" style="width:90px">
        <button class="btn" onclick="add()">Aggiungi</button>
      </div>
      <div class="muted" style="margin-top:6px;">Suggerimenti cartella presi da {{ base }} (solo directory, niente @eaDir).</div>
    </div>

    <div class="section">
      <h3>Lista download</h3>
      <table id="list"></table>
    </div>

    <!-- Modale Modifica -->
    <div id="overlay"><div class="modal">
      <h3>Modifica record</h3>
      <div style="margin:8px 0;">URL:</div>
      <input id="editUrl" size="80">
      <div style="margin:8px 0;">Cartella:</div>
      <span class="suggestion-wrap">
        <input id="editSub" list="editSubOptions" style="min-width:240px;">
        <datalist id="editSubOptions"></datalist>
        <div id="editSubSuggest" class="suggestions"></div>
      </span>
      <div style="margin-top:16px;" class="btn-row">
        <button class="btn" onclick="saveEdit()">Salva</button>
        <button class="btn" onclick="closeOverlay()">Annulla</button>
      </div>
    </div></div>

    <!-- Modale Config -->
    <div id="configOverlay"><div class="modal">
      <h3>Impostazioni</h3>
      <div class="row" style="margin-top:10px;">
        <label>IP/Host SMB:</label>
        <input id="smbHost" placeholder="192.168.1.10" style="width:200px">
      </div>
      <div class="row" style="margin-top:10px;">
        <label>Utente SMB:</label>
        <input id="smbUser" placeholder="utente" style="width:200px">
      </div>
      <div class="row" style="margin-top:10px;">
        <label>Password SMB:</label>
        <input id="smbPass" type="password" placeholder="password" style="width:200px">
      </div>
      <div class="row" style="margin-top:10px;">
        <label>Login utente:</label>
        <input id="loginUser" placeholder="admin" style="width:200px">
      </div>
      <div class="row" style="margin-top:10px;">
        <label>Login password:</label>
        <input id="loginPass" type="password" placeholder="password" style="width:200px">
      </div>
      <div class="row" style="margin-top:10px;">
        <label>Cartella SMB:</label>
        <select id="smbShare" style="min-width:240px;"></select>
        <button class="btn btn-sm" onclick="refreshShares()">Aggiorna</button>
      </div>
      <div class="row" style="margin-top:10px;">
        <label>Ciclo (minuti):</label>
        <input id="interval" type="number" min="1" style="width:120px">
      </div>
      <div class="row" style="margin-top:10px;">
        <label>Thread max:</label>
        <input id="threads" type="number" min="1" max="10" style="width:120px">
      </div>
      <div style="margin-top:16px;" class="btn-row">
        <button class="btn" onclick="saveConfig()">Salva</button>
        <button class="btn" onclick="closeConfig()">Chiudi</button>
      </div>
    </div></div>

    <script>
    let editIndex = -1;
    let folderSuggestions = [];
    const MAX_SUGGESTIONS = 8;

    function fileNameFromUrl(u){
      try{ return u.split('/').pop(); } catch(e){ return u; }
    }

    // --------- NUOVA LOGICA GENERAZIONE EPISODI (2-3 CIFRE, DA EP DI PARTENZA) ----------
    function generateEpisodeUrls(baseUrl, episodes){
  const re = /(.*?)(\d{2,3})([^0-9]*)(\.[a-zA-Z0-9]+)$/;

  const m = baseUrl.match(re);
  if(!m){
    return [baseUrl];
  }

  const prefix = m[1];
  const numStr = m[2];
  const mid    = m[3];
  const ext    = m[4];
  const pad    = numStr.length;
  const start  = parseInt(numStr);

  const out = [];
  for(let i=0;i<episodes;i++){
    const n = String(start + i).padStart(pad, "0");
    out.push(prefix + n + mid + ext);
  }
  return out;
}


    async function refreshList(){
      const r = await fetch('/api/list');
      const j = await r.json();
      let html = '<tr><th>#</th><th>Data</th><th>URL</th><th>Cartella</th><th>Azioni</th></tr>';
      j.forEach((x,i)=>{
        const urlDisp = x.url || '';
        const subDisp = x.subfolder || '';
        html += `
          <tr>
            <td>${i}</td>
            <td>${x.date||''}</td>
            <td style="word-break:break-all;">${urlDisp}</td>
            <td>${subDisp}</td>
            <td>
              <span class="btn-row">
                <button class="btn btn-sm" onclick="edit(${i})">Modifica</button>
                <button class="btn btn-sm" onclick="del(${i})">Elimina</button>
              </span>
            </td>
          </tr>`;
      });
      document.getElementById('list').innerHTML = html;
    }

    async function refreshStatus(){
      const r = await fetch('/api/status');
      const j = await r.json();
      let txt = j.downloading ? "Download in corso" : "In attesa";
      let s = j.next_run || 0;
      let mins = Math.floor(s/60), secs = s%60;
      txt += " | Prossimo ciclo tra " + String(mins).padStart(2,'0') + "m " + String(secs).padStart(2,'0') + "s";
      if(Object.keys(j.active||{}).length){
        txt += "<br>";
        for(let k in j.active){
          const v = j.active[k] || {};
          if(v.speed && v.speed.indexOf('404') === -1){
            txt += "<div>" + fileNameFromUrl(k) + ": " + (v.percent||'-') + " (" + (v.speed||'-') + ")</div>";
          }
        }
      }
      document.getElementById('statusBox').innerHTML = txt;
    }

    async function add(){
      const url = (document.getElementById('url').value||'').trim();
      const sub = (document.getElementById('subfolder').value||'').trim();
      const episodes = parseInt(document.getElementById('episodes').value) || 1;

      if(!url){ alert("Compila URL."); return; }

      const urls = generateEpisodeUrls(url, episodes);

      for(const fullUrl of urls){
        await fetch('/api/add',{
          method:'POST',
          headers:{'Content-Type':'application/json'},
          body:JSON.stringify({url:fullUrl, subfolder:sub})
        });
      }

      document.getElementById('url').value = '';
      document.getElementById('episodes').value = 1;
      refreshList();
    }

    async function del(i){
      await fetch('/api/delete',{
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({index:i})
      });
      refreshList();
    }

    async function edit(i){
      const r = await fetch('/api/list');
      const j = await r.json();
      if(i<0 || i>=j.length) return;
      editIndex = i;
      document.getElementById('editUrl').value = j[i].url || '';
      await populateFolderSelect('editSub', 'editSubOptions', j[i].subfolder || '');
      document.getElementById('overlay').style.display = 'flex';
    }

    function closeOverlay(){ document.getElementById('overlay').style.display = 'none'; }

    async function saveEdit(){
      const url = document.getElementById('editUrl').value;
      const sub = document.getElementById('editSub').value;
      await fetch('/api/update',{
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({index:editIndex, url, subfolder:sub})
      });
      closeOverlay();
      refreshList();
    }

    async function force(){ await fetch('/api/force',{method:'POST'}); }

    function openConfig(){
      fetch('/api/config').then(r=>r.json()).then(cfg=>{
        document.getElementById('interval').value = cfg.interval_minutes;
        document.getElementById('threads').value = cfg.max_threads;
        document.getElementById('smbHost').value = cfg.smb_host || '';
        document.getElementById('smbUser').value = cfg.smb_username || '';
        document.getElementById('smbPass').value = cfg.smb_password || '';
        document.getElementById('loginUser').value = cfg.login_username || '';
        document.getElementById('loginPass').value = cfg.login_password || '';
        refreshShares(cfg.smb_share || '');
        document.getElementById('configOverlay').style.display = 'flex';
      });
    }
    async function saveConfig(){
      const interval = parseInt(document.getElementById('interval').value);
      const threads = parseInt(document.getElementById('threads').value);
      const smbHost = document.getElementById('smbHost').value.trim();
      const smbUser = document.getElementById('smbUser').value.trim();
      const smbPass = document.getElementById('smbPass').value;
      const loginUser = document.getElementById('loginUser').value.trim();
      const loginPass = document.getElementById('loginPass').value;
      const smbShare = document.getElementById('smbShare').value;
      await fetch('/api/config',{
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({
          interval_minutes:interval,
          max_threads:threads,
          smb_host:smbHost,
          smb_username:smbUser,
          smb_password:smbPass,
          smb_share:smbShare,
          login_username:loginUser,
          login_password:loginPass
        })
      });
      closeConfig();
      await populateFolderSelect('subfolder', 'subfolderOptions');
    }
    function closeConfig(){ document.getElementById('configOverlay').style.display = 'none'; }

    async function refreshShares(selected){
      const smbHost = document.getElementById('smbHost').value.trim();
      const smbUser = document.getElementById('smbUser').value.trim();
      const smbPass = document.getElementById('smbPass').value;
      const r = await fetch('/api/smb/shares',{
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({smb_host:smbHost, smb_username:smbUser, smb_password:smbPass})
      });
      const j = await r.json();
      const select = document.getElementById('smbShare');
      select.innerHTML = '';
      const placeholder = document.createElement('option');
      placeholder.value = '';
      placeholder.textContent = j.error ? 'Errore SMB' : '-- Seleziona cartella --';
      select.appendChild(placeholder);
      (j.shares||[]).forEach(name=>{
        const opt = document.createElement('option');
        opt.value = name;
        opt.textContent = name;
        select.appendChild(opt);
      });
      if(selected){
        select.value = selected;
      }
    }

    function renderSuggestions(inputId, suggestId){
      const input = document.getElementById(inputId);
      const suggestBox = document.getElementById(suggestId);
      const query = (input.value || '').toLowerCase();
      const matches = folderSuggestions.filter(folder => folder.toLowerCase().includes(query));
      const limited = matches.slice(0, MAX_SUGGESTIONS);
      suggestBox.innerHTML = '';
      if(!limited.length){
        suggestBox.style.display = 'none';
        return;
      }
      limited.forEach(folder => {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.textContent = folder;
        btn.addEventListener('click', () => {
          input.value = folder;
          suggestBox.style.display = 'none';
        });
        suggestBox.appendChild(btn);
      });
      suggestBox.style.display = 'block';
    }

    function bindSuggestionInput(inputId, suggestId){
      const input = document.getElementById(inputId);
      const suggestBox = document.getElementById(suggestId);
      if(!input || !suggestBox) return;
      suggestBox.addEventListener('mousedown', (event) => {
        event.preventDefault();
      });
      input.addEventListener('input', () => renderSuggestions(inputId, suggestId));
      input.addEventListener('focus', () => renderSuggestions(inputId, suggestId));
      input.addEventListener('blur', () => {
        setTimeout(() => { suggestBox.style.display = 'none'; }, 150);
      });
    }

    async function populateFolderSelect(inputId, listId, selectedValue){
      const r = await fetch('/api/folders');
      const j = await r.json();
      folderSuggestions = j.folders || [];
      const list = document.getElementById(listId);
      list.innerHTML = '';
      folderSuggestions.forEach(folder=>{
        const opt = document.createElement('option');
        opt.value = folder;
        list.appendChild(opt);
      });
      if(selectedValue){
        const input = document.getElementById(inputId);
        input.value = selectedValue;
      }
      renderSuggestions('subfolder', 'subfolderSuggest');
      renderSuggestions('editSub', 'editSubSuggest');
    }

    bindSuggestionInput('subfolder', 'subfolderSuggest');
    bindSuggestionInput('editSub', 'editSubSuggest');

    refreshList();
    refreshStatus();
    populateFolderSelect('subfolder', 'subfolderOptions');
    setInterval(refreshStatus, 1000);
    </script>
    </body></html>
    """
    return render_template_string(html, data=data, base=base_label)


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        if username == config.get("login_username") and password == config.get("login_password"):
            session["logged_in"] = True
            next_url = request.args.get("next") or url_for("home")
            return redirect(next_url)
        error = "Credenziali non valide."
    html = """
    <html><head>
    <title>Login</title>
    <style>
      body { font-family: Arial, Helvetica, sans-serif; background:#111; color:#ddd; margin:0; height:100vh; display:flex; align-items:center; justify-content:center; }
      .card { background:#1d1d1d; padding:24px; border-radius:12px; min-width:320px; box-shadow: 0 10px 25px rgba(0,0,0,0.4); }
      input { width:100%; margin-top:6px; background:#1b1b1b; color:#fff; border:1px solid #444; padding:8px 10px; border-radius:6px; }
      label { display:block; margin-top:12px; color:#aaa; font-size:14px; }
      .btn { margin-top:16px; width:100%; background:#3a3a3a; color:#fff; border:1px solid #555; padding:10px 14px; border-radius:6px; cursor:pointer; }
      .btn:hover { background:#4a4a4a; }
      .error { margin-top:12px; color:#ff8b8b; font-size:14px; }
    </style>
    </head><body>
      <form class="card" method="post">
        <h2>Accedi</h2>
        <label>Username</label>
        <input name="username" autocomplete="username" required>
        <label>Password</label>
        <input type="password" name="password" autocomplete="current-password" required>
        <button class="btn" type="submit">Entra</button>
        {% if error %}<div class="error">{{ error }}</div>{% endif %}
      </form>
    </body></html>
    """
    return render_template_string(html, error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ------------------------- MAIN -------------------------

if __name__ == "__main__":
    load_config()
    app.run(host="0.0.0.0", port=8080)
