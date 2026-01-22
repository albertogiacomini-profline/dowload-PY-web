import os
import json
import threading
import time
import requests
import subprocess
from queue import Queue, Empty
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, render_template_string

app = Flask(__name__)

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
    "smb_share": "",
    "smb_username": "",
    "smb_password": "",
    "smb_domain": "",
    "smb_port": 445,
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
    folders = []
    for root, dirs, files in os.walk(base):
        # Filtra cartelle nascoste o di sistema
        dirs[:] = [d for d in dirs if not d.startswith(".") and "@eaDir" not in d]
        for d in dirs:
            full_path = os.path.relpath(os.path.join(root, d), base)
            if not any(x in full_path for x in ["@eaDir", "/."]):
                folders.append(full_path)
    return sorted(list(set(folders)))


# ------------------------- DOWNLOAD -------------------------

def download_file(url, dest_folder, completed_list):
    try:
        os.makedirs(dest_folder, exist_ok=True)
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
        config["smb_share"] = body.get("smb_share", config["smb_share"])
        config["smb_username"] = body.get("smb_username", config["smb_username"])
        config["smb_password"] = body.get("smb_password", config["smb_password"])
        config["smb_domain"] = body.get("smb_domain", config["smb_domain"])
        config["smb_port"] = body.get("smb_port", config["smb_port"])
        save_config()
        return jsonify({"ok": True})
    return jsonify(config)


@app.route("/api/smb/test", methods=["POST"])
def api_smb_test():
    payload = request.json or {}
    host = (payload.get("host") or "").strip()
    share = (payload.get("share") or "").strip()
    username = (payload.get("username") or "").strip()
    password = payload.get("password") or ""
    domain = (payload.get("domain") or "").strip()
    port = int(payload.get("port") or 445)

    if not host or not share:
        return jsonify({"ok": False, "message": "Host e share sono obbligatori."})

    if domain and username and "\\" not in username and "@" not in username:
        username = f"{domain}\\{username}"

    try:
        cmd = ["smbclient", f"//{host}/{share}", "-c", "ls", "-p", str(port)]
        if domain:
            cmd.extend(["-W", domain])
        if username or password:
            cmd.extend(["-U", f"{username}%{password}"])
        else:
            cmd.append("-N")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        if result.returncode != 0:
            error_msg = (result.stderr or result.stdout or "").strip()
            return jsonify(
                {
                    "ok": False,
                    "message": f"Errore: {error_msg or 'test SMB fallito'}",
                }
            )

        entries = []
        for line in result.stdout.splitlines():
            if not line.startswith("  "):
                continue
            name = line.strip().split()[0]
            if name in {".", ".."}:
                continue
            entries.append(line)
        count = len(entries)
        message = f"Connessione OK. {count} elementi trovati." if count else "Connessione OK."
        return jsonify({"ok": True, "message": message})
    except Exception as exc:
        return jsonify({"ok": False, "message": f"Errore: {exc}"})


# ------------------------- UI -------------------------

@app.route("/")
def home():
    data = load_json(DATA_FILE, [])
    folders = list_subfolders(BASE_PATH)
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
      .status-line { margin-top:8px; color:#c9c9c9; }
      .status-line.ok { color:#6bd16b; }
      .status-line.err { color:#ff6b6b; }
    </style>
    </head><body>

    <div class="top">
      <div id="statusBox">Stato: ...</div>
      <div class="btn-row">
        <button class="btn" onclick="force()">Forza download</button>
        <button class="btn" onclick="openConfig()">Impostazioni</button>
      </div>
    </div>

    <div class="section">
      <h3>Aggiungi download</h3>
      <div class="row" style="gap:12px;">
        <label>URL base:</label>
        <input id="url" size="60" placeholder="...Ep_01_SUB_ITA.mp4">
        <label>Cartella:</label>
        <input id="subfolder" list="folders" size="40" placeholder="Serie/S01">
        <label>Numero episodi:</label>
        <input id="episodes" type="number" min="1" value="1" style="width:90px">
        <button class="btn" onclick="add()">Aggiungi</button>
      </div>
      <div class="muted" style="margin-top:6px;">Suggerimenti cartella presi da {{ base }} (solo directory, niente @eaDir).</div>
      <datalist id="folders">
        {% for f in folders %}
          <option value="{{f}}">
        {% endfor %}
      </datalist>
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
      <input id="editSub" size="60" list="folders">
      <div style="margin-top:16px;" class="btn-row">
        <button class="btn" onclick="saveEdit()">Salva</button>
        <button class="btn" onclick="closeOverlay()">Annulla</button>
      </div>
    </div></div>

    <!-- Modale Config -->
    <div id="configOverlay"><div class="modal">
      <h3>Impostazioni</h3>
      <div class="row" style="margin-top:10px;">
        <label>Ciclo (minuti):</label>
        <input id="interval" type="number" min="1" style="width:120px">
      </div>
      <div class="row" style="margin-top:10px;">
        <label>Thread max:</label>
        <input id="threads" type="number" min="1" max="10" style="width:120px">
      </div>
      <div style="margin-top:16px;">
        <h4>Connessione SMB</h4>
        <div class="row" style="margin-top:10px;">
          <label>Host:</label>
          <input id="smbHost" type="text" style="width:180px" placeholder="server.local">
          <label>Share:</label>
          <input id="smbShare" type="text" style="width:180px" placeholder="cartella">
        </div>
        <div class="row" style="margin-top:10px;">
          <label>Dominio:</label>
          <input id="smbDomain" type="text" style="width:140px" placeholder="WORKGROUP">
          <label>Utente:</label>
          <input id="smbUser" type="text" style="width:160px" placeholder="utente">
          <label>Password:</label>
          <input id="smbPass" type="password" style="width:180px">
        </div>
        <div class="row" style="margin-top:10px;">
          <label>Porta:</label>
          <input id="smbPort" type="number" min="1" max="65535" style="width:120px">
          <button class="btn btn-sm" onclick="testSmb()">Test connessione</button>
        </div>
        <div id="smbStatus" class="status-line"></div>
      </div>
      <div style="margin-top:16px;" class="btn-row">
        <button class="btn" onclick="saveConfig()">Salva</button>
        <button class="btn" onclick="closeConfig()">Chiudi</button>
      </div>
    </div></div>

    <script>
    let editIndex = -1;

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

      if(!url || !sub){ alert("Compila URL e Cartella."); return; }

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
      document.getElementById('editSub').value = j[i].subfolder || '';
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
        document.getElementById('smbShare').value = cfg.smb_share || '';
        document.getElementById('smbDomain').value = cfg.smb_domain || '';
        document.getElementById('smbUser').value = cfg.smb_username || '';
        document.getElementById('smbPass').value = cfg.smb_password || '';
        document.getElementById('smbPort').value = cfg.smb_port || 445;
        document.getElementById('smbStatus').textContent = '';
        document.getElementById('smbStatus').className = 'status-line';
        document.getElementById('configOverlay').style.display = 'flex';
      });
    }
    async function saveConfig(){
      const interval = parseInt(document.getElementById('interval').value);
      const threads = parseInt(document.getElementById('threads').value);
      const smb_host = document.getElementById('smbHost').value.trim();
      const smb_share = document.getElementById('smbShare').value.trim();
      const smb_domain = document.getElementById('smbDomain').value.trim();
      const smb_username = document.getElementById('smbUser').value.trim();
      const smb_password = document.getElementById('smbPass').value;
      const smb_port = parseInt(document.getElementById('smbPort').value) || 445;
      await fetch('/api/config',{
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({
          interval_minutes:interval,
          max_threads:threads,
          smb_host,
          smb_share,
          smb_domain,
          smb_username,
          smb_password,
          smb_port
        })
      });
      closeConfig();
    }

    async function testSmb(){
      const payload = {
        host: document.getElementById('smbHost').value.trim(),
        share: document.getElementById('smbShare').value.trim(),
        domain: document.getElementById('smbDomain').value.trim(),
        username: document.getElementById('smbUser').value.trim(),
        password: document.getElementById('smbPass').value,
        port: parseInt(document.getElementById('smbPort').value) || 445
      };
      const statusEl = document.getElementById('smbStatus');
      statusEl.textContent = 'Test in corso...';
      statusEl.className = 'status-line';
      try{
        const r = await fetch('/api/smb/test',{
          method:'POST',
          headers:{'Content-Type':'application/json'},
          body:JSON.stringify(payload)
        });
        const j = await r.json();
        statusEl.textContent = j.message || 'Risposta non valida.';
        statusEl.className = j.ok ? 'status-line ok' : 'status-line err';
      }catch(e){
        statusEl.textContent = 'Errore durante il test.';
        statusEl.className = 'status-line err';
      }
    }
    function closeConfig(){ document.getElementById('configOverlay').style.display = 'none'; }

    refreshList();
    refreshStatus();
    setInterval(refreshStatus, 1000);
    </script>
    </body></html>
    """
    return render_template_string(html, data=data, folders=folders, base=BASE_PATH)


# ------------------------- MAIN -------------------------

if __name__ == "__main__":
    load_config()
    app.run(host="0.0.0.0", port=8080)
