# Downloader UI

Applicazione Flask per gestire una coda di download con UI web e scheduler.

## Requisiti

- Python 3.10+

## Setup rapido

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Installazione da zero su container Proxmox (Debian 13)

Usa lo script in `scripts/install_proxmox_debian13.sh`, eseguendolo come root
nel container e sostituendo `<URL_REPOSITORY>` con l'URL del repository.
Lo script installa le dipendenze, crea l'ambiente virtuale e registra il servizio
`downloader-ui.service` per avviare l'app automaticamente.

```bash
bash scripts/install_proxmox_debian13.sh
```

## Avvio

```bash
python app.py
```

L'app gira su `http://localhost:8080`.

## Configurazione tramite variabili d'ambiente

| Variabile | Descrizione | Default |
| --- | --- | --- |
| `DOWNLOADER_DATA_DIR` | Cartella dove salvare i file JSON e il timestamp di cleanup | `./data` |
| `DOWNLOADER_BASE_PATH` | Cartella base per i download | `/volume1/ANIME` |
| `DOWNLOADER_DATA_FILE` | Percorso file lista download | `./data/lista.json` |
| `DOWNLOADER_CONFIG_FILE` | Percorso file config | `./data/config.json` |
| `DOWNLOADER_CLEANUP_FILE` | Percorso file timestamp cleanup | `./data/cleanup.timestamp` |

Esempio:

```bash
DOWNLOADER_BASE_PATH=/mnt/anime DOWNLOADER_DATA_DIR=/var/lib/downloader python app.py
```


## Hardening operativo

L'app è pensata per uso privato/LAN o dietro Cloudflare Access. Le POST della UI usano
un token CSRF di sessione e `/api/config` non restituisce più password in chiaro al
frontend: i campi password vuoti mantengono il valore già configurato.

La secret key Flask viene letta da `DOWNLOADER_SECRET_KEY`; se assente viene generata
e conservata in `DOWNLOADER_SECRET_KEY_FILE` (default: `DATA_DIR/secret_key`).

Le sottocartelle sono normalizzate come path relativi e non accettano path assoluti o
segmenti `.`/`..`, per evitare scritture fuori dalla destinazione prevista.

Per installazioni persistenti è preferibile eseguire il servizio con `gunicorn` e un
utente dedicato non-root, come nello script `scripts/install_proxmox_debian13.sh`.
