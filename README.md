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
