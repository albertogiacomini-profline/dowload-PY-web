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

## Avvio

```bash
python app.py
```

L'app gira su `http://localhost:8080`.

## Configurazione tramite variabili d'ambiente

| Variabile | Descrizione | Default |
| --- | --- | --- |
| `DOWNLOADER_DATA_DIR` | Cartella dove salvare i file JSON e il timestamp di cleanup | `./data` |
| `DOWNLOADER_BASE_PATH` | Cartella base per i download (override della config/UI) | `./data/downloads` |
| `DOWNLOADER_DATA_FILE` | Percorso file lista download | `./data/lista.json` |
| `DOWNLOADER_CONFIG_FILE` | Percorso file config | `./data/config.json` |
| `DOWNLOADER_CLEANUP_FILE` | Percorso file timestamp cleanup | `./data/cleanup.timestamp` |

Esempio:

```bash
DOWNLOADER_BASE_PATH=/mnt/anime DOWNLOADER_DATA_DIR=/var/lib/downloader python app.py
```

Puoi anche impostare la cartella base direttamente dall'interfaccia (Impostazioni → Cartella base)
senza montare cartelle NFS sulla macchina. In assenza della variabile `DOWNLOADER_BASE_PATH`
viene usato il valore salvato nella configurazione (default `./data/downloads`).

### Cartelle Samba (SMB)

Imposta la cartella base con un percorso SMB, ad esempio:

```
smb://server/condivisione/cartella
```

Inserisci eventuali credenziali nella schermata Impostazioni (SMB utente/password/dominio).
