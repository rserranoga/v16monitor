# 🛰️ Balizas DGT V-16 Activas en Tiempo Real

Este proyecto muestra las **Balizas DGT V-16** activas, proporcionando su ubicación con OpenLayers.

## 🔗 [Ver el Mapa](https://sarah-otal.github.io/v16monitor/v16activas.html)

## 🏗️ Arquitectura del Sistema
El proyecto funciona de forma aútonoma, sin estar hospedado en GitHub.

| Component | Technology | Role |
| :--- | :--- | :--- |
| **Ingestion Engine** | R (httr, xml2) | Polls DGT XML feeds and parses IoT coordinates. |
| **Automation** | GCP Compute Engine | E2-micro instance running a 24/7 persistent loop. |
| **Hosting** | GitHub Pages | Serves the processed map to the public. |

## 🛠️ Implementación
* **Reboot Guard:** Managed via `crontab @reboot` to ensure 100% uptime.
* **Smart Sync:** Uses `git pull --rebase -X ours` to prevent merge conflicts.
* **Privacy:** Logic and historical data are stored privately on the VM.

## 👨‍💻 Autores
* **Sara Helena Otal Franco** - [@sarah-otal](https://github.com/sarah-otal), [sarah.otal@uah.es](mailto:sarah.otal@uah.es) [![Email](https://img.shields.io/badge/Email-Contact-blue?style=flat-square)](mailto:sarah.otal@uah.es)
* **Ramiro Serrano-Garcia** - [@rserranoga](https://github.com/rserranoga), [rserranoga@gmai.com](mailto:rserranoga@gmail.com)[![Email](https://img.shields.io/badge/Email-Contact-blue?style=flat-square)](mailto:rserranoga@gmail.com)

---
*Disclaimer: La exactitud de los datos depende de la frecuencia con la que los suministra el proveedor.*
