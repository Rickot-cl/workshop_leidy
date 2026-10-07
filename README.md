# Workshop 02:Pipeline ETL Automatizado Spotify & Grammys con Apache Airflow

## Descripción del Proyecto
Este proyecto implementa un pipeline ETL modular y automatizado utilizando **Apache Airflow**, **Pandera**, **PostgreSQL** y **Docker Compose**. 
El objetivo es extraer datasets heterogéneos (Spotify desde CSV y Grammys desde PostgreSQL), validar contratos de calidad de datos, aplicar transformaciones de enriquecimiento, realizar un merge relacional y cargar la capa analítica en PostgreSQL para alimentar un dashboard estático.

---

## Arquitectura de Infraestructura (Docker)
El entorno funciona sobre dos contenedores aislados comunicados mediante una red privada de Docker:
* `postgres_db` (PostgreSQL 15): Almacena la tabla origen `grammys_raw` y la tabla final `spotify_grammys_analytics`.
* `airflow_runner` (Apache Airflow 2.8.1 - Python 3.10): Orquestador standalone que ejecuta las DAGs y scripts Python.

---

## Estructura del Proyecto
```text
workshop_etl/
├── dags/
│   └── spotify_grammys.py      
├── data/
│   ├── datasetspotify.csv                 
│   ├── the_grammys_awards.csv                
│   └── processed/                  
├── scripts/
│   ├── data_analyze.py        
│   ├── data_quality.py            
│   ├── transformation.py       
│   └── reports.py          
├── reports/
│   └── dashboard.png              
├── docker-compose.yml              
├── requirements.txt                
└── README.md                       
