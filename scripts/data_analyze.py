import os
import pandas as pd
from sqlalchemy import create_engine

DB = os.getenv(
    "AIRFLOW_CONN_POSTGRES_DEFAULT",
    "postgresql://airflow:airflow_password@localhost:5432/target_db"
)

def grammys_db():
    grammys = "data/the_grammy_awards.csv"
    
    if not os.path.exists(grammys):
        raise FileNotFoundError(f"No se encontró el archivo en {grammys}")
        
    df_grammys = pd.read_csv(grammys)
    engine = create_engine(DB)
    
    df_grammys.to_sql("grammys_raw", con=engine, if_exists="replace", index=False)
    print("Dataset cargado en PostgreSQL.\n")

def analyze_datasets():
  
    df_spotify = pd.read_csv("data/datasetspotify.csv")
    print("\nDataset Spotify")
    print(f"Dimensiones: {df_spotify.shape}")
    print("Columnas y tipos:")
    print(df_spotify.dtypes)
    print("\nValores Nulos por Columna:")
    print(df_spotify.isnull().sum()[df_spotify.isnull().sum() > 0])
    print(f"Filas duplicadas: {df_spotify.duplicated().sum()}")
    print("\nMuestra Spotify:")
    print(df_spotify.head(2))
    
    engine = create_engine(DB)
    df_grammys = pd.read_sql("SELECT * FROM grammys_raw", con=engine)
    print("GRAMMYS DATASET")
    print(f"Dimensiones: {df_grammys.shape}")
    print("Columnas y tipos:")
    print(df_grammys.dtypes)
    print("\nValores Nulos por Columna:")
    print(df_grammys.isnull().sum()[df_grammys.isnull().sum() > 0])
    print(f"Filas duplicadas: {df_grammys.duplicated().sum()}")
    print("\nMuestra Grammys:")
    print(df_grammys.head(2))

if __name__ == "__main__":
    grammys_db()
    analyze_datasets()