import os
import re
import pandas as pd
from sqlalchemy import create_engine

DB_URI = os.getenv(
    "AIRFLOW_CONN_POSTGRES_DEFAULT",
    "postgresql://airflow:airflow_password@localhost:5432/target_db"
)

def clean_text(text):
    if pd.isna(text):
        return ""
    text = str(text).lower().strip()
    text = re.sub(r'[^\w\s]', '', text) 
    return re.sub(r'\s+', ' ', text)  

def transform_spotify(df_spotify: pd.DataFrame) -> pd.DataFrame:
    print("SPOTIFY")
    df = df_spotify.copy()
    
    df = df.drop_duplicates(subset=["track_id"])
    
    df["duration_min"] = (df["duration_ms"] / 60000).round(2)
    df["track_clean"] = df["track_name"].apply(clean_text)
    df["artist_clean"] = df["artists"].apply(clean_text)
    
    df["artists"] = df["artists"].fillna("Unknown Artist")
    df["album_name"] = df["album_name"].fillna("Unknown Album")
    
    print(f"Spotify procesado: {len(df)} registros.")
    return df

def extract_and_transform_grammys() -> pd.DataFrame:
    print("Extracción y transformación de GRAMMYS")
    engine = create_engine(DB_URI)
    df_grammys = pd.read_sql("SELECT * FROM grammys_raw", con=engine)
    
    df_grammys = df_grammys.dropna(subset=["nominee"])
    
    df_grammys["nominee_clean"] = df_grammys["nominee"].apply(clean_text)
    df_grammys["artist_clean"] = df_grammys["artist"].apply(clean_text)
    
    df_grammys_summary = df_grammys.groupby("nominee_clean").agg(
        grammy_year=("year", "max"),
        grammy_category=("category", lambda x: " | ".join(x.unique()[:3])),
        is_grammy_winner=("winner", "any") 
    ).reset_index()
    
    print(f"Grammys transformado y consolidado: {len(df_grammys_summary)} nominados únicos.")
    return df_grammys_summary

def merge_spotify_and_grammys(df_spotify: pd.DataFrame, df_grammys: pd.DataFrame) -> pd.DataFrame:
    print("SPOTIFY AND GRAMMYS")
    
    df_merged = pd.merge(
        df_spotify,
        df_grammys,
        left_on="track_clean",
        right_on="nominee_clean",
        how="left"
    )
    
    df_merged["is_grammy_winner"] = df_merged["is_grammy_winner"].fillna(False)
    df_merged["grammy_category"] = df_merged["grammy_category"].fillna("Not Nominated")
    df_merged["grammy_year"] = df_merged["grammy_year"].fillna(-1).astype(int)
    
    df_final = df_merged.drop(columns=["track_clean", "artist_clean", "nominee_clean"], errors="ignore")
    
    print(f"Total registros: {len(df_final)}")
    print(f"Total coincidencia de ganadores o nominados de Grammy: {df_final['is_grammy_winner'].sum()}")
    
    return df_final

def run_transformations_pipeline():
    from data_quality import validate_spotify_data
    df_spotify_raw = validate_spotify_data("/opt/airflow/data/datasetspotify.csv")
    
    df_spotify_transformed = transform_spotify(df_spotify_raw)
    df_grammys_transformed = extract_and_transform_grammys()
    
    df_final = merge_spotify_and_grammys(df_spotify_transformed, df_grammys_transformed)
    
    os.makedirs("/opt/airflow/data/processed", exist_ok=True)
    df_final.to_csv("/opt/airflow/data/processed/spotify_grammys_merged.csv", index=False)
    print("Archivo guardado en /opt/airflow/data/processed/spotify_grammys_merged.csv")

if __name__ == "__main__":
    run_transformations_pipeline()