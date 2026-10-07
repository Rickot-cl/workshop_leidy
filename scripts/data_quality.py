import os
import pandas as pd
import pandera as pa
from pandera import Column, Check, DataFrameSchema

spotify_schema = DataFrameSchema(
    columns={
        "track_id": Column(pa.String, nullable=False),
        "popularity": Column(pa.Int, Check.in_range(0, 100), nullable=False),
        "duration_ms": Column(pa.Int, Check.greater_than_or_equal_to(0), nullable=False),
        "explicit": Column(pa.Bool, nullable=False),
        "danceability": Column(pa.Float, Check.in_range(0.0, 1.0), nullable=True),
        "energy": Column(pa.Float, Check.in_range(0.0, 1.0), nullable=True),
        "loudness": Column(pa.Float, nullable=True),
        "valence": Column(pa.Float, Check.in_range(0.0, 1.0), nullable=True),
    },
    coerce=True,
    strict=False
)

def validate_spotify_data(file_path: str) -> pd.DataFrame:

    print("INICIANDO VALIDACIÓN CON PANDERA")
    if not os.path.exists(file_path):
        file_path = "/opt/airflow/data/datasetspotify.csv"

    df = pd.read_csv(file_path)
    
    df = df.dropna(subset=["track_id", "artists", "track_name"])
    
    try:
        validated_df = spotify_schema.validate(df)
        print("El dataset de Spotify cumple con todas las reglas del contrato.")
        return validated_df
    except pa.errors.SchemaError as err:
        print("El dataset viola las reglas de calidad.")
        print(err.failure_cases)
        raise err

if __name__ == "__main__":
    df_valid = validate_spotify_data("data/datasetspotify.csv")