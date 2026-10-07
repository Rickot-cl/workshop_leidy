import os
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sqlalchemy import create_engine

DB_URI = os.getenv(
    "AIRFLOW_CONN_POSTGRES_DEFAULT",
    "postgresql://airflow:airflow_password@localhost:5432/target_db",
)


def generate_static_dashboard():
    print("CONSULTANDO DATOS")
    engine = create_engine(DB_URI)

    query = """
    SELECT track_name, artists, popularity, duration_min, track_genre, is_grammy_winner, grammy_category
    FROM spotify_grammys_analytics;
    """
    df = pd.read_sql(query, con=engine)
    print(
        f"Datos consultados desde PostgreSQL: {len(df)} registros."
    )

    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle(
        "Impacto de Grammys en Popularidad de Spotify",
        fontsize=18,
        fontweight="bold",
    )

    df_winners = df[df["is_grammy_winner"] == True]
    top_artists = (
        df_winners.groupby("artists")["popularity"]
        .mean()
        .nlargest(10)
        .reset_index()
    )
    sns.barplot(
        data=top_artists,
        x="popularity",
        y="artists",
        ax=axes[0, 0],
        palette="viridis",
    )
    axes[0, 0].set_title("Top 10 Artistas con Grammy por popularidad promedio")
    axes[0, 0].set_xlabel("Popularidad (0-100)")
    axes[0, 0].set_ylabel("Artista")

    sns.boxplot(
        data=df,
        x="is_grammy_winner",
        y="popularity",
        ax=axes[0, 1],
        palette="Set2",
    )
    axes[0, 1].set_title("Ganadores vs Losers")
    axes[0, 1].set_xticklabels(["Sin Grammy", "Ganador de Grammy"])
    axes[0, 1].set_xlabel("Estado Grammy")
    axes[0, 1].set_ylabel("Popularidad")

    top_genres = df_winners["track_genre"].value_counts().nlargest(8)
    axes[1, 0].pie(
        top_genres.values,
        labels=top_genres.index,
        autopct="%1.1f%%",
        colors=sns.color_palette("pastel"),
    )
    axes[1, 0].set_title("Distribución de Géneros de canciones con Grammy")

    sns.scatterplot(
        data=df.sample(min(2000, len(df))),
        x="duration_min",
        y="popularity",
        hue="is_grammy_winner",
        alpha=0.6,
        ax=axes[1, 1],
    )
    axes[1, 1].set_title("Popularidad vs Duración de Canción")
    axes[1, 1].set_xlabel("Duración en minutos")
    axes[1, 1].set_ylabel("Popularidad")

    os.makedirs("/opt/airflow/reports", exist_ok=True)
    report_path = "/opt/airflow/reports/dashboard.png"
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig(report_path, dpi=300)
    print(f"Reporte estático generado en: {report_path}")


if __name__ == "__main__":
    generate_static_dashboard()