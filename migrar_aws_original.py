import psycopg2
import boto3
import os
from dotenv import load_dotenv
import time

load_dotenv()

# Conexión Local
pg_conn = psycopg2.connect(
    host=os.getenv("DB_HOST"), # IP de Frailes
    database="postgres",
    user="postgres",
    password=os.getenv("DB_PASSWORD"), # Contraseña de Frailes
    port="5432"
)

# Conexión AWS (Usa las credenciales de 'aws configure')
dynamodb = boto3.resource(
    'dynamodb',
    region_name=os.getenv("AWS_REGION"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)
table = dynamodb.Table('MusicaStartup')


def migrar():
    cur = pg_conn.cursor()
    print("🚀 Leyendo datos de Postgres...")
    
    # Traemos solo lo que ya procesamos con LastFM
    cur.execute("""
        SELECT id, drive_file_id, clean_title, artist, album, genre, 
               duration_ms, cover_image, web_view_link, music_summary 
        FROM musica_startup
        WHERE lastfm_processed = TRUE
    """)
    rows = cur.fetchall()
    
    print(f"📦 Iniciando carga de {len(rows)} canciones a AWS DynamoDB...")
    
    with table.batch_writer() as batch:
        count = 0
        for row in rows:
            # Preparar datos
            titulo = row[2] if row[2] else "Desconocido"
            artista = row[3] if row[3] else "Varios"
            album = row[4] if row[4] else ""
            
            # Generar SEARCH_KEYWORDS (¡Magia para la búsqueda!)
            keywords = f"{titulo} {artista} {album}".lower()
            
            item = {
                'id': str(row[0]),
                'drive_file_id': row[1],
                'clean_title': titulo,
                'artist': artista,
                'album': album,
                'genre': row[5],
                'duration_ms': row[6] if row[6] else 0,
                'cover_image': row[7],
                'web_view_link': row[8],
                'search_keywords': keywords, # Campo clave
                'music_summary': row[9] if row[9] else None,
                'mood_tags': []
            }
            
            batch.put_item(Item=item)
            count += 1
            if count % 50 == 0: print(f"   ... {count} subidas")

    print("✅ ¡MIGRACIÓN COMPLETADA EXITOSAMENTE!")

if __name__ == "__main__":
    migrar()