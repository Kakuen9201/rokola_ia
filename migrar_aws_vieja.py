import psycopg2
import boto3
import os
import time
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURACIÓN ---
# 1. Conexión a Frailes (Postgres)
try:
    pg_conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database="postgres",
        user="postgres",
        password=os.getenv("DB_PASSWORD"),
        port="5432"
    )
except Exception as e:
    print(f"❌ Error conectando a Postgres: {e}")
    exit()

# 2. Conexión a AWS (DynamoDB)
# Asegúrate de tener 'aws configure' listo o las variables de entorno
dynamodb = boto3.resource(
    'dynamodb',
    region_name=os.getenv("AWS_REGION", "us-east-1"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)
table = dynamodb.Table('MusicaStartup')

def limpiar_para_dynamo(valor, default):
    """DynamoDB odia los strings vacíos. Si es vacío, mandamos None o Default"""
    if valor is None or valor == "":
        return default
    return valor

def migrar():
    cur = pg_conn.cursor()
    print("🚀 [Fase Final] Leyendo datos enriquecidos de Postgres...")
    
    # --- LA CONSULTA MAESTRA ---
    # Traemos todo lo que tenga al menos un Artista Y (fue procesado por LastFM O tiene Foto recuperada)
    sql_query = """
        SELECT id, drive_file_id, clean_title, artist, album, genre, 
               duration_ms, cover_image, web_view_link, music_summary 
        FROM musica_startup
        WHERE artist IS NOT NULL 
          AND (lastfm_processed = TRUE OR cover_image IS NOT NULL)
    """
    
    cur.execute(sql_query)
    rows = cur.fetchall()
    total = len(rows)
    
    print(f"📦 Preparando carga de {total} canciones a DynamoDB...")
    print("⏳ Esto puede tomar unos minutos dependiendo de tu internet...")
    
    start_time = time.time()
    
    # Usamos batch_writer para máxima velocidad (sube en paquetes de 25)
    with table.batch_writer() as batch:
        count = 0
        for row in rows:
            # Desempaquetamos
            pid, drive_id, title, artist, album, genre, dur, cover, link, summary = row
            
            # Limpieza de datos (Null Safety)
            title = limpiar_para_dynamo(title, "Desconocido")
            artist = limpiar_para_dynamo(artist, "Varios")
            album = limpiar_para_dynamo(album, None) # Si no hay album, no enviamos el campo o null
            
            # Keywords para tu buscador frontend
            # Combinamos todo en minúsculas para facilitar búsquedas
            keywords = f"{title} {artist} {album if album else ''}".lower()
            
            item = {
                'id': str(pid),                # Partition Key
                'drive_file_id': drive_id,
                'clean_title': title,
                'artist': artist,
                'genre': limpiar_para_dynamo(genre, "General"),
                'duration_ms': int(dur) if dur else 0,
                'cover_image': limpiar_para_dynamo(cover, None),
                'web_view_link': link,
                'search_keywords': keywords,   # <--- CLAVE PARA EL BUSCADOR
                'music_summary': limpiar_para_dynamo(summary, None),
                'updated_at': int(time.time())
            }
            
            # Agregamos album solo si existe (para ahorrar espacio y limpieza)
            if album:
                item['album'] = album

            batch.put_item(Item=item)
            
            count += 1
            if count % 100 == 0: 
                print(f"   ... {count}/{total} subidas", end="\r")

    duration = time.time() - start_time
    print(f"\n✅ ¡MIGRACIÓN COMPLETADA! {count} canciones están en la nube.")
    print(f"⏱️ Tiempo total: {duration/60:.2f} minutos")

if __name__ == "__main__":
    migrar()