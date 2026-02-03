"""
=============================================================================
 PROYECTO: ROKOLA IA - MIGRACIÓN SEGURA (MODO AHORRO)
 DESCRIPCIÓN: Sube datos a DynamoDB con pausas para no superar la Capa Gratuita.
=============================================================================
"""

import psycopg2
import boto3
import os
import time  # <--- Importante para las pausas
from dotenv import load_dotenv
from decimal import Decimal

load_dotenv()

# --- CONFIGURACIÓN DE VELOCIDAD ---
ITEMS_POR_LOTE = 25   # DynamoDB escribe en lotes de 25 máx
PAUSA_ENTRE_LOTES = 1.2 # Segundos de descanso (1.2s garantiza estar bajo el límite)

def conectar_postgres():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database="postgres",
        user="postgres",
        password=os.getenv("DB_PASSWORD"),
        port="5432"
    )

def conectar_aws():
    return boto3.resource(
        'dynamodb',
        region_name=os.getenv("AWS_REGION"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )

def migrar():
    pg_conn = conectar_postgres()
    dynamodb = conectar_aws()
    table = dynamodb.Table('MusicaStartup') # Asegúrate que coincida con la nueva tabla creada
    
    cur = pg_conn.cursor()
    print("\n🇻🇪 INICIANDO MIGRACIÓN 'MODO AHORRO'")
    print(f"   Estrategia: {ITEMS_POR_LOTE} items cada {PAUSA_ENTRE_LOTES} segundos.")
    print("   Esto mantendrá el consumo bajo el radar de cobros.")

    # QUERY MAESTRA (La misma de siempre)
    sql = """
        SELECT 
            id, drive_file_id, clean_title, artist, album, genre, 
            duration_ms, cover_image, web_view_link, music_summary,
            origin, curator, lastfm_mbid, nationality, real_name,
            release_year, country, style, record_label, original_format, discogs_id
        FROM musica_startup
        WHERE drive_file_id IS NOT NULL 
    """
    
    cur.execute(sql)
    rows = cur.fetchall()
    total_records = len(rows)
    print(f"📦 Total a procesar: {total_records} canciones.\n")
    
    # Usamos batch_writer pero controlamos el flujo
    with table.batch_writer() as batch:
        count = 0
        lote_actual = 0
        
        for row in rows:
            # Desempaquetado (Igual que antes)
            (db_id, drive_id, title, artist, album, genre, 
             duration, cover, link, summary, origin, curator, mbid, 
             nationality, real_name, 
             release_year, d_country, style, label, fmt, d_id) = row

            safe_title = title if title else "Desconocido"
            safe_artist = artist if artist else "Varios Artistas"
            
            # Limpiezas básicas
            safe_year = int(release_year) if release_year else None
            safe_d_id = int(d_id) if d_id else None
            safe_duration = int(duration) if duration else 0
            
            # Keywords para búsqueda
            year_str = str(safe_year) if safe_year else ""
            keywords = f"{safe_title} {safe_artist} {album or ''} {nationality or ''} {year_str} {style or ''}".lower()

            item = {
                'tipo': 'CATALOGO',      # 🆕 <--- ¡AGREGA ESTA LÍNEA AQUÍ!
                'id': str(db_id),
                'drive_file_id': drive_id,
                'clean_title': safe_title,
                'artist': safe_artist,
                'album': album if album else "",
                'genre': genre if genre else "General",
                'duration_ms': safe_duration,
                'cover_image': cover if cover else "",
                'web_view_link': link,
                'search_keywords': keywords,
                'music_summary': summary if summary else "",
                'nationality': nationality if nationality else "",
                'real_name': real_name if real_name else safe_artist,
                'release_year': safe_year,
                'country': d_country if d_country else "",
                'style': style if style else "",
                'record_label': label if label else "",
                'original_format': fmt if fmt else "",
                'discogs_id': safe_d_id
            }
            
            # Limpiar nulos
            item = {k: v for k, v in item.items() if v is not None and v != ""}

            # Agregar al lote
            batch.put_item(Item=item)
            
            count += 1
            lote_actual += 1

            # --- EL FRENO DE MANO 🛑 ---
            if lote_actual >= ITEMS_POR_LOTE:
                print(f"   ⏳ {count}/{total_records} - Pausa táctica de {PAUSA_ENTRE_LOTES}s...", end='\r')
                time.sleep(PAUSA_ENTRE_LOTES) # Dormir para bajar el consumo de WCU
                lote_actual = 0 # Reiniciar contador del lote

    cur.close()
    pg_conn.close()
    print(f"\n\n✅ ¡LISTO! {count} canciones migradas sin despertar al cobrador de AWS.")

if __name__ == "__main__":
    migrar()