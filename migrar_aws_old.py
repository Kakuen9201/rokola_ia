"""
=============================================================================
 PROYECTO: ROKOLA IA - SISTEMA DE GESTIÓN DE ARCHIVOS SONOROS (DAM)
 SCRIPT:   MIGRACIÓN MAESTRA V2 (Postgres -> AWS DynamoDB)
 
 DESCRIPCIÓN:
 Script ETL completo. Sincroniza:
 1. Metadatos básicos (Título, Artista).
 2. Identidad Real y Geografía Humana (LastFM).
 3. Datos de Edición y Coleccionismo (Discogs: Año, Sello, Estilo).
 
 AUTOR:    Rommel Contreras
 FECHA:    Febrero 2026
=============================================================================
"""

import psycopg2
import boto3
import os
from dotenv import load_dotenv
from decimal import Decimal

# Cargar variables de entorno
load_dotenv()

# --- CONFIGURACIÓN DE CONEXIÓN ---
def conectar_postgres():
    try:
        return psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database="postgres",
            user="postgres",
            password=os.getenv("DB_PASSWORD"),
            port="5432"
        )
    except Exception as e:
        print(f"❌ Error crítico conectando a Postgres: {e}")
        exit(1)

def conectar_aws():
    try:
        return boto3.resource(
            'dynamodb',
            region_name=os.getenv("AWS_REGION"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
    except Exception as e:
        print(f"❌ Error crítico conectando a AWS: {e}")
        exit(1)

def migrar():
    # 1. Inicializar recursos
    pg_conn = conectar_postgres()
    dynamodb = conectar_aws()
    table = dynamodb.Table('MusicaStartup')
    
    cur = pg_conn.cursor()
    print("\n🚀 INICIANDO MIGRACIÓN MAESTRA V2: ROKOLA IA")
    print("   Integrando datos de LastFM + Discogs...")
    
    # 2. QUERY MAESTRA V2 (INCLUYE DATOS DISCOGS)
    sql = """
        SELECT 
            id, 
            drive_file_id, 
            clean_title, 
            artist, 
            album, 
            genre, 
            duration_ms, 
            cover_image, 
            web_view_link, 
            music_summary,
            origin,
            curator,
            lastfm_mbid,
            nationality,
            real_name,
            -- 👇 NUEVOS CAMPOS (DISCOGS) 👇
            release_year,
            country,
            style,
            record_label,
            original_format,
            discogs_id
        FROM musica_startup
        -- Quitamos el filtro estricto para que suba todo lo disponible
        -- (Incluso si solo tiene datos básicos)
        WHERE drive_file_id IS NOT NULL 
    """
    
    cur.execute(sql)
    rows = cur.fetchall()
    total_records = len(rows)
    
    print(f"📦 Paquete de datos preparado: {total_records} registros.")
    print(f"📡 Iniciando transmisión a AWS DynamoDB...")
    
    # 3. PROCESO DE CARGA (BATCH)
    with table.batch_writer() as batch:
        count = 0
        for row in rows:
            # Desempaquetado seguro (21 columnas)
            (db_id, drive_id, title, artist, album, genre, 
             duration, cover, link, summary, origin, curator, mbid, 
             nationality, real_name, 
             release_year, d_country, style, label, fmt, d_id) = row

            # --- LIMPIEZA Y VALIDACIÓN BÁSICA ---
            safe_title = title if title else "Desconocido"
            safe_artist = artist if artist else "Varios"
            safe_album = album if album else ""
            
            # --- DATOS HUMANOS (LastFM) ---
            safe_nationality = nationality if nationality else ""
            safe_real_name = real_name if real_name else ""
            
            # --- DATOS DISCOGS ---
            safe_year = int(release_year) if release_year else None
            safe_d_country = d_country if d_country else ""
            safe_style = style if style else ""
            safe_label = label if label else ""
            safe_format = fmt if fmt else ""
            safe_d_id = int(d_id) if d_id else None

            # --- GENERACIÓN DE PALABRAS CLAVE (SEO PROFUNDO V2) ---
            # Ahora incluye Año, Estilo y País de edición
            year_str = str(safe_year) if safe_year else ""
            keywords = f"{safe_title} {safe_artist} {safe_album} {safe_nationality} {safe_real_name} {year_str} {safe_style} {safe_d_country}".lower()
            
            # Manejo de tipos DynamoDB
            safe_duration = int(duration) if duration else 0
            safe_summary = summary if summary else "Sin descripción disponible."
            
            # --- CONSTRUCCIÓN DEL OBJETO DIGITAL ---
            item = {
                'id': str(db_id),
                'drive_file_id': drive_id,
                'clean_title': safe_title,
                'artist': safe_artist,
                'album': safe_album,
                'genre': genre if genre else "General",
                'duration_ms': safe_duration,
                'cover_image': cover if cover else "",
                'web_view_link': link,
                'search_keywords': keywords,
                'music_summary': safe_summary,
                
                # Datos Identidad
                'nationality': safe_nationality if safe_nationality else "No especificada",
                'real_name': safe_real_name if safe_real_name else safe_artist,
                
                # Datos Discogs (Nuevos)
                'release_year': safe_year,
                'country': safe_d_country,   # País de edición (Físico)
                'style': safe_style,         # Subgénero
                'record_label': safe_label,
                'original_format': safe_format,
                'discogs_id': safe_d_id
            }
            
            # Limpiamos claves con valor None (DynamoDB no le gusta null en algunos casos)
            # o dejamos que batch_writer maneje. Lo mejor es quitar keys vacías opcionales.
            item = {k: v for k, v in item.items() if v is not None and v != ""}

            # Subida
            batch.put_item(Item=item)
            
            count += 1
            if count % 50 == 0: 
                print(f"   ⏳ Sincronizando... {count}/{total_records} items")

    # 4. CIERRE
    cur.close()
    pg_conn.close()
    
    print("-" * 50)
    print(f"✅ SINCRONIZACIÓN MAESTRA EXITOSA.")
    print(f"   Total actualizado: {count} obras.")
    print("   Ahora tu Rokola sabe de Años, Sellos y Estilos.")

if __name__ == "__main__":
    migrar()