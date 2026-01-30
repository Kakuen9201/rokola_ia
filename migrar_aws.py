"""
=============================================================================
 PROYECTO: ROKOLA IA - SISTEMA DE GESTIÓN DE ARCHIVOS SONOROS (DAM)
 SCRIPT:   MIGRACIÓN Y SINCRONIZACIÓN DE METADATOS (Postgres -> AWS DynamoDB)
 
 DESCRIPCIÓN:
 Este script actúa como puente ETL (Extract, Transform, Load). 
 Extrae la información enriquecida y curada desde la base de datos maestra 
 (PostgreSQL en Frailes), incluyendo IDENTIDAD REAL y GEOGRAFÍA HUMANA,
 aplica normalización de datos y sincroniza el catálogo con la nube.
 
 AUTOR:    Rommel Contreras
 CARGO:    Director de Logos & Contexto
 FECHA:    Enero 2026
=============================================================================
"""

import psycopg2
import boto3
import os
from dotenv import load_dotenv
import time
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
    print("\n🚀 INICIANDO PROTOCOLO DE MIGRACIÓN: ROKOLA IA")
    print("   Autor: Rommel Contreras")
    print("   -------------------------------------------")
    print("   Leyendo catálogo maestro enriquecido (Identidad + Geografía)...")
    
    # 2. QUERY MAESTRA ACTUALIZADA
    # Incluye 'real_name'
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
            real_name
        FROM musica_startup
        WHERE lastfm_processed = TRUE
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
            # Desempaquetado seguro
            (db_id, drive_id, title, artist, album, genre, 
             duration, cover, link, summary, origin, curator, mbid, nationality, real_name) = row

            # --- LIMPIEZA Y VALIDACIÓN ---
            safe_title = title if title else "Desconocido"
            safe_artist = artist if artist else "Varios"
            safe_album = album if album else ""
            safe_nationality = nationality if nationality else ""
            safe_real_name = real_name if real_name else ""
            
            # --- GENERACIÓN DE PALABRAS CLAVE (SEO PROFUNDO) ---
            # Ahora la búsqueda encuentra: "Alberto Aguilera" -> Juan Gabriel
            keywords = f"{safe_title} {safe_artist} {safe_album} {safe_nationality} {safe_real_name}".lower()
            
            # Manejo de tipos para DynamoDB
            safe_duration = int(duration) if duration else 0
            safe_summary = summary if summary else "Sin descripción disponible."
            safe_origin = origin if origin else "Desconocido"
            safe_curator = curator if curator else "Sistema"
            safe_mbid = mbid if mbid else "N/A"
            safe_nat_display = nationality if nationality else "No especificada"
            safe_real_display = real_name if real_name else safe_artist # Si no hay nombre real, usa el artístico

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
                'search_keywords': keywords,     # Motor de búsqueda (ahora con identidad real)
                'music_summary': safe_summary,   # Contexto cultural
                'origin': safe_origin,           # Fuente del archivo
                'curator': safe_curator,         # Responsable
                'lastfm_mbid': safe_mbid,        # ID Universal
                'nationality': safe_nat_display, # Geografía Humana
                'real_name': safe_real_display   # Identidad Real
            }
            
            # Subida
            batch.put_item(Item=item)
            
            count += 1
            if count % 50 == 0: 
                print(f"   ⏳ Sincronizando... {count}/{total_records} items")

    # 4. CIERRE
    cur.close()
    pg_conn.close()
    
    print("-" * 50)
    print(f"✅ SINCRONIZACIÓN EXITOSA.")
    print(f"   Total actualizado: {count} obras culturales.")
    print("   Metadatos de Identidad y Geografía incorporados.")

if __name__ == "__main__":
    migrar()