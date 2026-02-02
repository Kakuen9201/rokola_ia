import psycopg2
import requests
import time
import re
import os
from dotenv import load_dotenv

""" Ejecución Continua (Bucle)
Recordar correr de esta manera
while true; do
    echo "🔄 Procesando lote de 50..."
    python3 discogs_miner.py
    sleep 5
done
 """

# --- CARGAR VARIABLES DE ENTORNO ---
load_dotenv()

# --- CONFIGURACIÓN SEGURA ---
DISCOGS_TOKEN = os.getenv("DISCOGS_TOKEN")
BATCH_SIZE = 50  # Procesar de 50 en 50
RATE_LIMIT_DELAY = 1.2  # 1.2 seg es seguro (API pide max 60/min)

# Validación de seguridad
if not DISCOGS_TOKEN:
    print("❌ ERROR CRÍTICO: No se encontró 'DISCOGS_TOKEN' en el archivo .env")
    exit(1)

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", "5432")
}

# Verificamos que existan las credenciales mínimas de DB
if not DB_CONFIG["password"] or not DB_CONFIG["host"]:
    print("❌ ERROR CRÍTICO: Faltan credenciales de Base de Datos en el archivo .env")
    exit(1)


# --- FUNCIONES ---

def limpiar_nombre(nombre_archivo):
    """Limpia el nombre del archivo para mejorar la búsqueda"""
    if not nombre_archivo: return ""
    nombre = re.sub(r'\.mp3$', '', nombre_archivo, flags=re.IGNORECASE)
    nombre = re.sub(r'^\d+[\s.-]*', '', nombre) # Quitar numeros track iniciales
    return nombre.strip()

def search_discogs(song_name, artist):
    """Busca en la API de Discogs"""
    url = "https://api.discogs.com/database/search"
    headers = {
        "Authorization": f"Discogs token={DISCOGS_TOKEN}",
        "User-Agent": "RokolaIA/1.0"
    }
    
    # Prioridad: Buscar por Artista + Canción
    query = f"{artist} - {song_name}"
    
    params = {
        "q": query,
        "type": "release", 
        "per_page": 1, 
        "format": "Vinyl" # Preferir vinilos
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        
        # Manejo de Rate Limit (Si nos pasamos, esperamos)
        if response.status_code == 429:
            print("⏳ Rate Limit alcanzado. Esperando 60 segundos...")
            time.sleep(60)
            return search_discogs(song_name, artist) # Reintentar
            
        data = response.json()
        
        if "results" in data and len(data["results"]) > 0:
            return data["results"][0]
            
    except Exception as e:
        print(f"   ❌ Error conexión Discogs: {e}")
    
    return None

def mina_de_datos():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        print(f"⛏️ Iniciando MINERO DISCOGS SEGURO (Lote de {BATCH_SIZE})...")
        print(f"   Host DB: {DB_CONFIG['host']}")
        
        # Seleccionamos canciones NO procesadas y que tengan Artista definido
        sql_select = """
            SELECT id, name, artist 
            FROM musica_startup 
            WHERE discogs_processed = FALSE 
            AND artist IS NOT NULL 
            LIMIT %s;
        """
        cur.execute(sql_select, (BATCH_SIZE,))
        rows = cur.fetchall()
        
        if not rows:
            print("✅ ¡Todo procesado! No hay canciones pendientes.")
            conn.close()
            return

        procesados = 0
        encontrados = 0

        for row in rows:
            db_id = row[0]
            filename = row[1]
            artist = row[2]
            
            clean_title = limpiar_nombre(filename)
            print(f"🎵 [{procesados + 1}/{len(rows)}] Buscando: {artist} - {clean_title}...")
            
            meta = search_discogs(clean_title, artist)
            
            if meta:
                encontrados += 1
                titulo_encontrado = meta.get('title', 'Sin Título')
                anio_encontrado = meta.get('year', 'N/A')
                print(f"   ✅ Encontrado: {titulo_encontrado} ({anio_encontrado})")
                
                # Preparamos datos
                year = meta.get('year')
                # Validación extra: a veces Discogs devuelve años raros o vacíos
                if year and not str(year).isdigit(): year = None
                
                country = meta.get('country')
                
                # Manejo seguro de listas (style, label, format pueden ser None o Listas)
                estilos = meta.get('style', [])
                style = ", ".join(estilos)[:255] if isinstance(estilos, list) else str(estilos)[:255]
                
                labels_raw = meta.get('label', [])
                labels = ", ".join(labels_raw)[:255] if isinstance(labels_raw, list) else str(labels_raw)[:255]
                
                formats_raw = meta.get('format', [])
                formats = ", ".join(formats_raw)[:255] if isinstance(formats_raw, list) else str(formats_raw)[:255]
                
                discogs_id = meta.get('id')
                
                # Actualizamos DB
                sql_update = """
                    UPDATE musica_startup 
                    SET release_year = %s,
                        country = %s,
                        style = %s,
                        record_label = %s,
                        original_format = %s,
                        discogs_id = %s,
                        discogs_processed = TRUE
                    WHERE id = %s;
                """
                cur.execute(sql_update, (year, country, style, labels, formats, discogs_id, db_id))
                
            else:
                print("   ⚠️ No encontrado. Marcando como procesado.")
                # Marcamos como procesado igual para no volver a buscarlo eternamente
                cur.execute("UPDATE musica_startup SET discogs_processed = TRUE WHERE id = %s;", (db_id,))
            
            conn.commit() # Guardar cambios paso a paso
            procesados += 1
            time.sleep(RATE_LIMIT_DELAY) # Respetar a Discogs

        print(f"\n✨ Lote terminado. Encontrados: {encontrados}/{procesados}")
        cur.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"❌ Error de Base de Datos: {e}")
    except Exception as e:
        print(f"❌ Error General: {e}")

if __name__ == "__main__":
    mina_de_datos()