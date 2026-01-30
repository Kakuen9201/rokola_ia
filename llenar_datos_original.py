import os
import psycopg2
import requests
import time
import re
from dotenv import load_dotenv

# Tiene que ir ANTES de usar os.getenv
load_dotenv()

# --- CONFIGURACIÓN ---
API_KEY = os.getenv("LASTFM_API_KEY")
DB_PARAMS = {
    "host": os.getenv("DB_HOST"),  # <--- Aquí leerá la IP de Frailes
    "database": "postgres",
    "user": "postgres",
    "password": os.getenv("DB_PASSWORD"),
    "port": "5432"
}

# --- FUNCIÓN 1: Limpiar el nombre del archivo ---
def limpiar_nombre(nombre_archivo):
    # 1. Quitar la extensión del archivo (.mp3, .MP3, .flac)
    nombre = re.sub(r'\.[a-zA-Z0-9]{3,4}$', '', nombre_archivo)
    # 2. Quitar números al inicio (ej: "01 ", "01.- ", "Track 1 ")
    nombre = re.sub(r'^\d+\s*[-_.]?\s*', '', nombre)
    # 3. Quitar guiones bajos por espacios
    nombre = nombre.replace('_', ' ')
    return nombre.strip()

# --- FUNCIÓN 2: Consultar Last.fm ---
def consultar_lastfm(artista, cancion):
    url = "http://ws.audioscrobbler.com/2.0/"
    params = {
        "method": "track.getInfo",
        "api_key": API_KEY,
        "artist": artista,
        "track": cancion,
        "format": "json"
    }
    try:
        response = requests.get(url, params=params, timeout=5)
        data = response.json()
        
        # Verificamos si Last.fm encontró la canción
        if "track" in data:
            t = data["track"]
            
            # Buscamos la imagen más grande disponible
            imagen = None
            if "album" in t and "image" in t["album"]:
                # Intentamos sacar la 'extralarge', si no, la última de la lista
                imgs = t["album"]["image"]
                imagen = next((img["#text"] for img in imgs if img["size"] == "extralarge"), imgs[-1]["#text"])

            summary = t.get("wiki", {}).get("summary", "")
            album_title = t.get("album", {}).get("title", None)
            
            return {
                "found": True,
                "duration": t.get("duration", 0),
                "image": imagen,
                "summary": summary,
                "mbid": t.get("mbid", None),
                "album": album_title
            }
    except Exception as e:
        print(f"   ❌ Error de conexión: {e}")
    
    return {"found": False}

# --- PROCESO PRINCIPAL ---
def procesar_musica():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
    except Exception as e:
        print("Error conectando a la BD. ¿Estás corriendo el script desde donde puedes ver a Postgres?")
        print(e)
        return

    print("🎵 Iniciando proceso de enriquecimiento...")

    while True:
        # 1. Traemos 10 canciones pendientes (lastfm_processed = FALSE)
        #    Solo traemos las que ya tienen artista (porque hicimos el UPDATE previo)
        sql_select = """
            SELECT id, name, artist 
            FROM musica_startup 
            WHERE lastfm_processed = FALSE 
            AND artist IS NOT NULL
            LIMIT 50;
        """

        cur.execute(sql_select)
        lote = cur.fetchall()

        if not lote:
            print("✅ ¡Todo listo! No quedan canciones pendientes.")
            break

        for id_db, nombre_archivo, artista_db in lote:
            # Limpieza
            titulo_limpio = limpiar_nombre(nombre_archivo)
            artista_db = artista_db.strip()

            print(f"👉 Procesando ID {id_db}: {artista_db} - {titulo_limpio}")

            # Consulta API
            info = consultar_lastfm(artista_db, titulo_limpio)

            if info["found"]:
                # Si encontramos info, guardamos TODO (incluyendo si descubrimos el álbum real)
                sql_update = """
                    UPDATE musica_startup 
                    SET clean_title = %s,
                        duration_ms = %s,
                        cover_image = %s,
                        music_summary = %s,
                        lastfm_mbid = %s,
                        album = COALESCE(album, %s), 
                        lastfm_processed = TRUE
                    WHERE id = %s
                """
                cur.execute(sql_update, (
                    titulo_limpio, 
                    info["duration"], 
                    info["image"], 
                    info["summary"], 
                    info["mbid"], 
                    info["album"], 
                    id_db
                ))
                print(f"   ✨ Encontrada! (Duración: {info['duration']}ms)")
            else:
                # Si NO encontramos, guardamos el título limpio y marcamos procesado para no repetir
                sql_fallback = """
                    UPDATE musica_startup 
                    SET clean_title = %s,
                        lastfm_processed = TRUE
                    WHERE id = %s
                """
                cur.execute(sql_fallback, (titulo_limpio, id_db))
                print("   ⚠️ No encontrada en Last.fm (Solo limpieza guardada)")

            conn.commit()
            time.sleep(0.5) # Respeto a la API (medio segundo entre canciones)

        # Eliminar luego
        #print("--- PRUEBA FINALIZADA: Se procesó 1 canción ---")
        #break  # <--- AGREGA ESTO (Hace que el while True se detenga)
                
        print("--- Lote terminado, siguiente ronda... ---")

    cur.close()
    conn.close()

if __name__ == "__main__":
    procesar_musica()