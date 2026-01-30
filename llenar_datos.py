import os
import psycopg2
import requests
import time
import re
import json
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# --- CONFIGURACIÓN ---
API_KEY = os.getenv("LASTFM_API_KEY")
DB_PARAMS = {
    "host": os.getenv("DB_HOST"),
    "database": "postgres",
    "user": "postgres",
    "password": os.getenv("DB_PASSWORD"),
    "port": "5432"
}

def limpiar_nombre(nombre_archivo):
    nombre = re.sub(r'\.[a-zA-Z0-9]{3,4}$', '', nombre_archivo)
    nombre = re.sub(r'^\d+\s*[-_.]?\s*', '', nombre)
    nombre = nombre.replace('_', ' ')
    return nombre.strip()

def limpiar_resumen(texto):
    if not texto: return None
    texto_limpio = re.sub(r'<a href=.*?>.*?</a>', '', texto)
    return texto_limpio.strip()

def obtener_genero_artista(artista):
    """Si la canción no tiene género, buscamos el del artista"""
    url = "http://ws.audioscrobbler.com/2.0/"
    params = {"method": "artist.getTopTags", "artist": artista, "api_key": API_KEY, "format": "json"}
    try:
        response = requests.get(url, params=params, timeout=5)
        data = response.json()
        if "toptags" in data and "tag" in data["toptags"]:
            return extraer_mejor_tag(data["toptags"]["tag"])
    except: pass
    return None

def extraer_mejor_tag(tags):
    if not tags: return None
    blacklist = ['seen live', 'favorites', 'my favorites', 'albums i own', 'spanish', 'latino', 'all']
    
    if isinstance(tags, list):
        for tag in tags:
            nombre = tag["name"].lower()
            if nombre not in blacklist and len(nombre) > 2:
                return tag["name"].title()
    elif isinstance(tags, dict):
        if tags.get("name", "").lower() not in blacklist:
            return tags.get("name").title()
    return None

def consultar_lastfm(artista, cancion):
    url = "http://ws.audioscrobbler.com/2.0/"
    params = {
        "method": "track.getInfo", "api_key": API_KEY, "artist": artista, "track": cancion,
        "autocorrect": 1, "lang": "es", "format": "json"
    }
    info = {
        "found": False, "mbid": None, "album": None, "image": None, 
        "genre": None, "summary": None, "duration": 0, "raw": None
    }
    try:
        response = requests.get(url, params=params, timeout=5)
        data = response.json()

        if "track" in data:
            t = data["track"]
            info["found"] = True
            info["mbid"] = t.get("mbid")
            info["raw"] = json.dumps(t) # ¡Aquí guardamos el JSON completo!
            info["duration"] = int(t.get("duration", "0"))

            if "album" in t:
                info["album"] = t["album"].get("title")
                if "image" in t["album"]:
                    for img in t["album"]["image"]:
                        if img["size"] == "extralarge":
                            info["image"] = img["#text"]
                            break
            if "wiki" in t:
                info["summary"] = limpiar_resumen(t["wiki"].get("summary"))
            if "toptags" in t:
                info["genre"] = extraer_mejor_tag(t["toptags"].get("tag"))
    except Exception as e:
        print(f"⚠️ Error API: {e}")
    
    return info

def procesar_musica():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    print("🔎 Buscando canciones pendientes de procesar (lastfm_processed = FALSE)...")
    
    # 1. SELECTOR CORREGIDO: Usamos la bandera de control
    sql_query = """
        SELECT id, clean_title, artist 
        FROM musica_startup 
        WHERE lastfm_processed = FALSE 
        AND artist IS NOT NULL
        LIMIT 500
    """
    cur.execute(sql_query)
    rows = cur.fetchall()

    print(f"📋 En cola: {len(rows)} canciones.\n")
    conteo_ok = 0

    for row in rows:
        id_db, titulo, artista = row
        titulo_limpio = limpiar_nombre(titulo)
        
        info = consultar_lastfm(artista, titulo_limpio)
        origen_genero = "Track"

        # Fallback de Artista si falta género
        if not info["genre"]:
            genero_artista = obtener_genero_artista(artista)
            if genero_artista:
                info["genre"] = genero_artista
                origen_genero = "Artista"

        # UPDATE MAESTRO
        if info["found"] or info["genre"]:
            sql_update = """
                UPDATE musica_startup 
                SET lastfm_processed = TRUE,  -- ¡Marcamos como listo!
                    lastfm_mbid = COALESCE(%s, lastfm_mbid),
                    album = COALESCE(%s, album),
                    cover_image = COALESCE(%s, cover_image),
                    genre = COALESCE(genre, %s), -- Solo llena si estaba vacío (respeta tu SQL masivo)
                    music_summary = COALESCE(%s, music_summary),
                    duration_ms = GREATEST(duration_ms, %s),
                    raw_metadata = COALESCE(%s, raw_metadata)
                WHERE id = %s
            """
            # NOTA: En 'genre', usé COALESCE(genre, %s) al revés que antes. 
            # Esto significa: "Si YA tengo género (ej. Salsa), MANTENLO. Si no, usa el de LastFM".
            # Así respetamos tu curetaje masivo SQL.

            cur.execute(sql_update, (
                info["mbid"], info["album"], info["image"], 
                info["genre"], info["summary"], info["duration"], 
                info["raw"], id_db
            ))
            print(f"✅ {artista[:15]} - {titulo_limpio[:15]}... | Data Guardada")
            conteo_ok += 1
        
        else:
            # Si LastFM no sabe nada, igual marcamos TRUE para sacarla de la cola
            print(f"❌ {artista} - {titulo_limpio} (Sin datos)")
            cur.execute("UPDATE musica_startup SET lastfm_processed = TRUE WHERE id = %s", (id_db,))

        conn.commit()
        time.sleep(0.2) 

    print(f"\n✨ Lote terminado. {conteo_ok} canciones enriquecidas.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    procesar_musica()