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

def limpiar_resumen(texto):
    if not texto: return None
    texto_limpio = re.sub(r'<a href=.*?>.*?</a>', '', texto)
    return texto_limpio.strip()

def obtener_genero_artista(artista):
    """Si la canción no tiene género, buscamos el del artista"""
    if not artista: return None
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
    # Si no tenemos artista, intentamos buscar solo por canción (menos preciso, pero mejor que nada)
    if not artista:
        params = {"method": "track.search", "track": cancion, "limit": 1}
    else:
        params = {"method": "track.getInfo", "artist": artista, "track": cancion, "autocorrect": 1}

    url = "http://ws.audioscrobbler.com/2.0/"
    params["api_key"] = API_KEY
    params["lang"] = "es"
    params["format"] = "json"
    
    info = {
        "found": False, "mbid": None, "album": None, "image": None, 
        "genre": None, "summary": None, "duration": 0, "raw": None,
        "real_artist_name": None, "real_track_name": None
    }
    
    try:
        response = requests.get(url, params=params, timeout=5)
        data = response.json()

        # Adaptador para track.search vs track.getInfo
        track_data = None
        if "track" in data and isinstance(data["track"], dict):
            track_data = data["track"] # Caso track.getInfo
        elif "results" in data and "trackmatches" in data["results"]:
             matches = data["results"]["trackmatches"]["track"]
             if matches:
                 # Si buscamos solo por nombre, hacemos una segunda llamada con artista+track encontrado
                 found_artist = matches[0]["artist"]
                 found_track = matches[0]["name"]
                 return consultar_lastfm(found_artist, found_track)

        if track_data:
            t = track_data
            info["found"] = True
            info["mbid"] = t.get("mbid")
            info["raw"] = json.dumps(t)
            info["duration"] = int(t.get("duration", "0"))
            info["real_track_name"] = t.get("name")
            
            if "artist" in t:
                info["real_artist_name"] = t["artist"].get("name")

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

    print("🔎 Buscando canciones pendientes (lastfm_processed = FALSE)...")
    
    # --- CORRECCIÓN 1: Usamos original_folder_name como artista y clean_title ---
    # COALESCE(artist, original_folder_name): Si artist es NULL, usa la carpeta.
    sql_query = """
        SELECT id, clean_title, COALESCE(artist, original_folder_name) as search_artist
        FROM musica_startup 
        WHERE lastfm_processed = FALSE 
        LIMIT 500
    """
    cur.execute(sql_query)
    rows = cur.fetchall()

    print(f"📋 En cola: {len(rows)} canciones.\n")
    conteo_ok = 0

    for row in rows:
        id_db, titulo_limpio, artista_busqueda = row
        
        # Validación extra por si title es None
        if not titulo_limpio: titulo_limpio = "Unknown" 
        
        # --- CORRECCIÓN 2: Ya no limpiamos aquí, confiamos en SQL ---
        info = consultar_lastfm(artista_busqueda, titulo_limpio)
        
        # Fallback de Género
        if not info["genre"] and info["real_artist_name"]:
            genero_artista = obtener_genero_artista(info["real_artist_name"])
            if genero_artista:
                info["genre"] = genero_artista

        # UPDATE MAESTRO
        if info["found"]:
            sql_update = """
                UPDATE musica_startup 
                SET lastfm_processed = TRUE,
                    lastfm_mbid = %s,
                    album = %s,
                    cover_image = %s,
                    genre = COALESCE(genre, %s),
                    music_summary = %s,
                    duration_ms = GREATEST(duration_ms, %s),
                    raw_metadata = %s,
                    artist = COALESCE(artist, %s), -- Guardamos el artista real si estaba vacío
                    real_name = %s -- Guardamos el nombre real de la canción
                WHERE id = %s
            """
            cur.execute(sql_update, (
                info["mbid"], info["album"], info["image"], 
                info["genre"], info["summary"], info["duration"], 
                info["raw"], info["real_artist_name"], info["real_track_name"],
                id_db
            ))
            print(f"✅ {titulo_limpio[:20]} -> {info['real_artist_name']} | OK")
            conteo_ok += 1
        
        else:
            print(f"❌ {titulo_limpio[:20]} (No encontrado en LastFM)")
            # Marcamos TRUE para no trabar la cola, pero no llenamos datos
            cur.execute("UPDATE musica_startup SET lastfm_processed = TRUE WHERE id = %s", (id_db,))

        conn.commit()
        time.sleep(0.25) # Respeto a la API

    print(f"\n✨ Lote terminado. {conteo_ok} canciones enriquecidas.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    procesar_musica()