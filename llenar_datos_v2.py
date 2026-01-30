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

# --- HERRAMIENTAS DE LIMPIEZA ---
def limpiar_nombre(nombre_archivo):
    nombre = re.sub(r'\.[a-zA-Z0-9]{3,4}$', '', nombre_archivo)
    nombre = re.sub(r'^\d+\s*[-_.]?\s*', '', nombre)
    nombre = nombre.replace('_', ' ')
    return nombre.strip()

def limpiar_resumen(texto):
    if not texto: return None
    texto_limpio = re.sub(r'<a href=.*?>.*?</a>', '', texto)
    return texto_limpio.strip()

# --- 1. CONSULTAR ARTISTA (PLAN B) ---
def obtener_genero_artista(artista):
    """Si la canción no tiene género, buscamos el del artista"""
    url = "http://ws.audioscrobbler.com/2.0/"
    params = {
        "method": "artist.getTopTags",
        "artist": artista,
        "api_key": API_KEY,
        "format": "json"
    }
    try:
        response = requests.get(url, params=params, timeout=5)
        data = response.json()
        if "toptags" in data and "tag" in data["toptags"]:
            tags = data["toptags"]["tag"]
            return extraer_mejor_tag(tags)
    except:
        pass
    return None

# --- 2. LÓGICA DE EXTRACCIÓN DE TAGS ---
def extraer_mejor_tag(tags):
    if not tags: return None
    
    # Lista negra de tags inútiles
    blacklist = [
        'seen live', 'favorites', 'my favorites', 'albums i own', 
        'spanish', 'latino', 'female vocalists', 'male vocalists', 
        'singer-songwriter', 'under 2000 listeners', 'all', '00s'
    ]
    
    if isinstance(tags, list):
        for tag in tags:
            nombre = tag["name"].lower()
            if nombre not in blacklist and len(nombre) > 2:
                return tag["name"].title() # Retorna el primero bueno (ej: "Salsa")
    elif isinstance(tags, dict):
        nombre = tags.get("name", "").lower()
        if nombre not in blacklist:
            return tags.get("name").title()
            
    return None

# --- 3. CONSULTAR CANCIÓN (PLAN A) ---
def consultar_lastfm(artista, cancion):
    url = "http://ws.audioscrobbler.com/2.0/"
    params = {
        "method": "track.getInfo",
        "api_key": API_KEY,
        "artist": artista,
        "track": cancion,
        "autocorrect": 1,
        "lang": "es",
        "format": "json"
    }

    info = {
        "found": False,
        "mbid": None, "album": None, "image": None, 
        "genre": None, "summary": None, "duration": 0, "raw": None
    }

    try:
        response = requests.get(url, params=params, timeout=5)
        data = response.json()

        if "track" in data:
            t = data["track"]
            info["found"] = True
            info["mbid"] = t.get("mbid")
            info["raw"] = json.dumps(t)
            info["duration"] = int(t.get("duration", "0"))

            # Album
            if "album" in t:
                info["album"] = t["album"].get("title")
                # Imagen
                if "image" in t["album"]:
                    for img in t["album"]["image"]:
                        if img["size"] == "extralarge":
                            info["image"] = img["#text"]
                            break

            # Resumen
            if "wiki" in t:
                info["summary"] = limpiar_resumen(t["wiki"].get("summary"))

            # Género (Tags de la canción)
            if "toptags" in t:
                info["genre"] = extraer_mejor_tag(t["toptags"].get("tag"))

    except Exception as e:
        print(f"⚠️ Error API: {e}")
    
    return info

# --- FUNCIÓN PRINCIPAL ---
def procesar_musica():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    # Buscamos canciones SIN GÉNERO (aunque ya hayan sido procesadas antes)
    print("🔎 Buscando canciones huérfanas de género...")
    
    sql_query = """
        SELECT id, clean_title, artist 
        FROM musica_startup 
        WHERE (genre IS NULL OR genre = 'General')
        AND artist IS NOT NULL
        LIMIT 500
    """
    cur.execute(sql_query)
    rows = cur.fetchall()

    print(f"📋 En cola: {len(rows)} canciones.\n")

    conteo_ok = 0
    conteo_artista = 0

    for row in rows:
        id_db, titulo, artista = row
        titulo_limpio = limpiar_nombre(titulo)
        
        # 1. Consultar Canción
        info = consultar_lastfm(artista, titulo_limpio)
        origen_genero = "Track"

        # 2. Si no hay género, consultar Artista (Fallback)
        if not info["genre"]:
            genero_artista = obtener_genero_artista(artista)
            if genero_artista:
                info["genre"] = genero_artista
                origen_genero = "Artista (Fallback)"
                conteo_artista += 1
                
        # 3. Guardar si encontramos ALGO relevante
        if info["found"] or info["genre"]:
            
            # Construimos el UPDATE dinámico
            # Solo actualizamos lo que no sea None
            sql_update = """
                UPDATE musica_startup 
                SET lastfm_processed = TRUE,
                    lastfm_mbid = COALESCE(%s, lastfm_mbid),
                    album = COALESCE(%s, album),
                    cover_image = COALESCE(%s, cover_image),
                    genre = COALESCE(%s, genre),      -- Aquí entra el nuevo género
                    music_summary = COALESCE(%s, music_summary),
                    duration_ms = GREATEST(duration_ms, %s),
                    raw_metadata = COALESCE(%s, raw_metadata)
                WHERE id = %s
            """
            cur.execute(sql_update, (
                info["mbid"], info["album"], info["image"], 
                info["genre"], info["summary"], info["duration"], 
                info["raw"], id_db
            ))
            
            # Logs detallados para ver si funciona
            log_genero = f"[{info['genre']}]" if info['genre'] else "[---]"
            print(f"✅ {artista[:15]} - {titulo_limpio[:15]}... | {log_genero} ({origen_genero})")
            conteo_ok += 1
        
        else:
            print(f"❌ {artista} - {titulo_limpio} (Sin datos)")

        conn.commit()
        time.sleep(0.2) 

    print(f"\n✨ Resumen: {conteo_ok} procesados. {conteo_artista} rescatados por tags de Artista.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    procesar_musica()