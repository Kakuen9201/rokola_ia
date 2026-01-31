import os
import time
import requests
import psycopg2
import re
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURACIÓN ---
DB_PARAMS = {
    "host": os.getenv("DB_HOST"),
    "database": "postgres",
    "user": "postgres",
    "password": os.getenv("DB_PASSWORD"),
    "port": "5432"
}

ITUNES_URL = "https://itunes.apple.com/search"
DEEZER_URL = "https://api.deezer.com/search"

def limpiar_texto(texto):
    """Elimina basura: parentesis, corchetes y espacios extra"""
    if not texto: return ""
    # Elimina contenido dentro de (), [], {}
    texto_limpio = re.sub(r'[\(\[\{].*?[\)\]\}]', '', texto)
    # Elimina caracteres no alfanuméricos al inicio (como guiones o puntos)
    texto_limpio = re.sub(r'^[^a-zA-Z0-9]+', '', texto_limpio)
    return texto_limpio.strip()

def _request_itunes(term):
    """Hace la petición a iTunes y devuelve la URL grande o None"""
    try:
        params = {"term": term, "media": "music", "entity": "song", "limit": 1}
        response = requests.get(ITUNES_URL, params=params, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data["resultCount"] > 0:
                artwork = data["results"][0].get("artworkUrl100")
                if artwork:
                    return artwork.replace("100x100", "600x600")
    except:
        pass
    return None

def _request_deezer(term):
    """Hace la petición a Deezer y devuelve la URL XL o None"""
    try:
        params = {"q": term, "limit": 1}
        response = requests.get(DEEZER_URL, params=params, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if "data" in data and len(data["data"]) > 0:
                album = data["data"][0].get("album", {})
                return album.get("cover_xl") or album.get("cover_big") or album.get("cover_medium")
    except:
        pass
    return None

def buscar_inteligente(artist, title):
    """La estrategia Híbrida de 6 pasos"""
    
    # Limpieza previa
    artist_clean = limpiar_texto(artist)
    title_clean = limpiar_texto(title)

    # 1. ITUNES NORMAL
    url = _request_itunes(f"{artist} {title}")
    if url: return url, "iTunes Normal"

    # 2. DEEZER NORMAL
    url = _request_deezer(f"{artist} {title}")
    if url: return url, "Deezer Normal"

    if artist != artist_clean or title != title_clean:
        # 3. ITUNES LIMPIO
        url = _request_itunes(f"{artist_clean} {title_clean}")
        if url: return url, "iTunes Limpio"
        
        # 4. DEEZER LIMPIO
        url = _request_deezer(f"{artist_clean} {title_clean}")
        if url: return url, "Deezer Limpio"

    # 5. SOLO TÍTULO
    if len(title_clean) > 3:
        url = _request_itunes(title_clean)
        if url: return url, "iTunes Título"
        
        url = _request_deezer(title_clean)
        if url: return url, "Deezer Título"

    return None, None

def main():
    if not DB_PARAMS["host"]:
        print("❌ Faltan variables de entorno.")
        return

    print(f"🔌 Conectando a Frailes ({DB_PARAMS['host']})...")
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    print("🔍 Buscando canciones restantes sin imagen...")
    cur.execute("SELECT id, artist, clean_title FROM musica_startup WHERE cover_image IS NULL OR cover_image = ''")
    rows = cur.fetchall()
    total = len(rows)
    
    if total == 0:
        print("✅ ¡Misión Cumplida! No queda nada sin foto.")
        return

    print(f"🎯 Misión: Encontrar {total} carátulas con Estrategia Híbrida (iTunes + Deezer).")
    print("-" * 60)

    encontradas = 0
    start_time = time.time()

    try:
        for i, row in enumerate(rows):
            song_id, artist_raw, title_raw = row
            
            # --- CORRECCIÓN: Manejo seguro de NULLs ---
            artist = artist_raw if artist_raw else "Desconocido"
            title = title_raw if title_raw else "Sin Título"
            
            # Print de progreso dinámico
            print(f"[{i+1}/{total}] 🎵 {artist[:15]}.. - {title[:15]}..", end=" ", flush=True)
            
            image_url, metodo = buscar_inteligente(artist, title)
            
            if image_url:
                cur.execute("UPDATE musica_startup SET cover_image = %s WHERE id = %s", (image_url, song_id))
                conn.commit()
                # Espacios extra al final para limpiar la línea
                print(f"\r[{i+1}/{total}] ✅ {artist[:15]}.. - {title[:15]}.. (Vía: {metodo})     ")
                encontradas += 1
            else:
                print(f"\r[{i+1}/{total}] ❌ {artist[:15]}.. - {title[:15]}.. (No hallada)        ")
            
            time.sleep(0.1)

    except KeyboardInterrupt:
        print("\n✋ Detenido.")
    
    finally:
        cur.close()
        conn.close()
        duration = time.time() - start_time
        print("-" * 60)
        print(f"🎉 FIN. Tiempo: {duration/60:.2f} min. Nuevas Fotos: {encontradas}")

if __name__ == "__main__":
    main()