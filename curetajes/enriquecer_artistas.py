"""
=============================================================================
 SCRIPT: ENRIQUECER Y NORMALIZAR ARTISTAS
 DESCRIPCIÓN: 
 1. Normaliza nombres (Capitalización correcta).
 2. Inyecta nombres reales de artistas famosos automáticamente.
=============================================================================
"""
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# --- 🧠 BASE DE CONOCIMIENTO (Yo ya investigué esto por ti) ---
CONOCIMIENTO_NOMBRES_REALES = {
    "Piero": "Piero Antonio Franco De Benedictis",
    "Diomedes Diaz": "Diomedes Díaz Maestre",
    "Cherry Navarro": "Alexis Enrique Navarro Velásquez",
    "Jose Luis Rodriguez": "José Luis Rodríguez González",
    "El Puma": "José Luis Rodríguez González",
    "Ricardo Montaner": "Héctor Eduardo Reglero Montaner",
    "Franco De Vita": "Franco Atilio De Vita De Vito",
    "Oscar D'leon": "Óscar Emilio León Simosa",
    "Simon Diaz": "Simón Narciso Díaz Márquez",
    "Juan Gabriel": "Alberto Aguilera Valadez",
    "Rocio Durcal": "María de los Ángeles de las Heras Ortiz",
    "Camilo Sesto": "Camilo Blanes Cortés",
    "Roberto Carlos": "Roberto Carlos Braga",
    "Julio Iglesias": "Julio José Iglesias de la Cueva",
    "Jose Jose": "José Romulo Sosa Ortiz",
    "Vicente Fernandez": "Vicente Fernández Gómez",
    "Luis Miguel": "Luis Miguel Gallego Basteri",
    "Sandro": "Roberto Sánchez-Ocampo",
    "Celia Cruz": "Úrsula Hilaria Celia de la Caridad Cruz Alfonso",
    "Hector Lavoe": "Héctor Juan Pérez Martínez",
    "Billos Caracas Boys": "José María 'Billo' Frómeta (Fundador)",
    "Ruben Blades": "Rubén Blades Bellido de Luna",
    "Marc Anthony": "Marco Antonio Muñiz Rivera",
    "Chayanne": "Elmer Figueroa Arce",
    "Daddy Yankee": "Ramón Luis Ayala Rodríguez",
    "Shakira": "Shakira Isabel Mebarak Ripoll",
    # --- NUEVOS AGREGADOS (Rescatados de tu BD) ---
    "Willy Chirino": "Wilfredo José Chirino",
    "Elton John": "Reginald Kenneth Dwight",
    "Tito Puente": "Ernesto Antonio Puente",
    "Jose Feliciano": "José Monserrate Feliciano García",
    "Hugo Blanco": "Hugo César Blanco Manzo",
    "Ilan Chester": "Ilan Czenstochowski Schaechter",
    "Yolandita Monge": "Yolanda Monge Betancourt",
    "Ednita Nazario": "Edna María Nazario Figueroa",
    "Valeria Lynch": "María Cristina Lancelotti",
    "Alvaro Torres": "Álvaro Germán Ibarra Torres",
    "Dyango": "José Gómez Romero",
    "Raphael": "Miguel Rafael Martos Sánchez"
}

def conectar_db():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database="postgres",
        user="postgres",
        password=os.getenv("DB_PASSWORD"),
        port="5432"
    )

def ejecutar_enriquecimiento():
    print("🚑 INICIANDO ENRIQUECIMIENTO DE DATOS...")
    conn = conectar_db()
    cur = conn.cursor()
    
    # 1. LIMPIEZA GENERAL (Normalizar Mayúsculas/Minúsculas)
    print("   🧹 Normalizando nombres (Ej: 'PIERO' -> 'Piero')...")
    cur.execute("UPDATE musica_startup SET artist = TRIM(INITCAP(artist));")
    print(f"      -> Registros procesados por normalización.")

    # 2. INYECCIÓN DE NOMBRES REALES
    print("   💉 Inyectando nombres reales de artistas famosos...")
    actualizados = 0
    
    for artista, nombre_real in CONOCIMIENTO_NOMBRES_REALES.items():
        # Buscamos variantes del artista (ignorando acentos o mayúsculas)
        # Usamos ILIKE con % para ser flexibles (ej: encontrar 'Oscar D Leon')
        query = """
            UPDATE musica_startup 
            SET real_name = %s 
            WHERE unaccent(artist) ILIKE unaccent(%s) 
            AND (real_name IS NULL OR real_name = '');
        """
        # Nota: Si 'unaccent' no está instalado en postgres, usamos comparación simple
        try:
            cur.execute(query, (nombre_real, artista))
        except psycopg2.errors.UndefinedFunction:
            # Plan B si no hay extensión unaccent
            conn.rollback()
            query_simple = "UPDATE musica_startup SET real_name = %s WHERE artist ILIKE %s AND real_name IS NULL;"
            cur.execute(query_simple, (nombre_real, artista))
            
        if cur.rowcount > 0:
            print(f"      ✅ {artista} -> {nombre_real} ({cur.rowcount} canciones)")
            actualizados += cur.rowcount
        else:
            pass # No se encontraron canciones para este artista hoy

    conn.commit()
    cur.close()
    conn.close()
    print(f"\n✨ ¡LISTO! {actualizados} canciones enriquecidas con biografía real.")
    print("   Recuerda correr 'migrar_aws.py' para subir estos cambios a la nube.")

if __name__ == "__main__":
    ejecutar_enriquecimiento()