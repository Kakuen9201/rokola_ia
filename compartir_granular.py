import os
import time
import psycopg2
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURACIÓN ---
# Alcances requeridos para modificar permisos
SCOPES = ['https://www.googleapis.com/auth/drive']

# Si no encuentra la variable, usa 'credentials.json' por defecto, pero es mejor que venga del env.
CREDENTIALS_FILE = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'credentials.json')

# Conexión DB
DB_PARAMS = {
    "host": os.getenv("DB_HOST"),
    "database": "postgres",
    "user": "postgres",
    "password": os.getenv("DB_PASSWORD"),
    "port": "5432"
}


def get_drive_service():
    """Autenticación con Google Drive (Híbrida: Service Account o OAuth)"""
    creds = None
    
    # 1. Intentamos cargar como Service Account (Robot)
    try:
        creds = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=SCOPES)
    except Exception:
        # 2. Si falla (porque es un JSON de OAuth normal), usamos el flujo de Usuario
        print("⚠️ Usando OAuth de usuario. PREPARA EL TRUCO DEL CURL...")
        
        # --- ESTA FUE LA LÍNEA QUE FALTABA ---
        flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
        
        # Iniciamos el servidor local en el puerto 8080 sin abrir navegador
        creds = flow.run_local_server(port=8080, open_browser=False)
        
    return build('drive', 'v3', credentials=creds)



def hacer_publico_archivo(service, file_id):
    """
    Configura el archivo para que sea legible por cualquiera con el link,
    independientemente de la carpeta padre.
    """
    try:
        # Definimos el permiso: role='reader', type='anyone'
        user_permission = {
            'type': 'anyone',
            'role': 'reader',
        }
        
        # Ejecutamos la orden
        service.permissions().create(
            fileId=file_id,
            body=user_permission,
            fields='id',
        ).execute()
        return True
        
    except Exception as e:
        if "already exists" in str(e).lower():
            return True # Ya estaba compartido
        print(f"❌ Error con {file_id}: {e}")
        return False

def main():
    service = get_drive_service()
    
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    
    # Solo seleccionamos archivos que existen en tu app
    print("📚 Leyendo catálogo de Postgres...")
    cur.execute("SELECT drive_file_id, clean_title FROM musica_startup")
    rows = cur.fetchall()
    total = len(rows)
    
    print(f"🔒 Iniciando blindaje de {total} archivos...")
    print("NOTA: Asegúrate de haber quitado el acceso público a la CARPETA RAÍZ primero.")
    print("-" * 60)
    
    count = 0
    start_time = time.time()

    for i, row in enumerate(rows):
        file_id, title = row
        
        # Pequeño delay para no saturar la API de Google (Rate Limits)
        # Google permite unas 10 peticiones por segundo aprox, seamos conservadores.
        time.sleep(0.2) 
        
        exito = hacer_publico_archivo(service, file_id)
        
        if exito:
            print(f"[{i+1}/{total}] 🔓 {title[:30]}... -> PÚBLICO")
            count += 1
        else:
            print(f"[{i+1}/{total}] ⚠️ {title[:30]}... -> FALLÓ")

    duration = time.time() - start_time
    print("-" * 60)
    print(f"✅ Proceso terminado. {count} archivos liberados individualmente.")
    print(f"⏱️ Tiempo: {duration/60:.2f} minutos")

if __name__ == "__main__":
    main()