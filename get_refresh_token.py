from google_auth_oauthlib.flow import InstalledAppFlow

# Estos son los permisos que pediremos (Lectura de Drive)
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

def obtener_token():
    # Asegúrate de tener 'credentials.json' en la misma carpeta
    flow = InstalledAppFlow.from_client_secrets_file(
        'credentials.json', SCOPES)
    
    # Esto abrirá tu navegador para que te loguees
    creds = flow.run_local_server(port=8080)
    
    print("\n" + "="*60)
    print("¡ÉXITO! AQUÍ TIENES TU REFRESH TOKEN (Cópialo todo):")
    print("="*60 + "\n")
    print(creds.refresh_token)
    print("\n" + "="*60)
    print("Guarda este token en las Variables de Entorno de tu Lambda.")

if __name__ == '__main__':
    obtener_token()