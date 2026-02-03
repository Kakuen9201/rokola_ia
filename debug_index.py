import boto3
from boto3.dynamodb.conditions import Key
import os
from dotenv import load_dotenv

load_dotenv()

def probar_indice_artista():
    print("🕵️‍♂️ DIAGNÓSTICO DE ARTISTA (Piero)...")
    
    try:
        dynamodb = boto3.resource(
            'dynamodb',
            region_name=os.getenv("AWS_REGION"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
        table = dynamodb.Table('MusicaStartup')
    except Exception as e:
        print(f"❌ Error conexión: {e}")
        return

    # DATOS A VERIFICAR EN AWS
    nombre_indice = "ArtistIndex"  # <--- ¿Se llama así en tu consola?
    nombre_columna = "artist"      # <--- ¿La clave es esta? (Minúsculas)
    artista_buscado = "Piero"

    print(f"🧪 Consultando índice: '{nombre_indice}'")
    print(f"   Clave: {nombre_columna} = '{artista_buscado}'")

    try:
        response = table.query(
            IndexName=nombre_indice,
            KeyConditionExpression=Key(nombre_columna).begins_with(artista_buscado)
        )
        
        items = response.get('Items', [])
        count = response['Count']
        
        if count > 0:
            print(f"\n✅ ¡ÉXITO! El índice funciona.")
            print(f"   Encontrados: {count} canciones.")
            # Verificamos si Mi Viejo está aquí
            tiene_mi_viejo = any(i['clean_title'] == "Mi Viejo" for i in items)
            if tiene_mi_viejo:
                print("   ✅ 'Mi Viejo' aparece en la lista del índice.")
            else:
                print("   ⚠️ El índice funciona, pero 'Mi Viejo' NO está en él (¿Falta sincronizar?).")
        else:
            print(f"\n⚠️ El índice respondió 0 resultados.")
            print("   Esto significa que no hay canciones que empiecen EXACTAMENTE con 'Piero'.")

    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO DEL ÍNDICE:")
        print(f"   {str(e)}")
        print("-" * 30)
        if "Requested index not found" in str(e):
            print("💡 CAUSA: No existe un índice llamado 'ArtistIndex' en AWS.")
        if "ValidationException" in str(e):
            print("💡 CAUSA: La columna clave del índice no es 'artist'.")

if __name__ == "__main__":
    probar_indice_artista()
