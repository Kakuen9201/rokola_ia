import boto3
from boto3.dynamodb.conditions import Key
import os
from dotenv import load_dotenv

load_dotenv()

def probar_indice_artista():
    print("🕵️‍♂️ DIAGNÓSTICO DE ARTISTA (Piero)...")
    try:
        dynamodb = boto3.resource('dynamodb', region_name=os.getenv("AWS_REGION"))
        table = dynamodb.Table('MusicaStartup')
        
        # Intentamos usar el índice
        response = table.query(
            IndexName='ArtistIndex',
            KeyConditionExpression=Key('artist').begins_with('Piero')
        )
        
        items = response.get('Items', [])
        print(f"✅ ¡ÉXITO! El índice respondió {len(items)} resultados.")
        
        # Buscamos a Mi Viejo en la lista
        encontrado = False
        for item in items:
            if item.get('clean_title') == "Mi Viejo":
                print(f"   🎉 ¡ENCONTRADO! 'Mi Viejo' está en el índice.")
                encontrado = True
                break
        
        if not encontrado:
            print("   ⚠️ El índice funciona pero NO trae 'Mi Viejo'.")
            
    except Exception as e:
        print(f"❌ EL ÍNDICE SIGUE FALLANDO:\n   {e}")

if __name__ == "__main__":
    probar_indice_artista()
