import json
import boto3
import base64
import time
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal

# --- CONFIGURACIÓN ---
SECRET_KEY = "logoscontexto"
TABLE_NAME = "MusicaStartup" # Asegúrate de que este nombre coincida con tu tabla real

# Inicializamos recursos fuera del handler para reutilizar conexiones (Mejora de rendimiento)
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(TABLE_NAME)

# --- CLASES Y FUNCIONES AUXILIARES ---

class DecimalEncoder(json.JSONEncoder):
    """
    Ayuda a convertir los números de DynamoDB (Decimal) a JSON estándar (int/float).
    Vital para los nuevos campos: release_year y discogs_id.
    """
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super(DecimalEncoder, self).default(obj)

def ofuscar_id_con_tiempo(drive_id):
    """
    Genera el token cifrado para el reproductor.
    Caducidad: 15 minutos (900 segundos).
    """
    if not drive_id: return ""
    
    expiracion = int(time.time()) + 900 
    payload = f"{drive_id}|{expiracion}"
    
    cifrado = []
    key_len = len(SECRET_KEY)
    for i, char in enumerate(payload):
        key_char = SECRET_KEY[i % key_len]
        xor_result = ord(char) ^ ord(key_char)
        cifrado.append(chr(xor_result))
    
    texto_cifrado = "".join(cifrado)
    return base64.b64encode(texto_cifrado.encode("utf-8")).decode("utf-8")

def limpiar_datos(items):
    """
    Prepara la respuesta para el Frontend:
    1. Genera el token de seguridad.
    2. Elimina IDs reales de Google Drive.
    3. MANTIENE los metadatos de Discogs (year, country, style) automáticamente.
    """
    safe_items = []
    for item in items:
        safe_item = item.copy()
        
        # Generar Token Seguro
        raw_id = safe_item.get('drive_file_id')
        if raw_id:
            safe_item['drive_token'] = ofuscar_id_con_tiempo(raw_id)
        
        # BORRAR DATOS SENSIBLES
        safe_item.pop('drive_file_id', None)
        safe_item.pop('web_view_link', None)
        safe_item.pop('search_keywords', None) # Opcional: limpiar keywords internas para ahorrar ancho de banda
        
        safe_items.append(safe_item)
    return safe_items

def response(code, body):
    return {
        'statusCode': code,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Content-Type': 'application/json'
        },
        'body': json.dumps(body, cls=DecimalEncoder)
    }

# --- HANDLER PRINCIPAL ---

def lambda_handler(event, context):
    # Manejo de parámetros queryStringParameters (puede venir None)
    params = event.get('queryStringParameters') or {}
    
    if not params:
        return response(400, {'error': 'Faltan parámetros. Usa ?q=busqueda'})

    try:
        # ---------------------------------------------------------
        # 1. BÚSQUEDA POR ID (Para reproducir directo)
        # ---------------------------------------------------------
        if 'id' in params:
            item = table.get_item(Key={'id': params['id']}).get('Item')
            if item: 
                cleaned_list = limpiar_datos([item])
                return response(200, cleaned_list[0])
            return response(404, {'error': 'ID no encontrado'})

        # ---------------------------------------------------------
        # 2. BÚSQUEDA INTELIGENTE (Parámetro ?q=)
        # ---------------------------------------------------------
        if 'q' in params:
            raw_query = params['q'].strip()
            user_query = raw_query.lower()
            
            if not user_query:
                return response(400, {'error': 'Consulta vacía'})

            # --- ESTRATEGIA A: Atajo por Artista (Rápido y barato) ---
            # Intenta ver si lo que escribieron es exactamente el inicio de un nombre de artista
            try:
                artist_guess = raw_query.title() # Ej: "Rocio" -> "Rocio"
                result_fast = table.query(
                    IndexName='ArtistIndex',
                    KeyConditionExpression=Key('artist').begins_with(artist_guess)
                )
                items_shortcut = result_fast.get('Items', [])
                
                # Si encontramos algo sustancial, retornamos eso y evitamos el SCAN costoso
                if len(items_shortcut) > 0:
                    print(f"🚀 Atajo funcionó para: {artist_guess}")
                    return response(200, limpiar_datos(items_shortcut))
            except Exception as e:
                print(f"⚠️ Atajo falló (continuando con scan): {e}")

            # --- ESTRATEGIA B: Scan Profundo (Búsqueda completa) ---
            print(f"🐢 Iniciando Scan completo para: {user_query}")
            words = user_query.split()
            
            # Construimos filtro dinámico para todas las palabras
            filter_exp = Attr('search_keywords').contains(words[0])
            for w in words[1:]:
                filter_exp = filter_exp & Attr('search_keywords').contains(w)

            # Ejecutamos Scan paginado
            response_scan = table.scan(FilterExpression=filter_exp)
            data = response_scan.get('Items', [])

            # Paginación limitada para proteger la Lambda (max 3MB o 10 pags)
            pages_read = 1
            while 'LastEvaluatedKey' in response_scan:
                if len(data) >= 50: 
                    break # Ya tenemos suficientes resultados
                if pages_read > 5:
                    break # Evitar timeout de Lambda

                response_scan = table.scan(
                    FilterExpression=filter_exp,
                    ExclusiveStartKey=response_scan['LastEvaluatedKey']
                )
                data.extend(response_scan.get('Items', []))
                pages_read += 1

            return response(200, limpiar_datos(data))

        return response(400, {'error': 'Parámetro no soportado. Usa ?q=, ?id= o ?artist='})
    
    except Exception as e:
        print(f"❌ Error CRÍTICO: {str(e)}")
        return response(500, {'error': 'Error interno del servidor', 'details': str(e)})