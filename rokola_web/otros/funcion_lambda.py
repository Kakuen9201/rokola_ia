import json
import boto3
import base64
import time
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal

# --- CONFIGURACIÓN DE SEGURIDAD ---
SECRET_KEY = "logoscontexto"

def ofuscar_id_con_tiempo(drive_id):
    """
    Crea un token cifrado XOR que incluye: ID + Separador(|) + Timestamp de expiración.
    """
    if not drive_id: return ""
    
    # 🔥 CAMBIO AQUÍ: 15 Minutos (15 * 60 = 900 segundos)
    # Antes era 3600 (1 hora)
    expiracion = int(time.time()) + 900 
    
    # 2. Creamos el payload compuesto
    payload = f"{drive_id}|{expiracion}"
    
    # 3. Cifrado XOR
    cifrado = []
    key_len = len(SECRET_KEY)
    for i, char in enumerate(payload):
        key_char = SECRET_KEY[i % key_len]
        xor_result = ord(char) ^ ord(key_char)
        cifrado.append(chr(xor_result))
    
    texto_cifrado = "".join(cifrado)
    
    # 4. Convertimos a Base64
    return base64.b64encode(texto_cifrado.encode("utf-8")).decode("utf-8")

def limpiar_datos(items):
    """
    Recibe una lista de items de DynamoDB, genera el token seguro
    y elimina los IDs/Links originales antes de responder.
    """
    safe_items = []
    for item in items:
        # Creamos una copia para no alterar el objeto original si se usara luego
        safe_item = item.copy()
        
        # Generar Token Seguro
        raw_id = safe_item.get('drive_file_id')
        safe_item['drive_token'] = ofuscar_id_con_tiempo(raw_id)
        
        # BORRAR DATOS SENSIBLES (Ya no viajan al navegador)
        safe_item.pop('drive_file_id', None)
        safe_item.pop('web_view_link', None)
        
        safe_items.append(safe_item)
    return safe_items

# Helper para decimales
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj)
        return super(DecimalEncoder, self).default(obj)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('MusicaStartup')

def lambda_handler(event, context):
    params = event.get('queryStringParameters', {})
    
    # Si no hay parámetros (petición vacía), retornamos error o lista vacía según prefieras
    if not params:
        return response(400, {'error': 'Faltan parámetros'})

    try:
        # 1. BÚSQUEDA POR ID (Caso especial: devuelve un solo objeto)
        if 'id' in params:
            item = table.get_item(Key={'id': params['id']}).get('Item')
            if item: 
                # Limpiamos el item único pasándolo como lista y sacándolo después
                cleaned_list = limpiar_datos([item])
                return response(200, cleaned_list[0])
            return response(404, {'error': 'ID no encontrado'})

        # 2. BÚSQUEDA EXACTA POR ARTISTA (Índice)
        if 'artist' in params:
            items = []
            result = table.query(IndexName='ArtistIndex', KeyConditionExpression=Key('artist').eq(params['artist']))
            items.extend(result.get('Items', []))
            
            while 'LastEvaluatedKey' in result:
                result = table.query(IndexName='ArtistIndex', KeyConditionExpression=Key('artist').eq(params['artist']), ExclusiveStartKey=result['LastEvaluatedKey'])
                items.extend(result.get('Items', []))
            
            # LIMPIAR ANTES DE RESPONDER
            return response(200, limpiar_datos(items))

        # 3. BÚSQUEDA EXACTA POR TÍTULO
        if 'song_name' in params:
            result = table.query(IndexName='TitleIndex', KeyConditionExpression=Key('clean_title').eq(params['song_name']))
            # LIMPIAR ANTES DE RESPONDER
            return response(200, limpiar_datos(result.get('Items', [])))

        # =========================================================
        # 4. BÚSQUEDA INTELIGENTE (ATAJO + SCAN COMPLETO)
        # =========================================================
        if 'q' in params:
            raw_query = params['q'].strip()
            user_query = raw_query.lower()
            
            # --- ESTRATEGIA A: Intentar adivinar el Artista (Rápido) ---
            try:
                artist_guess = raw_query.title()
                result_fast = table.query(
                    IndexName='ArtistIndex',
                    KeyConditionExpression=Key('artist').begins_with(artist_guess)
                )
                items_shortcut = result_fast.get('Items', [])
                
                if len(items_shortcut) > 0:
                    print(f"🚀 Atajo 'begins_with' funcionó para: {artist_guess}")
                    # LIMPIAR ANTES DE RESPONDER
                    return response(200, limpiar_datos(items_shortcut))
            except Exception as e:
                print(f"Atajo falló, ignorando: {e}")

            # --- ESTRATEGIA B: Scan Profundo ---
            print("🐢 Iniciando Scan completo con paginación...")
            words = user_query.split()
            if not words: return response(400, {'error': 'Consulta vacía'})

            filter_exp = Attr('search_keywords').contains(words[0])
            for w in words[1:]:
                filter_exp = filter_exp & Attr('search_keywords').contains(w)

            response_scan = table.scan(FilterExpression=filter_exp)
            data = response_scan.get('Items', [])

            pages_read = 1
            while 'LastEvaluatedKey' in response_scan:
                if len(data) >= 50: 
                    print("✋ Límite de 50 resultados alcanzado.")
                    break
                
                if pages_read > 10:
                    print("✋ Límite de páginas de escaneo alcanzado.")
                    break

                print(f"📖 Leyendo página {pages_read + 1}...")
                response_scan = table.scan(
                    FilterExpression=filter_exp,
                    ExclusiveStartKey=response_scan['LastEvaluatedKey']
                )
                data.extend(response_scan.get('Items', []))
                pages_read += 1

            # LIMPIAR ANTES DE RESPONDER
            return response(200, limpiar_datos(data))

        return response(400, {'error': 'Parámetro no soportado'})
    
    except Exception as e:
        print(f"Error CRÍTICO: {str(e)}")
        return response(500, {'error': str(e)})

def response(code, body):
    return {
        'statusCode': code,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Content-Type': 'application/json'
        },
        'body': json.dumps(body, cls=DecimalEncoder)
    }