import json
import boto3
import base64
import time
import os
from boto3.dynamodb.conditions import Key
from decimal import Decimal

# --- CONFIGURACIÓN DE SEGURIDAD ---
# Usa una variable de entorno en Lambda o escribe la clave aquí (mismo valor que en tu JS)
SECRET_KEY = os.environ.get('SECRET_KEY', 'logoscontexto') 

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('MusicaStartup')

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def ofuscar_id(drive_id):
    """Genera un token temporal cifrado simple (XOR)"""
    try:
        # 1. Crear payload con expiración (ej. 1 hora)
        expiracion = int(time.time()) + 3600 
        payload = f"{drive_id}|{expiracion}"
        
        # 2. Cifrado XOR simple (coincide con tu frontend)
        resultado = []
        key_len = len(SECRET_KEY)
        for i in range(len(payload)):
            char_code = ord(payload[i]) ^ ord(SECRET_KEY[i % key_len])
            resultado.append(chr(char_code))
        
        token_str = "".join(resultado)
        # 3. Base64 para que viaje limpio
        return base64.b64encode(token_str.encode('utf-8')).decode('utf-8')
    except Exception as e:
        print(f"Error ofuscando: {e}")
        return None

def lambda_handler(event, context):
    query_param = None
    if event.get('queryStringParameters'):
        query_param = event['queryStringParameters'].get('q')
    elif event.get('body'):
        try:
            body = json.loads(event['body'])
            query_param = body.get('q')
        except:
            pass

    if not query_param:
        return {'statusCode': 400, 'body': json.dumps({'error': 'Falta q'})}

    search_term = query_param.strip()
    print(f"🔎 Buscando: '{search_term}'")

    try:
        items = []
        
        # 1. Búsqueda por ARTISTA (Index nuevo)
        response = table.query(
            IndexName='BusquedaGlobal',
            KeyConditionExpression=Key('tipo').eq('CATALOGO') & Key('artist').begins_with(search_term)
        )
        items = response.get('Items', [])

        # 2. Búsqueda por TÍTULO (Fallback)
        if len(items) == 0:
            print("   -> Intentando por Título exacto...")
            response_title = table.query(
                IndexName='TitleIndex',
                KeyConditionExpression=Key('clean_title').eq(search_term)
            )
            items.extend(response_title.get('Items', []))

        # --- 🛡️ CAPA DE SEGURIDAD Y LIMPIEZA ---
        items_seguros = []
        for item in items:
            drive_id = item.get('drive_file_id')
            
            # Si tiene ID, generamos el token seguro
            if drive_id:
                item['drive_token'] = ofuscar_id(drive_id)
            
            # 🚨 IMPORTANTE: Borrar el ID original y enlaces directos
            item.pop('drive_file_id', None)
            item.pop('web_view_link', None) # Si tenías el link directo de view, bórralo también
            item.pop('search_keywords', None) # No hace falta enviarlo al front
            
            # Solo agregamos si logramos generar token (o si es metadata pública)
            items_seguros.append(item)

        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'body': json.dumps(items_seguros, cls=DecimalEncoder)
        }

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': str(e)})
        }