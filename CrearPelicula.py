import boto3
import uuid
import os
import json
import traceback

def lambda_handler(event, context):
    #  imprimos un JSON estandarizado en CloudWatch
    def _log(tipo, log_datos):
        try:
            print(json.dumps({"tipo": tipo, "log_datos": log_datos}, ensure_ascii=False))
        except Exception:
            # Fallback por si hay datos no serializables
            try:
                print(json.dumps({"tipo": tipo, "log_datos": str(log_datos)}, ensure_ascii=False))
            except Exception:
                # Último recurso
                print('{"tipo":"%s","log_datos":"<unserializable>"}' % tipo)

    try:
        # Entrada (json)
        _log("INFO", {"message": "received_event", "event": event})

        # Manejo del body (JSON o dict)
        body = event.get('body', {}) if isinstance(event, dict) else {}
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except Exception:
                # No es JSON decodificable
                body = {}

        tenant_id = body.get('tenant_id')
        pelicula_datos = body.get('pelicula_datos')
        nombre_tabla = os.environ.get("TABLE_NAME")

        if not tenant_id or pelicula_datos is None:
            raise ValueError("Falta 'tenant_id' o 'pelicula_datos' en el body del evento")
        if not nombre_tabla:
            raise ValueError("Variable de entorno TABLE_NAME no definida")

        # Proceso
        uuidv4 = str(uuid.uuid4())
        pelicula = {
            'tenant_id': tenant_id,
            'uuid': uuidv4,
            'pelicula_datos': pelicula_datos
        }
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(nombre_tabla)
        response = table.put_item(Item=pelicula)

        # Salida (json)
        _log("INFO", {"message": "item_created", "pelicula": pelicula, "response": str(response)})

        return {
            'statusCode': 200,
            'pelicula': pelicula,
            'response': response
        }

    except Exception as e:
        # Log de error estandarizado
        err_data = {
            'error': str(e),
            'trace': traceback.format_exc()
        }
        try:
            err_data['event'] = event
        except Exception:
            err_data['event'] = str(event)

        _log("ERROR", err_data)

        return {
            'statusCode': 500,
            'body': {'error': str(e)}
        }
