from database import database
from datetime import datetime, timedelta
from mercadolibre import funciones_ml
import json
import traceback
from models import clases
import os

## devuelve objeto parametros con el nuevo access_token y refresh token, ademas de actualizar los mismos en la base de datos
def actualizacion_token(parametros):
    try:
        nuevo_token = funciones_ml.refresh_token(parametros)
        if nuevo_token.status_code == 200:
            nuevo_token=json.loads(nuevo_token.text)
            parametros.access_token = nuevo_token['access_token']
            parametros.refresh_token = nuevo_token['refresh_token']
            query = "UPDATE conexion_clientes SET access_token = '" + str(parametros.access_token) + "', refresh_token = '" + str(parametros.refresh_token) + "' WHERE user_id = '" + str(parametros.user_id) + "';"
            database.execute_update_query(query, parametros.conn_str)
            parametros.status_actualizacion = 200
        else:
            parametros.status_actualizacion = nuevo_token.status_code
    except Exception as e:
        traceback.print_exc() 
        return e

