from models.nuevas_clases import cliente
from models.clases import HandleDB
import os, logging
from datetime import datetime
carpeta_logs = 'logs'
if not os.path.exists(carpeta_logs):
    os.makedirs(carpeta_logs)
    
ruta_archivo_log = os.path.join(carpeta_logs, f"log_pruebas{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
logging.basicConfig(filename=ruta_archivo_log, level=logging.ERROR)
db = HandleDB()

def carga_inicial():
    try: 
        clientes = db.cliente_pendiente_carga()

        for cli in clientes:
            cliente_actual = cliente()
            cliente_actual.existente(cli)
            cliente_actual.ordenes_historicas(cliente_actual.user_id)
            ordenes = db.chequeo_ordenes(cliente_actual.user_id)
            cliente_actual.cargar_ordenes(cliente_actual.user_id,ordenes)
            cliente_actual.cargar_envios(cliente_actual.user_id)
            cliente_actual.items(cliente_actual.user_id)
            cliente_actual.preguntas(cliente_actual.user_id)
            cliente_actual.publicidad(cliente_actual.user_id)
        return 200
    except Exception as e:
            logging.error(f"Error inesperado: {e}")
            return 400     

def procesar_notificaciones():
    try:
        clientes = db.clientes_con_notificaciones()
        for cli in clientes:
            cliente_actual = cliente()
            ordenes_not=cliente_actual.db.check_orders_v2(cliente_actual.user_id)
            preguntas=cliente_actual.db.check_questions(cliente_actual.user_id)
            stock_full=cliente_actual.db.check_fbm_stock_operations(cliente_actual.user_id)
            envios=cliente_actual.db.check_shipments(cliente_actual.user_id)
            flex=cliente_actual.db.check_flex_handshakes(cliente_actual.user_id)
            items=cliente_actual.db.check_items(cliente_actual.user_id)
            
            if len(ordenes_not) > 0:
                ordenes = cliente_actual.db.ids_order_v2(cliente_actual.user_id,ordenes_not)
                cliente_actual.cargar_ordenes(cliente_actual.user_id,ordenes)
                cliente_actual.cargar_envios(cliente_actual.user_id)
                
            if len(preguntas) > 0:
                id_preguntas = cliente_actual.db.ids_questions(cliente_actual.user_id,preguntas)
                cliente_actual.act_preguntas(cliente_actual.user_id,id_preguntas)
            
            if len(items) > 0:
                items_ids = cliente_actual.db.ids_items(cliente_actual.user_id,items)
                cliente_actual.
    
    
    return 200
    except Exception as e:
            logging.error(f"Error inesperado: {e}")
            return 400 
    

    