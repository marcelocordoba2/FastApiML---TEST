from database import database
from mercadolibre import funciones_ml
import json
import psycopg2
from datetime import datetime, timedelta
from mercadolibre import utils
from pandas import json_normalize
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import time, os, logging


carpeta_logs = 'logs'
if not os.path.exists(carpeta_logs):
    os.makedirs(carpeta_logs)
    
ruta_archivo_log = os.path.join(carpeta_logs, f"log_clases{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
logging.basicConfig(filename=ruta_archivo_log, level=logging.ERROR)       

# CLASE NOTIFICACION__________________________________________________________________________________
class notification:
    def __init__(self):
        self.db = HandleDB()
    
    def registrar_notificacion(self,contenido):
        try:
            noti_df = json_normalize(contenido)
            self.db.cargar_notificacion(noti_df)
            return 200
        except Exception as e:
            logging.error(f"Error inesperado: {e}")
            return 400

# CLASE CLIENTE_______________________________________________________________________________________
class cliente:
    def __init__(self):
        self.db = HandleDB()
        
    def nuevo(self, site, code):
        try:
            self.site = site
            self.code = code
            
            datos = self.db.cargar_app(self.site)
            self.app_id = datos[0]
            self.client_secret = datos[1]
            self.redirect_uri = datos[2]
            
            respuesta = funciones_ml.get_token(self)
            respuesta = json.loads(respuesta.text)
            
            self.access_token=respuesta['access_token']
            self.user_id=respuesta['user_id']
            self.refresh_token=respuesta['refresh_token'] 
            
            respuesta=funciones_ml.users_me(self) 
            respuesta = json.loads(respuesta.text)
            self.nickname = respuesta['nickname']
            
            resp = self.db.consulta_cliente(self.user_id)
            if resp[0] == 0:
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                datos = (self.app_id,self.code,self.access_token,self.user_id,self.refresh_token,self.nickname,current_time)
                self.db.insert(datos)
            else:
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                datos = (self.code,self.access_token,self.refresh_token,self.nickname,current_time,self.user_id)
                self.db.update_user(datos)
                
            return 200
        except KeyError as e:
            logging.error(f"Error: Clave faltante - {e}")
            return 400
        except Exception as e:
            logging.error(f"Error inesperado: {e}")
            return 400
        
    def existente(self,user_id):
        try:
            self.user_id = user_id
            
            datos = self.db.datos_conexion_clientes(self.user_id)
            self.app_id = datos[0]
            self.access_token = datos[1]
            self.refresh_token = datos[2]
            self.code = datos[3]
            
            datos = self.db.datos_aplicaciones(self.app_id)
            self.site = datos[0]
            self.client_secret = datos[1]
            self.redirect_uri = datos[2]
            
            respuesta=funciones_ml.users_me(self) 
            respuesta = json.loads(respuesta.text)
            print(respuesta)
            self.nickname = respuesta['nickname']
            
            self.offset=0
            self.limit = 50
            self.fecha_hasta = datetime.today() 
            self.fecha_desde = (datetime.today() - timedelta(days=366))
            
            self.order_id = None
            self.shipping_id = None
            self.inventory_id_id = None
            
            return 200
        except Exception as e:
            logging.error(f"Error inesperado: {e}")
            return 400
            
    def ordenes_historicas(self,user_id):
        
        try:
            self.existente(user_id) if self.user_id != user_id else None
            respuestas=[]
            total = 0
            self.offset = 0
            while self.fecha_desde <= self.fecha_hasta:
                while self.offset <= total:
                    response = funciones_ml.ordenes(self)
                    total = json.loads(response.text)['paging']['total']
                    respuestas.append(response)
                    self.offset+=self.limit
                self.offset = 0
                self.fecha_desde = self.fecha_desde + timedelta(days=1)

            respuestas_convertidas=[]
            for resp in respuestas:
                for resul in json.loads(resp.text)['results']:
                    respuestas_convertidas.append(resul)
            
            ordenes_historicas = json_normalize(respuestas_convertidas)[['id','last_updated','shipping.id','seller.id']].rename(columns=lambda x: x.replace('.', '_')).drop_duplicates(subset='id',keep='first')
            existentes_hist = self.db.ordenes_historicas_existentes(self.user_id)
            ordenes_historicas = ordenes_historicas[~ordenes_historicas['id'].isin(existentes_hist)]
            existentes_orders = self.db.ordenes_existentes(self.user_id)
            ordenes_historicas = ordenes_historicas[~ordenes_historicas['id'].isin(existentes_orders)]
            self.db.cargar_ordenes_historicas(ordenes_historicas)        
            
            return 200
        except Exception as e:
            logging.error(f"Error inesperado: {e}")
            return 400     
     
    def cargar_ordenes(self,user_id,ordenes_a_cargar):
        try:
            self.existente(user_id) if self.user_id == user_id else None
                           
            envios = self.db.listar_envios(self.user_id, ordenes_a_cargar)
            cont=0
            lista = []
            for orden in ordenes_a_cargar:
                self.order_id = orden
                consulta_orden = funciones_ml.consulta_orden(self)
                lista.append(consulta_orden)
                cont+=1
                print (f'proceso... {cont}',end='\r')
            cont_a +=1
            lista_convertida = []
            for lis in lista:
                lista_convertida.append(json.loads(lis.text))
            
            orders = json_normalize(lista_convertida).drop(columns=['order_items','payments','mediations']).rename(columns=lambda x: x.replace('.', '_'))
            payments = json_normalize(lista_convertida,'payments').rename(columns=lambda x: x.replace('.', '_'))
            order_items = json_normalize(lista_convertida,'order_items','id',meta_prefix='order_').drop(columns='item.variation_attributes').rename(columns=lambda x: x.replace('.', '_'))                
            
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            orders['local_last_update']=current_time
            payments['local_last_update']=current_time
            order_items['local_last_update']=current_time
            
            self.db.borrar_order_items(order_items['order_id'])
            if len(envios)>0:
                self.db.borrar_envios(envios)
            self.db.borrar_payments(payments['id'])
            self.db.borrar_ordenes(orders['id'])
            
            self.db.cargar_tablas(orders,payments,order_items)      
        
            return 200
        except Exception as e:
            logging.error(f"Error inesperado: {e}")
            return 400 

    def cargar_envios(self,user_id):
        try:
            self.existente(user_id) if self.user_id == user_id else None
            envios=self.db.chequeo_envios(self.user_id)
            if len(envios)> 0: 
                cont=0
                lista = []
            for envio in envios:
                self.shipping_id = envio
                consulta_envio = funciones_ml.envios(self)
                time.sleep(1)
                lista.append(consulta_envio)
                cont+=1
                print (f'proceso... {cont}',end='\r')
            
            lista_convertida = []
            for elemento in lista:
                lista_convertida.append(json.loads(elemento.text))

            envio_df = json_normalize(lista_convertida).drop(columns=['substatus_history','shipping_items']).rename(columns=lambda x: x.replace('.', '_')) 
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            envio_df['local_last_update']=current_time
            self.db.cargar_envios(envio_df)
            return 200
        except Exception as e:
            logging.error(f"Error inesperado: {e}")
            return 400

    def items(self, user_id):
        try:
            self.existente(user_id) if self.user_id == user_id else None
            
            respuestas=[]
            total = 0
            self.offset = 0
            while self.offset <= total:
                response = funciones_ml.user_items(self,'active')
                total = json.loads(response.text)['paging']['total']
                respuestas.append(response)
                self.offset+=self.limit

            total = 0
            self.offset = 0
            while self.offset <= total:
                response = funciones_ml.user_items(self,'paused')
                total = json.loads(response.text)['paging']['total']
                respuestas.append(response)
                self.offset+=self.limit

            total = 0
            self.offset = 0
            while self.offset <= total:
                response = funciones_ml.user_items(self,'pending')
                total = json.loads(response.text)['paging']['total']
                respuestas.append(response)
                self.offset+=self.limit

            total = 0
            self.offset = 0
            while self.offset <= total:
                response = funciones_ml.user_items(self,'not_yet_active')
                total = json.loads(response.text)['paging']['total']
                respuestas.append(response)
                self.offset+=self.limit

            total = 0
            self.offset = 0
            while self.offset <= total:
                response = funciones_ml.user_items(self,'programmed')
                total = json.loads(response.text)['paging']['total']
                respuestas.append(response)
                self.offset+=self.limit

            total = 0
            self.offset = 0
            while self.offset <= total:
                response = funciones_ml.user_items(self,'closed')
                total = json.loads(response.text)['paging']['total']
                respuestas.append(response)
                self.offset+=self.limit
                
            respuestas_convertidas=[]
            for resp in respuestas:
                for resul in json.loads(resp.text)['results']:
                    respuestas_convertidas.append(resul)

            items = []
            for resp in respuestas_convertidas:
                items.append(json.loads(funciones_ml.item_details(self,resp).text))

            bodys = []
            for it in items:
                bodys.append(it[0]['body'])

            items_df = json_normalize(bodys).drop(columns=['variations','attributes']).rename(columns=lambda x: x.replace('.', '_'))
            atributos = json_normalize(bodys,'attributes','id',meta_prefix='item_').pivot_table(values='value_name',columns='name',index='item_id',aggfunc=lambda x: x.iloc[0]).reset_index()[['item_id','Marca','Modelo','SKU']]
            variaciones = json_normalize(bodys,'variations','id',meta_prefix='item_').drop(columns=['item_relations','attribute_combinations','picture_ids',])

            items_df.drop_duplicates('id',inplace=True)
            variaciones.drop_duplicates('id',inplace=True)
            
            self.db.cargar_items(items,atributos,variaciones)
            return 200
        except Exception as e:
            logging.error(f"Error inesperado: {e}")
            return 400
            
    def preguntas(self,user_id):
        try:
            self.existente(user_id) if self.user_id == user_id else None
            items = self.db.items_vendedor(self.user_id)
            
            lista = []
            for item in items:
                lista.append(funciones_ml.preg_resp(self,item))
            lista_convertida = []
            for respuesta in lista:
                lista_convertida.append(json.loads(respuesta.text))
            lista_convertida = [elemento for elemento in lista_convertida if len(elemento) != 0]
            preguntas = json_normalize(lista_convertida,'questions').rename(columns=lambda x: x.replace('.', '_'))
            
            self.db.cargar_preguntas(preguntas)   
             
            return 200
        except Exception as e:
            logging.error(f"Error inesperado: {e}")
            return 400
        
    def publicidad(self,user_id):
        
        try:
            self.existente(user_id) if self.user_id == None else None
            total = 0 
            campañas_lista = []
            self.offset=0
            while self.offset <= total:
                respuesta = funciones_ml.campañas_usuario(self)
                if respuesta.status_code == 200:
                    campañas_lista.append(respuesta)
                    self.offset+=self.limit
                    total = json.loads(respuesta.text)['paging']['total']
                else:
                    break

            campañas_conv   = []
            for elemento in campañas_lista:
                campañas_conv.append(json.loads(elemento.text))
            campañas = json_normalize(campañas_conv,'results').rename(columns=lambda x: x.replace('.','_'))
            
            anuncios_lista = []
            total = 0 
            self.offset=0
            while self.offset <= total:
                respuesta = funciones_ml.anuncios(self,'marketplace')
                if respuesta.status_code == 200:
                    anuncios_lista.append(respuesta)
                    self.offset+=self.limit
                    total = json.loads(respuesta.text)['paging']['total']
                else:
                    break

            total = 0 
            self.offset=0
            while self.offset <= total:
                respuesta = funciones_ml.anuncios(self,'mshops')
                if respuesta.status_code == 200:
                    anuncios_lista.append(respuesta)
                    self.offset+=self.limit
                    total = json.loads(respuesta.text)['paging']['total']
                else:
                    break

            anuncios_conv   = []
            for elemento in anuncios_lista:
                anuncios_conv.append(json.loads(elemento.text))
                
            anuncios = json_normalize(anuncios_conv,'results')
            anuncios = anuncios[anuncios['campaign_id'] !=0].reset_index(drop=True)
            
            resultados = []
            for index, row in anuncios.iterrows():
                campaña = row['campaign_id']
                MLA = row['id']
                if campaña != 0:
                    fecha_limite=self.fecha_hasta-timedelta(days=90)
                    fecha_inicial = self.fecha_hasta-timedelta(days=1)
                    general = funciones_ml.metrica_anuncio(self,campaña,fecha_limite,fecha_inicial,MLA)
                    cont_gral=0
                    for ele in list(json.loads(general.text)[0].values()):
                        if type(ele) != str:
                            cont_gral+=ele
                    if cont_gral > 0:    
                        while fecha_inicial >= fecha_limite:
                            metrica = funciones_ml.metrica_anuncio(self,campaña,fecha_inicial,fecha_inicial,MLA)
                            cont=0
                            for ele in list(json.loads(metrica.text)[0].values()):
                                if type(ele) != str:
                                    cont+=ele
                            if cont > 0:
                                registro = {
                                    "campaña": campaña,
                                    "fecha": fecha_inicial.strftime('%Y-%m-%d'),
                                    "data": json.loads(metrica.text)[0]  
                                }
                                resultados.append(registro)
                            fecha_inicial=fecha_inicial-timedelta(days=1)
            
            metricas_anuncios = json_normalize(resultados).rename(columns=lambda x: x.replace('.','_'))
            
            self.db.cargar_publicidad(campañas,anuncios,metricas_anuncios)
            return 200
        except Exception as e:
            logging.error(f"Error inesperado: {e}")
            return 400       
     
    def act_preguntas(self,user_id,preguntas):
        try:
            self.existente(user_id) if self.user_id == user_id else None
            
            lista = []
            for preg in preguntas:
                lista.append(funciones_ml.pregunta(self,preg))
            lista_convertida = []
            for respuesta in lista:
                lista_convertida.append(json.loads(respuesta.text))
            preguntas_df = json_normalize(lista_convertida,'questions').rename(columns=lambda x: x.replace('.', '_'))
            existentes = self.db.preguntas_existentes(self.user_id,preguntas)
            preguntas_df = preguntas_df[~preguntas_df['id'].isin(existentes)]

            self.db.cargar_preguntas(preguntas_df)   
             
            return 200
        except Exception as e:
            logging.error(f"Error inesperado: {e}")
            return 400
            
    def act_items(self,user_id,item): ## PENDIENTE DE TERMINAR
        try:
            self.existente(user_id) if self.user_id == user_id else None
                        
            return 200
        except Exception as e:
            logging.error(f"Error inesperado: {e}")
            return 400
        

# CLASE ADMINISTRACION DB_________________________________________________________________________________

class HandleDB():
    def __init__(self):
        conn_str = database.conexion()
        self._con = psycopg2.connect(conn_str)
        self._cur = self._con.cursor()
        self._engine = create_engine(conn_str)

### consultas generales___________________________________________________________________________________

    def usuarios_con_notificaciones(self):
        self._cur.execute(f"SELECT distinct(user_id) FROM public.notificaciones where revisado = false")
        data = self._cur.fetchall()
        return data
    
    def cliente_pendiente_carga(self):
        self._cur.execute(f"select user_id from conexion_clientes left join ordenes_historial on conexion_clientes.user_id = ordenes_historial.seller_id where ordenes_historial.seller_id is null")
        data = self._cur.fetchall()
        lista = []
        for cliente in data:
            lista.append(cliente[0])
        return lista
    
    def cliente_ordenes_pendientes(self):
        self._cur.execute(f"select ordenes_historial.seller_id from ordenes_historial left join orders on ordenes_historial.id = orders.id where orders.id is null")
        data = self._cur.fetchall()
        lista= []
        for cliente in data:
            lista.append(cliente[0])
        return lista
    
### consultas de usuario__________________________________________________________________________________

    def consulta_cliente(self,user_id):
        self._cur.execute(f"select count(*) from conexion_clientes where user_id = {str(user_id)}")
        data = self._cur.fetchone()
        return data

    def conteo_notificaciones(self,user_id):
        self._cur.execute(f"SELECT topic,count(*) FROM public.notificaciones where user_id = {str(user_id)} group by topic;")
        data = self._cur.fetchall()
        diccionario = dict(data)
        return diccionario
    
    def listar_notificaciones(self,user_id):
        self._cur.execute(f"SELECT _id FROM public.notificaciones where user_id = {str(user_id)} and topic = 'orders_v2' and revisado = false")
        data = self._cur.fetchall()
        lista = []
        for noti in data:
            lista.append(noti[0])
        return lista
   
    def chequeo_envios(self,user_id):
        self._cur.execute(f"SELECT distinct(shipping_id) FROM orders LEFT JOIN envios on orders.shipping_id = envios.id WHERE orders.seller_id = {str(user_id)} and envios.id IS NULL;")
        data = self._cur.fetchall()
        lista = []
        for envio in data:
            lista.append(envio[0])
        return lista
 
    def chequeo_ordenes(self,user_id):
        self._cur.execute(f"select ordenes_historial.id from ordenes_historial left join orders on ordenes_historial.id = orders.id where ordenes_historial.seller_id = {str(user_id)} and orders.id is null")
        data = self._cur.fetchall()
        lista= []
        for orden in data:
            lista.append(orden[0])
        return lista
        
    def datos_conexion_clientes(self,user_id):
        self._cur.execute(f"select app_id, access_token, refresh_token, code from conexion_clientes where user_id = {str(user_id)}")
        data = self._cur.fetchone()
        return data

    def ordenes_historicas_existentes(self,user_id):
        self._cur.execute(f"select id from ordenes_historial where seller_id = {str(user_id)}")
        data = self._cur.fetchall()
        lista= []
        for orden in data:
            lista.append(orden[0])
        return lista

    def ordenes_existentes(self,user_id):
        self._cur.execute(f"select id from orders where seller_id = {str(user_id)}")
        data = self._cur.fetchall()
        lista= []
        for orden in data:
            lista.append(orden[0])
        return lista


### gestion de alta de cliente__________________________________________________________________________________
   
    def cargar_app(self,site):
        self._cur.execute(f"select app_id, client_secret, uri from aplicaciones where site = '{site}'")
        data = self._cur.fetchone()
        return data
    
    def update_user(self, datos):
        update_query = """
            UPDATE conexion_clientes 
            SET code = %s, access_token = %s, refresh_token = %s,nickname = %s, last_updated = %s
            WHERE user_id = %s
        """
        self._cur.execute(update_query, datos)
        self._con.commit()

    def insert(self, datos):
        self._cur.execute(
            "INSERT INTO conexion_clientes VALUES(%s, %s, %s, %s, %s, %s)",
            datos
        )
        self._con.commit()

    def datos_aplicaciones(self,aplicacion):
        self._cur.execute(f"select site, client_secret, uri from aplicaciones where app_id = {str(aplicacion)}")
        data = self._cur.fetchone()
        return data
   
### gestion de notificaciones____________________________________________________________________________________    
 
    def cargar_notificacion(self,notificacion):
        try:
            with self._engine.connect() as connection: 
                notificacion.to_sql('notificaciones', connection, index=False, if_exists='append',method='multi')
        except Exception as e:
            self._con.rollback()
            raise

    def listar_ordenes(self,user_id,notificaciones):
        self._cur.execute(f"SELECT distinct(resource) FROM public.notificaciones where user_id = {str(user_id)} and _id in {notificaciones}")
        data = self._cur.fetchall()
        lista = []
        for orden in data:
            lista.append(orden[0][-16:])
        return lista

    def marcar_notificaciones(self,notificaciones):
        
        placeholders = ', '.join(['%s' for _ in notificaciones])

        # Construye la consulta con la cláusula IN y los marcadores de posición
        update_query = f"""
            UPDATE public.notificaciones 
            SET revisado = true
            WHERE _id IN ({placeholders})
        """

        # Ejecuta la consulta con los valores de notificaciones
        self._cur.execute(update_query, notificaciones)
        self._con.commit()
        return 200
    
    def clientes_con_notificaciones(self):
        
        self._cur.execute(f"select distinct(user_id) from notificaciones where revisado = false")
        data = self._cur.fetchall()
        lista = []
        for cliente in data:
            lista.append(cliente[0])
        return lista
    
    def check_orders_v2(self,user_id):
        self._cur.execute(f"select _id from notificaciones where user_id = {str(user_id)} and topic = 'orders_v2' and revisado = false")
        data = self._cur.fetchall()
        lista = []
        for envio in data:
            lista.append(envio[0])
        return lista

    def check_questions(self,user_id):
        self._cur.execute(f"select _id from notificaciones where user_id = {str(user_id)} and topic = 'questions' and revisado = false")
        data = self._cur.fetchall()
        lista = []
        for envio in data:
            lista.append(envio[0])
        return lista
    
    def check_fbm_stock_operations(self,user_id):
        self._cur.execute(f"select _id from notificaciones where user_id = {str(user_id)} and topic = 'fbm_stock_operations' and revisado = false")
        data = self._cur.fetchall()
        lista = []
        for envio in data:
            lista.append(envio[0])
        return lista

    def check_shipments(self,user_id):
        self._cur.execute(f"select _id from notificaciones where user_id = {str(user_id)} and topic = 'shipments' and revisado = false")
        data = self._cur.fetchall()
        lista = []
        for envio in data:
            lista.append(envio[0])
        return lista    
    
    def check_flex_handshakes(self,user_id):
        self._cur.execute(f"select _id from notificaciones where user_id = {str(user_id)} and topic = 'flex-handshakes' and revisado = false")
        data = self._cur.fetchall()
        lista = []
        for envio in data:
            lista.append(envio[0])
        return lista    
    
    def check_items(self,user_id):
        self._cur.execute(f"select _id from notificaciones where user_id = {str(user_id)} and topic = 'items' and revisado = false")
        data = self._cur.fetchall()
        lista = []
        for envio in data:
            lista.append(envio[0])
        return lista       
    
    def ids_order_v2(self,user_id, notificaciones):
        
        self._cur.execute(f"select distinct(resource) from notificaciones where user_id = {str(user_id)} and topic = 'orders_v2' and revisado = false and _id in {tuple(notificaciones)}")
        data = self._cur.fetchall()
        lista = []
        for orden in data:
            lista.append(orden[0][-16:])
        return lista
    
    def ids_questions(self,user_id, notificaciones):
        
        self._cur.execute(f"select distinct(resource) from notificaciones where user_id = {str(user_id)} and topic = 'questions' and revisado = false and _id in {tuple(notificaciones)}")
        data = self._cur.fetchall()
        lista = []
        for orden in data:
            lista.append(orden[0][-11:])
        return lista    
    
    def ids_fbm_stock_operations(self,user_id, notificaciones):
        
        self._cur.execute(f"select distinct(resource) from notificaciones where user_id = {str(user_id)} and topic = 'fbm_stock_operations' and revisado = false and _id in {tuple(notificaciones)}")
        data = self._cur.fetchall()
        lista = []
        for orden in data:
            lista.append(orden[0][-11:])
        return lista 

    def ids_shipments(self,user_id, notificaciones):
        
        self._cur.execute(f"select distinct(resource) from notificaciones where user_id = {str(user_id)} and topic = 'shipments' and revisado = false and _id in {tuple(notificaciones)}")
        data = self._cur.fetchall()
        lista = []
        for orden in data:
            lista.append(orden[0][-11:])
        return lista  
    
    def ids_flex_handshakes(self,user_id, notificaciones):
        
        self._cur.execute(f"select distinct(resource) from notificaciones where user_id = {str(user_id)} and topic = 'flex_handshakes' and revisado = false and _id in {tuple(notificaciones)}")
        data = self._cur.fetchall()
        lista = []
        for orden in data:
            lista.append(orden[0][-11:])
        return lista    

    def ids_items(self,user_id, notificaciones):
        
        self._cur.execute(f"select distinct(resource) from notificaciones where user_id = {str(user_id)} and topic = 'items' and revisado = false and _id in {tuple(notificaciones)}")
        data = self._cur.fetchall()
        lista = []
        for orden in data:
            lista.append(orden[0][-11:])
        return lista  

 
### ordenes_______________________________________________________________________________________________________

    def cargar_ordenes_historicas(self,ordenes):
        try:
            with self._engine.connect() as connection: 
                ordenes.to_sql('ordenes_historial', connection, index=False, if_exists='append',method='multi')
        except Exception as e:
            self._con.rollback()
            raise
    
    def listar_envios(self,user_id,ordenes):
        self._cur.execute(f"SELECT distinct(shipping_id) FROM public.orders where seller_id = {str(user_id)} and id in {tuple(ordenes)}")
        data = self._cur.fetchall()
        lista = []
        for envio in data:
            lista.append(envio[0])
        return lista

    def cargar_tablas(self,orders,payments,order_items):
        try:
            with self._engine.connect() as connection: 
                orders.to_sql('orders', connection, index=False, if_exists='append',method='multi')
                payments.to_sql('payments', connection, index=False, if_exists='append',method='multi')
                order_items.to_sql('order_items', connection, index=False, if_exists='append',method='multi')
        except Exception as e:
            self._con.rollback()
            raise
        
    def cargar_envios(self,envios):
        try:
            with self._engine.connect() as connection: 
                envios.to_sql('envios', connection, index=False, if_exists='append',method='multi')
        except Exception as e:
            self._con.rollback()
            raise
    
    def borrar_order_items(self,ordenes):
        self._cur.execute(f"DELETE FROM public.order_items where order_items.order_id in {tuple(ordenes)}")
        self._con.commit()
        return 200
    
    def borrar_payments(self,pagos):
        self._cur.execute(f"DELETE FROM public.payments where payments.id in {tuple(pagos)}")
        self._con.commit()
        return 200
    
    def borrar_ordenes(self,ordenes):
        self._cur.execute(f"DELETE FROM public.orders where orders.id in {tuple(ordenes)}")
        self._con.commit()
        return 200
    
    def borrar_envios(self,envios):
        self._cur.execute(f"DELETE FROM public.envios where id in {tuple(envios)}")
        self._con.commit()
        return 200
 
  
### items _______________________________________________________________________________________________________

    def cargar_items(self,items,atributos,variaciones):
        try:
            with self._engine.connect() as connection: 
                items.to_sql('items', connection, index=False, if_exists='append')
                atributos.to_sql('atributos', connection, index=False, if_exists='append')    
                variaciones.to_sql('variaciones', connection, index=False, if_exists='append')
        except Exception as e:
            self._con.rollback()
            raise
    
    def items_vendedor(self, user_id):
        self._cur.execute(f"select id from items where seller_id = {str(user_id)}")
        data = self._cur.fetchall()
        lista= []
        for item in data:
            lista.append(item[0])
        return lista

### preguntas__ __________________________________________________________________________________________________

    def cargar_preguntas(self,preguntas):
        try:
            with self._engine.connect() as connection: 
                preguntas.to_sql('preguntas', connection, index=False, if_exists='append')
        except Exception as e:
            self._con.rollback()
            raise

    def preguntas_existentes(self,user_id,preguntas):
        self._cur.execute(f"select id from preguntas where seller_id = {str(user_id)} and id in {tuple(preguntas)}")
        data = self._cur.fetchall()
        lista= []
        for preg in data:
            lista.append(preg[0])
        return lista

### publicidad ___________________________________________________________________________________________________

    def cargar_publicidad(self,campañas,anuncios,metricas):
        try:
            with self._engine.connect() as connection: 
                campañas.to_sql('campañas', connection, index=False, if_exists='append',method='multi')
                anuncios.to_sql('anuncios', connection, index=False, if_exists='append',method='multi')
                metricas.to_sql('metricas_anuncios', connection, index=False, if_exists='append',method='multi')
        except Exception as e:
            self._con.rollback()
            raise
        
    def __del__(self):
        self._con.close()