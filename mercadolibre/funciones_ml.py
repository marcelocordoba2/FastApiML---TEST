import requests
import time
from mercadolibre import utils
from datetime import datetime, timedelta


## cambiar code generado por la autorizacion del cliente, por token
def get_token(parametros):
        
    url = "https://api.mercadolibre.com/oauth/token"

    payload = 'grant_type=authorization_code&client_id=' + str(parametros.app_id) + '&client_secret=' + parametros.client_secret + '&code=' + (parametros.code) + '&redirect_uri=' + parametros.redirect_uri + '&code_verifier==%24CODE_VERIFIER'
    headers = {
    'accept': 'application/json',
    'content-type': 'application/x-www-form-urlencoded'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    return response

## actualizar refresh token
def refresh_token(parametros):
       
    url = "https://api.mercadolibre.com/oauth/token"
    payload = 'grant_type=refresh_token&client_id=' + str(parametros.app_id) + '&client_secret=' + str(parametros.client_secret) + '&refresh_token=' + str(parametros.refresh_token)
    headers = {
        'accept': 'application/json',
        'content-type': 'application/x-www-form-urlencoded'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    return response

## obtener informacion del usuario
def users_me(parametros):
    url = "https://api.mercadolibre.com/users/me/?attributes=nickname"
    headers = {'Authorization': f'Bearer {parametros.access_token}'}
    response = requests.request("GET", url, headers=headers, data={})
    return response

def GET(url,headers,payload,parametros):
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        response.raise_for_status()  
        return response
    except requests.exceptions.HTTPError as err:
        if err.response.status_code == 401:
            utils.actualizacion_token(parametros) 
            headers['Authorization'] = f'Bearer {parametros.access_token}'
            response = requests.get(url, headers=headers, data=payload)
            response.raise_for_status()
            return response
        elif err.response.status_code == 429:
            print("Error 429: Too Many Requests. Esperando 2 minutos antes de volver a intentar...")
            time.sleep(120)  # Pausa el programa durante 2 minutos (en segundos)
            response = requests.get(url, headers=headers, data=payload)
            response.raise_for_status()
            return response
        else:
            print(f"Error HTTP: {err}")
            raise

def stock_cargo(parametros):
    fecha= datetime.now()
    mes = datetime(fecha.year, fecha.month, 1).strftime('%Y-%m-%d')

    url = f"https://api.mercadolibre.com/billing/integration/periods/key/{mes}/group/ML/full/details?document_type=BILL&limit={str(parametros.limit)}&offset={str(parametros.offset)}"

    payload = {}
    headers = {
    'Authorization': f'Bearer {parametros.access_token}'
    }
    response = GET(url, headers, payload, parametros)
    return response

## Devuelve las ordenes generadas con los criterios de fecha, offset y limit
def ordenes(parametros):
    url = "https://api.mercadolibre.com/orders/search?offset=" + str(parametros.offset) + "&limit=" + str(parametros.limit) + "&sort=date_asc&seller=" + str(parametros.user_id) + "&order.date_created.from=" + parametros.fecha_desde.strftime('%Y-%m-%d') + "T00:00:00.000-04:00&order.date_created.to=" + parametros.fecha_desde.strftime('%Y-%m-%d') + "T23:59:59.999-04:00"
    headers = {'Authorization': 'Bearer ' + parametros.access_token}
    payload = {}
    response = GET(url, headers, payload, parametros)
    return response

## Devuelve informacion de una sola orden
def consulta_orden(parametros):
    url = "https://api.mercadolibre.com/orders/" + str(parametros.order_id)
    headers = {'Authorization': 'Bearer ' + parametros.access_token}
    payload = {}
    response = GET(url, headers, payload, parametros)
    return response

## devuelve informacion de un envio en particular
def envios(parametros):
    url = "https://api.mercadolibre.com/shipments/" + str(parametros.shipping_id)
    headers = {'Authorization': 'Bearer ' + parametros.access_token}
    payload = {}
    response = GET(url, headers, payload, parametros)
    return response

## Llamada a listado de publicaciones de un seller
def items_vendedor(parametros):
    url = "https://api.mercadolibre.com/users/" + str(parametros.user_id) + "/items/search?offset=" + str(parametros.offset) + "&limit=" + str(parametros.limit)
    headers = {'Authorization': 'Bearer ' + parametros.access_token}
    payload = {}
    response = GET(url, headers, payload, parametros)
    return response

## obtiene preguntas y respuestas
def preg_resp(parametros,MLA):
    url = f"https://api.mercadolibre.com/questions/search?item={MLA}&api_version=4"
    headers = {'Authorization': 'Bearer ' + parametros.access_token}
    payload = {}
    response = GET(url, headers, payload, parametros)
    return response

def pregunta(parametros,id):
    url = f"https://api.mercadolibre.com/questions/{str(id)}?api_version=4"
    headers = {'Authorization': 'Bearer ' + parametros.access_token}
    payload = {}
    response = GET(url, headers, payload, parametros)
    return response

## obtener el costo de un venta
def costos(parametros):
    url = "https://api.mercadolibre.com/sites/" + parametros.site + "/listing_prices?price=" + str(parametros.price)
    headers = {'Authorization': 'Bearer ' + parametros.access_token}
    payload = {}
    response = GET(url, headers, payload, parametros)
    return response

## obtener campañas por usuario 
def campañas_usuario(parametros):
    url = f"https://api.mercadolibre.com/advertising/product_ads_2/campaigns/search?user_id={str(parametros.user_id)}&offset={str(parametros.offset)}&limit={str(parametros.limit)}"
    headers = {'Authorization': f'Bearer {parametros.access_token}'}
    payload = {}
    response = GET(url, headers, payload, parametros)
    return response

## obtener stock en fulfillment
def stock_full(parametros):
    url = "https://api.mercadolibre.com/inventories/" + parametros.inventory_id + "/stock/fulfillment"
    headers = {'Authorization': 'Bearer ' + parametros.access_token}
    payload = {}
    response = GET(url, headers, payload, parametros)
    return response

## consultar un item
def consulta_item(parametros):
    url = "https://api.mercadolibre.com/items/" + parametros.item
    headers = {'Authorization': 'Bearer ' + parametros.access_token}
    payload = {}
    response = GET(url, headers, payload, parametros)
    return response

def arbol_categorias(parametros):
  
    url = "https://api.mercadolibre.com/sites/" + parametros.site + "/categories/all?withAttributes=true>" + parametros.site.lower() + ".gz"

    payload = {}
    headers = {
        'Authorization': 'Bearer ' + parametros.access_token
        }

    response = GET(url, headers, payload, parametros)
    return response

def user_items(parametros,status):
    url = f'https://api.mercadolibre.com/users/{str(parametros.user_id)}/items/search?orders=start_time_desc&offset={str(parametros.offset)}&limit={str(parametros.limit)}&status={status}'
    headers = {
    'Authorization': f'Bearer {parametros.access_token}'
    }
    payload = {}
    response = GET(url, headers, payload, parametros)
    return response

def item_details(parametros,mla):
    
    url = f"https://api.mercadolibre.com/items?ids={mla}&attributes=id,status,date_created,last_updated,catalog_product_id,site_id,catalog_product_id,listing_type_id,category_id,inventory_id,seller_id,official_store_id, substatus, oficial_store_name,title,permalink,thumbnail,condition,accepts_mercadopago,currency_id,catalog_listing,available_quantity,price,original_price,sale_price,differential_pricing,tags,channels,installments,variations,shipping,attributes"
    headers = {
        'Authorization': f'Bearer {parametros.access_token}'
    }
    payload = {}
    response = GET(url, headers, payload, parametros)
    return response

def envio(parametros,envio):
    url = f"https://api.mercadolibre.com/shipments/{str(envio)}"

    payload = {}
    headers = {
    'Authorization': f'Bearer {parametros.access_token}'
    }

    response = GET(url, headers, payload, parametros)
    return response

def anuncios(parametros,canal):
    url = f"https://api.mercadolibre.com/advertising/product_ads/ads/search?user_id={str(parametros.user_id)}&limit={str(parametros.limit)}&offset={str(parametros.offset)}&channel={str(canal)}"

    payload = {}
    headers = {
    'Authorization': f'Bearer {parametros.access_token}'
    }

    response = GET(url, headers, payload, parametros)
    return response

def metrica_anuncio(parametros,campaña,fecha_desde,fecha_hasta,MLA):
    
    
    url = f"https://api.mercadolibre.com/advertising/product_ads_2/campaigns/{str(campaña)}/ads/metrics?date_from={str(fecha_desde.strftime('%Y-%m-%d'))}&date_to={fecha_hasta.strftime('%Y-%m-%d')}&ids={str(MLA)}"

    payload = {}
    headers = {
    'Authorization': f'Bearer {parametros.access_token}'
    }

    response = GET(url, headers, payload, parametros)
    return response
    
    
    
    
    