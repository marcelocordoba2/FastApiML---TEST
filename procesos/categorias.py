##### TRAE EL ARBOL DE CATEGORIAS DE MERCADO LIBRE.
##### ACTUALIZAR UNA VEZ POR MES.

from mercadolibre import funciones_ml
import json
from pandas import json_normalize
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import traceback, logging
from database import database

def actualizacion_categorias(parametros):
    conn_str=database.conexion()
    arbol = funciones_ml.arbol_categorias(parametros)
    categorias = json.loads(arbol.text)
    cate=[]

    for clave in categorias:
        
        if len(categorias[clave]['path_from_root'])==1:
            cate.append({
                'id':categorias[clave]['id'],
                'nombre':categorias[clave]['name'],
                'nivel': 1,
                'nivel_1':categorias[clave]['path_from_root'][0]['id']
                })
        
        if len(categorias[clave]['path_from_root'])==2:
            cate.append({
                'id':categorias[clave]['id'],
                'nombre':categorias[clave]['name'],
                'nivel': 2,
                'nivel_1':categorias[clave]['path_from_root'][0]['id'],
                'nivel_2':categorias[clave]['path_from_root'][1]['id']
                })
        
        if len(categorias[clave]['path_from_root'])==3:
            cate.append({
                'id':categorias[clave]['id'],
                'nombre':categorias[clave]['name'],
                'nivel': 3,
                'nivel_1':categorias[clave]['path_from_root'][0]['id'],
                'nivel_2':categorias[clave]['path_from_root'][1]['id'],
                'nivel_3':categorias[clave]['path_from_root'][2]['id']
                })
            
        if len(categorias[clave]['path_from_root'])==4:
            cate.append({
                'id':categorias[clave]['id'],
                'nombre':categorias[clave]['name'],
                'nivel': 4,
                'nivel_1':categorias[clave]['path_from_root'][0]['id'],
                'nivel_2':categorias[clave]['path_from_root'][1]['id'],
                'nivel_3':categorias[clave]['path_from_root'][2]['id'],
                'nivel_4':categorias[clave]['path_from_root'][3]['id']
                })
            
        if len(categorias[clave]['path_from_root'])==5:
            cate.append({
                'id':categorias[clave]['id'],
                'nombre':categorias[clave]['name'],
                'nivel': 5,
                'nivel_1':categorias[clave]['path_from_root'][0]['id'],
                'nivel_2':categorias[clave]['path_from_root'][1]['id'],
                'nivel_3':categorias[clave]['path_from_root'][2]['id'],
                'nivel_4':categorias[clave]['path_from_root'][3]['id'],
                'nivel_5':categorias[clave]['path_from_root'][4]['id']
                })
            
        if len(categorias[clave]['path_from_root'])==6:
            cate.append({
                'id':categorias[clave]['id'],
                'nombre':categorias[clave]['name'],
                'nivel': 6,
                'nivel_1':categorias[clave]['path_from_root'][0]['id'],
                'nivel_2':categorias[clave]['path_from_root'][1]['id'],
                'nivel_3':categorias[clave]['path_from_root'][2]['id'],
                'nivel_4':categorias[clave]['path_from_root'][3]['id'],
                'nivel_5':categorias[clave]['path_from_root'][4]['id'],
                'nivel_6':categorias[clave]['path_from_root'][5]['id']
                })
        
        if len(categorias[clave]['path_from_root'])==7:
            cate.append({
                'id':categorias[clave]['id'],
                'nombre':categorias[clave]['name'],
                'nivel': 7,
                'nivel_1':categorias[clave]['path_from_root'][0]['id'],
                'nivel_2':categorias[clave]['path_from_root'][1]['id'],
                'nivel_3':categorias[clave]['path_from_root'][2]['id'],
                'nivel_4':categorias[clave]['path_from_root'][3]['id'],
                'nivel_5':categorias[clave]['path_from_root'][4]['id'],
                'nivel_6':categorias[clave]['path_from_root'][5]['id'],
                'nivel_7':categorias[clave]['path_from_root'][6]['id']
                })
            
    categorias = json_normalize(cate)
    cat_nivel_1 = categorias[categorias['nivel']==1][['id','nombre']].drop_duplicates('id')
    cat_nivel_2 = categorias[categorias['nivel']==2][['id','nombre']].drop_duplicates('id')
    cat_nivel_3 = categorias[categorias['nivel']==3][['id','nombre']].drop_duplicates('id')
    cat_nivel_4 = categorias[categorias['nivel']==4][['id','nombre']].drop_duplicates('id')
    cat_nivel_5 = categorias[categorias['nivel']==5][['id','nombre']].drop_duplicates('id')
    cat_nivel_6 = categorias[categorias['nivel']==6][['id','nombre']].drop_duplicates('id')
    cat_nivel_7 = categorias[categorias['nivel']==7][['id','nombre']].drop_duplicates('id')


    engine = create_engine(conn_str)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        with session.begin(): 
            cat_nivel_1.to_sql('cat_nivel_1', engine, index=False, if_exists='replace')
            cat_nivel_2.to_sql('cat_nivel_2', engine, index=False, if_exists='replace')
            cat_nivel_3.to_sql('cat_nivel_3', engine, index=False, if_exists='replace')
            cat_nivel_4.to_sql('cat_nivel_4', engine, index=False, if_exists='replace')
            cat_nivel_5.to_sql('cat_nivel_5', engine, index=False, if_exists='replace')
            cat_nivel_6.to_sql('cat_nivel_6', engine, index=False, if_exists='replace')
            cat_nivel_7.to_sql('cat_nivel_7', engine, index=False, if_exists='replace')
            categorias.to_sql('categorias',engine,index=False,if_exists='replace')
        
        return 200    
    except Exception as e:
        session.rollback()
        traza_pila = traceback.format_exc()
        logging.error(f"Error en cargar_detalle_orden: {e}\n{traza_pila}")    
        raise
    finally:
        session.close()