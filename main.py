from fastapi import FastAPI, HTTPException, Request, Query
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from fastapi.responses import JSONResponse
from datetime import datetime
import logging,traceback
from sqlalchemy import create_engine,text
import os
from models.clases import cliente,notification

app = FastAPI()

# Configuracion de logging_________________________________________________________________________________
carpeta_logs = 'logs'
if not os.path.exists(carpeta_logs):
    os.makedirs(carpeta_logs)

ruta_archivo_log = os.path.join(carpeta_logs, f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
logging.basicConfig(filename=ruta_archivo_log, level=logging.ERROR)

# Configuracion de tareas periodicas______________________________________________________________________
def tarea_diaria():    
    print(f"tarea ejecutada")
    
@app.on_event("startup")
def iniciar_planificador():
    scheduler = AsyncIOScheduler()
    scheduler.add_job(tarea_diaria,IntervalTrigger(seconds=5))
    scheduler.start()
    print("planificador iniciado")
    
# end points___________________________________________________________________________________________

@app.get('/')
def read_root():
    return "Proceso Completo."

@app.post('/MLA_callbacks')
async def webhook(request: Request):
    try:
        payload= await request.json()
        noti = notification()
        load = noti.registrar_notificacion(payload)
        
        if load == 200:
            return JSONResponse(content={'message': 'OK'}, status_code=200)
    except Exception as e:
        logging.error(f'Error al procesar la notificación: {e}')
        raise HTTPException(status_code=500, detail='Internal Server Error')

@app.get('/MLA_redirect')
async def redireccionamiento(code: str = Query(...)):
  
    try:
        cli = cliente()  
        cli.nuevo("MLA",code)  
        return f"Bienvenido {cli.nickname}! En breve tendras disponible tu historial en Weiman."
    except Exception as e:
        traza_pila = traceback.format_exc()
        logging.error(f'Error al procesar la notificación: {e}\n{traza_pila }')
        raise HTTPException(status_code=500, detail='Internal Server Error')

    
@app.get('/MLU_redirect')
async def redireccionamiento(code: str = Query(...)):
    try:
        cli = cliente()  
        cli.nuevo("MLU",code)  
        return f"Bienvenido {cliente.nickname}! En breve tendras disponible tu historial en Weiman."
    except Exception as e:
        traza_pila = traceback.format_exc()
        logging.error(f'Error al procesar la notificación: {e}\n{traza_pila }')
        raise HTTPException(status_code=500, detail='Internal Server Error')

@app.get('/MLM_redirect')
async def redireccionamiento(code: str = Query(...)):
    try:
        cli = cliente()  
        cli.nuevo("MLM",code)  
        return f"Bienvenido {cliente.nickname}! En breve tendras disponible tu historial en Weiman."
    except Exception as e:
        traza_pila = traceback.format_exc()
        logging.error(f'Error al procesar la notificación: {e}\n{traza_pila }')
        raise HTTPException(status_code=500, detail='Internal Server Error')
