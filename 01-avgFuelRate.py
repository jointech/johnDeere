#LISTA TODAS LAS MEDIDAS POR ID DE MAQUINA 
import requests
import pyodbc
from datetime import datetime
from zoneinfo import ZoneInfo, available_timezones
from pprint import pprint
import json
import os
from dotenv import load_dotenv

load_dotenv()

SQL_HOST=os.getenv('SQL_HOST')
SQL_USER=os.getenv('SQL_USER')
SQL_PASSWORD=os.getenv('SQL_PASSWORD')
SQL_DATABASE=os.getenv('SQL_DATABASE')

#startDate = datetime.datetime.now()
startDate = datetime.now().isoformat(timespec='microseconds')+'Z'
#2023-11-22T13:08:15.000Z

# fecha= datetime.now()
# startDate = fecha.isoformat()
    
#API_URL = "https://partnerapi.deere.com/platform/organizations/4641821/machines"


url = os.getenv('API_URL')
clientId =os.getenv('API_CLIENTE_ID') 
clientSecret = os.getenv('API_CLIENTE_SECRET')
accessToken = os.getenv('API_ACCESS_TOKEN')
scope = os.getenv('API_SCOPE')
        
cabezera = {
                "Accept": "application/vnd.deere.axiom.v3+json",
                "Authorization": "Bearer " + accessToken,
                "clientId": clientId,
                "clientSecret": clientSecret,
                "scope": scope,
                "x-deere-no-paging":"true",
                }
parametros = {
            "embed":"measurementDefinition",
            #"startDate": startDate,
            "startDate":os.getenv('START_DATE'),
            #"startDate":'2023-11-16T08:00:00.000Z',
            #"utcoffSet":datetime.utcnow(),
            
                
           
            }

lista_id = []
            
    #response = requests.get(url, headers=cabezera, params=parametros) 
response = requests.get(url, headers=cabezera, params=parametros) 
if response.status_code == 200:
    #print(response)
    result =json.loads(response.text)
    # #result =json.loads(response)   
    #print(result)
    for org in result["values"]:  
        for link in org['links']:
            if link['rel'] == 'measurements':
                connectionsUri = link['uri']
                #pprint (connectionsUri)
                respuesta = requests.get(connectionsUri, headers=cabezera, params=parametros)
                if respuesta.status_code == 200:
                    resultado = json.loads(respuesta.text)
                    for hr in resultado['values']:
                        #if (hr['machineMeasurementDefinition']['id'] == "8b7660f4-0026-45a7-89cf-1d993dfd4232"):
                        if (hr['machineMeasurementDefinition']['id'] == "8B7660F4-0026-45A7-89CF-1D993DFD4232"):    
                            
                          #Avg Fuel Rate  
                         connection = pyodbc.connect(f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_HOST};DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no')
                         with connection.cursor() as cursor:
            

                             guid = hr["machineMeasurementDefinition"]["id"] 
                             vin = org['vin']                       
                             medida = hr["machineMeasurementDefinition"]["name"]
                             valor = hr['series']["intervals"][0]["buckets"]['buckets'][0]['value']
                             fecha = hr['series']["intervals"][0]["buckets"]['buckets'][0]['actualStartDate']
                             unidad = hr["machineMeasurementDefinition"]["unitOfMeasure"]

                             dic = {"guid":guid,"vin":vin, "Medida": medida,"valor":valor, "fecha":fecha,"unidad":unidad }
            
                             lista_id.append(dic)
                             
                             #cursor.execute("INSERT INTO metricas (guid, vin, medida, valor, fecha, unidad) VALUES ('"+ guid +"','"+ vin +"','"+ medida +"',{{valor}} ,'" + fecha + "' , '"+ unidad +"')")           
                             
                             cursor.execute(
                                  """INSERT INTO combustible (guid, vin, medida, valor, fecha, unidad)
                                  VALUES (?,?,?,?,?,?)""",
                                  (guid, vin, medida, valor,fecha,unidad),
                                )

                         connection.commit()
                         connection.close()
                    pprint (lista_id) 
        




        