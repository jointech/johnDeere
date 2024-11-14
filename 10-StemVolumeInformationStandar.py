#LISTA TODAS LAS MEDIDAS POR ID DE MAQUINA 
import requests
from pprint import pprint
import json
import os
from dotenv import load_dotenv
import pyodbc
from datetime import datetime, timedelta 
import pytz


load_dotenv()

SQL_HOST=os.getenv('SQL_HOST')
SQL_USER=os.getenv('SQL_USER')
SQL_PASSWORD=os.getenv('SQL_PASSWORD')
SQL_DATABASE=os.getenv('SQL_DATABASE')

startDate = datetime.now().isoformat(timespec='microseconds')+'Z'    

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
            "startDate":os.getenv('START_DATE'),
            #"startDate": startDate,
            #"utcoffSet":datetime.utcnow(),
            #"startDate":'2023-11-15T08:00:00.000Z',
            
                
           
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
                        # ACÁ SACO LA METRICA FUEL CONSUMED
                        #if (hr['machineMeasurementDefinition']['id'] == "e53e31f3-e89e-4cf4-be11-6ed6ab5cf337"): 
                        if (hr['machineMeasurementDefinition']['id'] == "E53E31F3-E89E-4CF4-BE11-6ED6AB5CF337"):    
                            
                            #Stem Volume Information [m3]  
                           
                            
                         connection = pyodbc.connect(f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_HOST};DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no')
                         with connection.cursor() as cursor:
            

                             guid = hr["machineMeasurementDefinition"]["id"] 
                             vin = org['vin']                       
                             medida = hr["machineMeasurementDefinition"]["name"]
                             valor = str(hr['series']["intervals"][0]["buckets"]['buckets'][1]['value'])
                             fecha = hr['series']["intervals"][0]["buckets"]['buckets'][1]['actualStartDate']
                             #unidad = hr["machineMeasurementDefinition"]["unitOfMeasure"]

                             dic = {"guid":guid,"vin":vin, "Medida": medida,"valor":valor, "fecha":fecha}
            
                             lista_id.append(dic)
                             
                             

                             cursor.execute(
                                  """INSERT INTO produccion (guid, vin, medida, valor, fecha)
                                  VALUES (?,?,?,?,?)""",
                                  (guid, vin, medida, valor,fecha),
                                )
                             
                         connection.commit()
                         connection.close()

                pprint (lista_id)