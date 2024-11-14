import requests
import psycopg2
from psycopg2 import DatabaseError
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
                #"x-deere-no-paging":"true",
                }
parametros = {
                "embed":"measurementDefinition",
                "startDate" : os.getenv('START_DATE'),
                #"startDate" : startDate,
                #"utcoffSet":datetime.utcnow(),
                #"endDate":"2023-11-15T08:00:00.000Z",
                #fuel consumido, Engine Utilization
                #"measurementDefinitionId":"8b7660f4-0026-45a7-89cf-1d993dfd4232",
                
  
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
        #pprint (org['vin'])
        for link in org['links']:
            if link['rel'] == 'engineHours':
                connectionsUri = link['uri']
                #pprint (connectionsUri)
                respuesta = requests.get(connectionsUri, headers=cabezera, params=parametros)
                if respuesta.status_code == 200:
                    resultado = json.loads(respuesta.text)
                    for hr in resultado['values']:
                        
                     connection = pyodbc.connect(f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_HOST};DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no')
                     with connection.cursor() as cursor:
                        
                        GUID = org['GUID']
                        vin = org['vin']
                        medida = hr['@type']
                        horas = str(hr['reading']['valueAsDouble'])
                        unidad = hr['reading']['unit']
                        #fecha = hr['series']["intervals"][0]["buckets"]['buckets'][0]['actualStartDate']
                        reportTime = hr['reportTime']
                        
                        #dic = {"guid":guid,"vin":vin, "Medida": nombre_medida,"valor":valor, "fecha":fecha, "unidad":unidad}
                        dic = { "GUID": GUID, "Vin": vin, "medida": medida,"Horas":horas,"Unidad":unidad,"Fecha":reportTime}
                
                        lista_id.append(dic)
                        

                        cursor.execute(
                                        """INSERT INTO horas (guid, vin, medida, horas, unidad, fecha)
                                        VALUES (?,?,?,?,?,?)""",
                                        (GUID,vin,medida,horas,unidad, reportTime),
                                        )

                     connection.commit()
                     connection.close()

                    pprint (lista_id)