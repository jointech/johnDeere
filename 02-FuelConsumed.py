#LISTA TODAS LAS MEDIDAS POR ID DE MAQUINA 
import requests
import pyodbc
from pprint import pprint
import json
import os
from dotenv import load_dotenv

from datetime import datetime, timedelta 
import pytz

load_dotenv()

SQL_HOST=os.getenv('SQL_HOST')
SQL_USER=os.getenv('SQL_USER')
SQL_PASSWORD=os.getenv('SQL_PASSWORD')
SQL_DATABASE=os.getenv('SQL_DATABASE')

url = os.getenv('API_URL')
clientId =os.getenv('API_CLIENTE_ID') 
clientSecret = os.getenv('API_CLIENTE_SECRET')
accessToken = os.getenv('API_ACCESS_TOKEN')
scope = os.getenv('API_SCOPE')

startDate = datetime.now().isoformat(timespec='microseconds')+'Z'
        
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
                #"startDate":startDate,
                "startDate":os.getenv('START_DATE'),
                #"utcoffSet":datetime.utcnow(),
                #"endDate":os.getenv('END_DATE'),
 
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
                        #if (hr['machineMeasurementDefinition']['id'] == "475ca6b1-999b-4031-a3ac-113e42c03508"):
                        if (hr['machineMeasurementDefinition']['id'] == "475CA6B1-999B-4031-A3AC-113E42C03508"):    
                            
                        
                        #if (hr['machineMeasurementDefinition']['id'] == "0cfd2dc7-3e63-4b30-a8b3-d608e8d68f4f"):
                            
                         connection = pyodbc.connect(f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_HOST};DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no')
                         with connection.cursor() as cursor:
            

                             guid = hr["machineMeasurementDefinition"]["id"] 
                             vin = org['vin']                       
                             medida = hr["machineMeasurementDefinition"]["name"]
                             result= hr['series']["intervals"][0]["buckets"]['buckets'][0]['value']/3600
                             valor = str(result)
                             fecha = hr['series']["intervals"][0]["buckets"]['buckets'][0]['actualStartDate']
                             unidad = hr["machineMeasurementDefinition"]["unitOfMeasure"]

                             dic = {"guid":guid,"vin":vin, "Medida": medida,"valor":valor, "fecha":fecha, "Unidad" : unidad}
            
                             lista_id.append(dic)
                             
                             #cursor.execute("INSERT INTO metricas (guid, vin, medida, valor, fecha, unidad) VALUES ('"+ guid +"','"+ vin +"','"+ medida +"','"+ valor +"','"+ fecha +"' ,'"+ unidad +"')")           

                             cursor.execute(
                                  """INSERT INTO combustible (guid, vin, medida, valor, fecha, unidad)
                                  VALUES (?,?,?,?,?,?)""",
                                  (guid, vin, medida, valor,fecha,unidad),
                                )

                         connection.commit()
                         connection.close()

                         pprint (lista_id)