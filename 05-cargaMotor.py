#CARGAS ALTA, BAJA , MEDIA, RALENTI
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
                "startDate":os.getenv("START_DATE"),
                #"startDate": startDate,
                #"utcoffSet":datetime.utcnow(),
                #"startDate":"2023-11-15T08:00:00.000Z",
                
                #Engine Utilization
                
  
            }

lista_id = []
            
    #response = requests.get(url, headers=cabezera, params=parametros) 
response = requests.get(url, headers=cabezera, params=parametros) 
if response.status_code == 200:
    #print(response)
    result =json.loads(response.text)
    # #result =json.loads(response)   
    #pprint(result)
    
    for org in result["values"]:  
        for link in org['links']:
            if link['rel'] == 'measurements':
                connectionsUri = link['uri']
                #pprint (connectionsUri)
                respuesta = requests.get(connectionsUri, headers=cabezera, params=parametros)
                if respuesta.status_code == 200:
                    resultado = json.loads(respuesta.text)
                    for hr in resultado['values']:
                        #if (hr['machineMeasurementDefinition']['id'] == "2f718da3-3af9-42e5-93f8-be2374d7d10f"):
                        if (hr['machineMeasurementDefinition']['id'] == "2F718DA3-3AF9-42E5-93F8-BE2374D7D10F"):    
                            
                         #pprint (hr)   
                         connection = pyodbc.connect(f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_HOST};DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no')
                         with connection.cursor() as cursor:
        
                            id = hr["machineMeasurementDefinition"]["id"]                     
                            medida = hr["machineMeasurementDefinition"]["name"]
                            unidad = hr["machineMeasurementDefinition"]["unitOfMeasure"]
                            StartDate = hr['series']["intervals"][0]["buckets"]['buckets'][0]['actualStartDate']
                            vin = org['vin']#intervalStartDate
                           
                            CargaAlta1 = hr['series']["intervals"][0]["buckets"]['buckets'][2]['value'] /3600
                            CargaAlta = str(CargaAlta1)
                            
                            CargaMedia1 = hr['series']["intervals"][0]["buckets"]['buckets'][3]['value'] /3600
                            CargaMedia = str(CargaMedia1)
                            
                            CargaBaja1 = hr['series']["intervals"][0]["buckets"]['buckets'][0]['value'] /3600
                            CargaBaja = str(CargaBaja1) 
                            
                            Ralenti1 = hr['series']["intervals"][0]["buckets"]['buckets'][1]['value'] /3600
                            Ralenti =str(Ralenti1)
                            
                            KeyOn1 = hr['series']["intervals"][0]["buckets"]['buckets'][4]['value'] /3600
                            KeyOn = str(KeyOn1)

                            
                            
                            #dic = {"CargaAlta":CargaAlta,"CargaMedia":CargaMedia,"CargaBaja":CargaBaja,"Ralenti":Ralenti,"KeyOn":KeyOn,} 
                            dic = { "id": id,"medida": medida,"Unidad":unidad,"StartDate":StartDate,"CargaMedia":CargaMedia,"CargaBaja":CargaBaja,"KeyOn":KeyOn,"vin":vin,"CargaAlta":CargaAlta}                           
                                
                            lista_id.append(dic)
                            #cursor.execute("INSERT INTO cargas (id, medida, startdate, ralenti, carga_altam, cargabaja, keyon, Carga_media, vin, unidad) VALUES ('"+ id +"','"+ medida +"','"+ StartDate +"','"+ Ralenti +"','"+ CargaBaja +"' ,'"+ CargaAlta +"','"+ CargaMedia +"','"+ KeyOn +"','"+ vin +"','"+ unidad +"')")                                          
                            
                            cursor.execute(
                                  """INSERT INTO cargas (id, medida, startdate, ralenti, carga_altam, cargabaja, keyon,Carga_media,vin,unidad)
                                  VALUES (?,?,?,?,?,?,?,?,?,?)""",
                                  (id, medida, StartDate, Ralenti, CargaBaja, CargaAlta,CargaMedia,KeyOn,vin,unidad ),
                                )


                         connection.commit()
                         connection.close()
     
                pprint (lista_id)