import requests
import pyodbc
from pprint import pprint
import json
import os
from dotenv import load_dotenv
from datetime import datetime
from zoneinfo import ZoneInfo, available_timezones

load_dotenv()

url = os.getenv('API_URL_MACHINE')
clientId =os.getenv('API_CLIENTE_ID') 
clientSecret = os.getenv('API_CLIENTE_SECRET')
accessToken = os.getenv('API_ACCESS_TOKEN')
scope = os.getenv('API_SCOPE')

SQL_HOST=os.getenv('SQL_HOST')
SQL_USER=os.getenv('SQL_USER')
SQL_PASSWORD=os.getenv('SQL_PASSWORD')
SQL_DATABASE=os.getenv('SQL_DATABASE')

#startDate = 2023-11-22T13:08:15.000Z


# fecha= datetime.now()
# startDate = fecha.isoformat()


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
                #"startDate" :'startDate'
                "startDate" : os.getenv('START_DATE')
                #"startDate":"2023-10-19T08:00:00.000Z",

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
            if link['rel'] == 'machines':
                
                connectionsUri = link['uri']
                #pprint (connectionsUri)
                respuesta = requests.get(connectionsUri, headers=cabezera, params=parametros)
                if respuesta.status_code == 200:
                    resultado = json.loads(respuesta.text)
                    #pprint(resultado)
                    for hr in resultado['values']:
                         
                         # if hr['equipmentType']['marketSegment'] == "Forestry":
                         #if hr['equipmentType']['marketSegment'] != 'Construction' and hr['equipmentType']['marketSegment'] != 'Agriculture' and hr['equipmentType']['marketSegment'] != 'Unknown' and hr['equipmentType']['marketSegment'] != 'Engines & Components': 
                        if org['id'] == '463153':
                                connection = pyodbc.connect(f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_HOST};DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no')
                                with connection.cursor() as cursor:
                                
                                    #Segment = hr['equipmentType']['marketSegment']
                                    guid = hr['GUID']
                                    Tipo = hr['equipmentType']['name']
                                    Marca = hr['equipmentMake']['name']
                                    modelo = hr['equipmentModel']['name']
                                    name = org['name']
                                    id = hr['id']
                                    vin = hr['vin']

                            
                                    #dic = { "Segment": Segment, "Cliente": name}
                                    dic = { "GUID": guid ,"Tipo": Tipo, "Marca": Marca,"Modelo": modelo,"nombre": name, "id": id, "vin":vin}
                                    
                                    lista_id.append(dic)
                                    
                                    cursor.execute(
                                                   """INSERT INTO modelos (guid, Tipo, Marca, modelo, cliente, id_maquina, vin)
                                                   VALUES (?,?,?,?,?,?,?)""",
                                                   (guid,Tipo, Marca, modelo, name,id,vin),
                                                   )
       
                                connection.commit()
                                connection.close()
                                
                        else:
                            
                            if org['id'] == '1118081':
                                connection = pyodbc.connect(f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_HOST};DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no')
                                with connection.cursor() as cursor:
                                
                                    #Segment = hr['equipmentType']['marketSegment']
                                    guid = hr['GUID']
                                    Tipo = hr['equipmentType']['name']
                                    Marca = hr['equipmentMake']['name']
                                    modelo = hr['equipmentModel']['name']
                                    name = org['name']
                                    id = hr['id']
                                    vin = hr['vin']
                                    #latitud = hr['lat']
                                    #longitud = hr['lon']
                            
                                    #dic = { "Segment": Segment, "Cliente": name}
                                    dic = { "GUID": guid ,"Tipo": Tipo, "Marca": Marca,"Modelo": modelo,"nombre": name, "id": id, "vin":vin}
                                    
                                    lista_id.append(dic)
                                    
                                    cursor.execute(
                                                   """INSERT INTO modelos (guid, Tipo, Marca, modelo, cliente, id_maquina, vin)
                                                   VALUES (?,?,?,?,?,?,?)""",
                                                   (guid,Tipo, Marca, modelo, name,id,vin),
                                                   )
       
                                connection.commit()
                                connection.close()
                                
                                
                                pprint(lista_id)
                                
 

                                
              
            