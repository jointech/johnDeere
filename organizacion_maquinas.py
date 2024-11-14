#ACA INSERTO LA ORGANIZACION Y LAS MAQUINAS DE LA ORGANIZACION QUE DESPUES USARE PARA FILTRAR

import requests
import pyodbc
from pprint import pprint
import json
import os
from dotenv import load_dotenv

load_dotenv()

SQL_HOST=os.getenv('SQL_HOST')
SQL_USER=os.getenv('SQL_USER')
SQL_PASSWORD=os.getenv('SQL_PASSWORD')
SQL_DATABASE=os.getenv('SQL_DATABASE')
    
#url = "https://partnerapi.deere.com/platform/organizations/"

url = os.getenv('API_URL')
#url = os.getenv('API_URL_MACHINE')
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
                #"startDate":"2023-10-20T08:00:00.000Z",
                "startDate" : os.getenv('START_DATE'),
                #"endDate":"2023-10-19T21:00:00.000Z",
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
            if link['rel'] == 'self':
                connectionsUri = link['uri']
                pprint (connectionsUri)
                respuesta = requests.get(connectionsUri, headers=cabezera, params=parametros)
                if respuesta.status_code == 200:
                    resultado = json.loads(respuesta.text)
                    #for org in resultado['values']:
                        # pprint (org['GUID'])
                        # pprint (org['vin'])
                        # pprint (hr['reading']['valueAsDouble'])
                        # pprint (hr['reading']['unit'])
                    connection = pyodbc.connect(f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_HOST};DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no')
                     
                    with connection.cursor() as cursor:
                        GUID = org['GUID']
                        vin = org['vin']
                        nombre = org['name']
                        id = org['id']
                        #org_id = '4641821'
                        org_id = '463153'
                        org_nombre = 'Cima Arauco'
                        contacto = 'hugoguino@arauco.cl'
                        

                        
                        dic = { "GUID": GUID, "Vin": vin, "Nombre":nombre,"id":id, "Organizacion": org_id, "orgNombre":org_nombre, "contacto":contacto}

                        lista_id.append(dic)
                         
                        cursor.execute("INSERT INTO machines (guid,vin, nombre, id_maquina, id_org, org_nombre, org_contacto) VALUES ('"+ GUID +"','"+ vin +"','"+ nombre +"','"+ id +"','"+ org_id +"','"+ org_nombre +"', '"+ contacto +"')")                                

                    connection.commit()
                    connection.close()

            pprint (lista_id)