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
                #"x-deere-no-paging":"true",
                }
parametros = {
                "embed":"measurementDefinition",
                #"startDate" :'startDate'
                #"startDate" : os.getenv('START_DATE')
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
            if link['rel'] == 'measurements':
                connectionsUri = link['uri']
                #pprint (connectionsUri)
                respuesta = requests.get(connectionsUri, headers=cabezera, params=parametros)
                if respuesta.status_code == 200:
                    resultado = json.loads(respuesta.text)
                    #pprint(resultado)
                    for org in resultado['values']:
                        
                        connection = pyodbc.connect(f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_HOST};DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no')
                        with connection.cursor() as cursor:
                            
                            org = org['name']
                     
                            dic = { "Org": org,}
                
                            lista_id.append(dic)
                            
                            #cursor.execute("INSERT INTO alertas (vin, description, hora, severidad, color) VALUES ('"+ vin +"','"+ description +"','"+ hora +"','"+ severity +"','"+ color +"')")
                            # cursor.execute(
                            #                  """INSERT INTO alertas (vin, description, hora, severidad, color)
                            #                  VALUES (?,?,?,?,?)""",
                            #                  (vin,description, hora, severity, color),
                            #          )
                            
                    connection.commit()
                    connection.close()

                pprint (lista_id)
              
            