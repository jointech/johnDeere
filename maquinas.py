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
                        if org['name'] != 'D&D Ing-Construcción-Corrosión y Protección Ltda'\
                            and org['name'] != 'Corteva Temuco'\
                            and org['name'] != 'Corteva Agrisciences Chile Ltda'\
                            and org['name'] != 'Cornejo junior '\
                            and org['name'] != 'ControlRoll'\
                            and org['name'] != 'Constructora Tecton SpA'\
                            and org['name'] != 'Constructora Nuevos Aires S.A.'\
                            and org['name'] != 'Constructora Excon S.A.'\
                            and  org['name'] !='Constructora Mario Jaramillo Gallardo'\
                            and  org['name'] !='Constructora Loon'\
                            and  org['name'] !='Constructora Ingebel'\
                            and  org['name'] !='Constructora HF'\
                            and  org['name'] !='Constructora Ingebel '\
                            and  org['name'] !='Construcción y Proyectos Flos Limitada'\
                            and  org['name'] !='Conpax Maquinarias'\
                            and  org['name'] !='Coalco'\
                            and  org['name'] !='Cifuentes Ltda.'\
                            and  org['name'] !=' Boquial'\
                            and  org['name'] !='Christian Apablaza V'\
                            and  org['name'] !='Chile_Arauco CIMA'\
                            and  org['name'] !='Cervecería Rural'\
                            and  org['name'] !='Constructora Inversha Ltda.':
                                
                            if hr['equipmentType']['marketSegment'] != "Agriculture":
                                    
                                    connection = pyodbc.connect(f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_HOST};DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no')
                                    with connection.cursor() as cursor:
                                
                                #vin = hr['vin']
                                        guid = hr['GUID']
                                        #Tipo = hr['equipmentType']['name']
                                        #Marca = hr['category']['name']
                                        #modelo = hr['machineCategories']['machineCategories'][1]['name']
                                        name = org['name']
                                        id = hr['id']
                            
                                    dic = { "GUID": guid ,"nombre": name, "id": id}
                        
                                    lista_id.append(dic)
                                    
                                    #cursor.execute("INSERT INTO modelos (guid, tipo, marca, modelo, cliente, id_maquina) VALUES ('"+ guid +"','"+ Tipo +"','"+ Marca +"','"+ modelo +"','"+ id +"')")
                                    cursor.execute(
                                                    """INSERT INTO modelos (guid, cliente, id_maquina)
                                                    VALUES (?,?,?)""",
                                                    (guid, name,id),
                                                    )
                                    
                                    connection.commit()
                                    connection.close()
                                    
                        else:
                            
                                    if hr['equipmentType']['marketSegment'] == "Forestry":
                                        connection = pyodbc.connect(f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_HOST};DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no')
                                        with connection.cursor() as cursor:
                                    
                                    #vin = hr['vin']
                                            guid = hr['GUID']
                                            Tipo = hr['equipmentType']['name']
                                            Marca = hr['equipmentMake']['name']
                                            modelo = hr['equipmentModel']['name']
                                            name = org['name']
                                            id = hr['id']
                                
                                        dic = { "GUID": guid , "Tipo": Tipo, "equipmentMake": Marca,"equipmentModel": modelo,"nombre": name, "id": id}
                            
                                        lista_id.append(dic)
                                        
                                        #cursor.execute("INSERT INTO modelos (guid, tipo, marca, modelo, cliente, id_maquina) VALUES ('"+ guid +"','"+ Tipo +"','"+ Marca +"','"+ modelo +"','"+ id +"')")
                                        cursor.execute(
                                                        """INSERT INTO modelos (guid, tipo, marca, modelo, cliente, id_maquina)
                                                        VALUES (?,?,?,?,?,?)""",
                                                        (guid, Tipo, Marca, modelo, name,id),
                                                        )
                                        
                                        connection.commit()
                                        connection.close()    
                                    
                                    else:
                            
                                     if hr['equipmentType']['marketSegment'] != "Engines & Components":
                                        connection = pyodbc.connect(f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_HOST};DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no')
                                        with connection.cursor() as cursor:
                                    
                                    #vin = hr['vin']
                                            guid = hr['GUID']
                                            Tipo = hr['equipmentType']['name']
                                            Marca = hr['equipmentMake']['name']
                                            modelo = hr['equipmentModel']['name']
                                            name = org['name']
                                            id = hr['id']
                                
                                        dic = { "GUID": guid , "Tipo": Tipo, "equipmentMake": Marca,"equipmentModel": modelo,"nombre": name, "id": id}
                            
                                        lista_id.append(dic)
                                        
                                        #cursor.execute("INSERT INTO modelos (guid, tipo, marca, modelo, cliente, id_maquina) VALUES ('"+ guid +"','"+ Tipo +"','"+ Marca +"','"+ modelo +"','"+ id +"')")
                                        cursor.execute(
                                                        """INSERT INTO modelos (guid, tipo, marca, modelo, cliente, id_maquina)
                                                        VALUES (?,?,?,?,?,?)""",
                                                        (guid, Tipo, Marca, modelo, name,id),
                                                        )
                                        
                                        connection.commit()
                                        connection.close()   
                                    
                                     else:
                    
                                        if hr['equipmentType']['marketSegment'] != "Construction":
                                                        connection = pyodbc.connect(f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_HOST};DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no')
                                                        with connection.cursor() as cursor:
                                                    
                                                    #vin = hr['vin']
                                                            guid = hr['GUID']
                                                            Tipo = hr['equipmentType']['name']
                                                            Marca = hr['equipmentMake']['name']
                                                            modelo = hr['equipmentModel']['name']
                                                            name = org['name']
                                                            id = hr['id']
                                                
                                                        dic = { "GUID": guid , "Tipo": Tipo, "equipmentMake": Marca,"equipmentModel": modelo,"nombre": name, "id": id}
                                            
                                                        lista_id.append(dic)
                                                        
                                                        #cursor.execute("INSERT INTO modelos (guid, tipo, marca, modelo, cliente, id_maquina) VALUES ('"+ guid +"','"+ Tipo +"','"+ Marca +"','"+ modelo +"','"+ id +"')")
                                                        cursor.execute(
                                                                        """INSERT INTO modelos (guid, tipo, marca, modelo, cliente, id_maquina)
                                                                        VALUES (?,?,?,?,?,?)""",
                                                                        (guid, Tipo, Marca, modelo, name,id),
                                                                        )
                                                        
                                                        connection.commit()
                                                        connection.close()              
                                        else:
                                            connection = pyodbc.connect(f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_HOST};DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no')
                                            with connection.cursor() as cursor:
                                                                    
                                                                #vin = hr['vin']
                                                                        guid = hr['GUID']
                                                                        Tipo = hr['equipmentType']['name']
                                                                        Marca = hr['equipmentMake']['name']
                                                                        modelo = hr['equipmentModel']['name']
                                                                        name = org['name']
                                                                        id = hr['id']
                                                            
                                            dic = { "GUID": guid , "Tipo": Tipo, "equipmentMake": Marca,"equipmentModel": modelo,"nombre": name, "id": id}
                                
                                            lista_id.append(dic)
                                            
                                            #cursor.execute("INSERT INTO modelos (guid, tipo, marca, modelo, cliente, id_maquina) VALUES ('"+ guid +"','"+ Tipo +"','"+ Marca +"','"+ modelo +"','"+ id +"')")
                                            cursor.execute(
                                                            """INSERT INTO modelos (guid, tipo, marca, modelo, cliente, id_maquina)
                                                            VALUES (?,?,?,?,?,?)""",
                                                            (guid, Tipo, Marca, modelo, name,id),
                                                            )
                                            
                                            connection.commit()
                                            connection.close()              

                                
                                            pprint (lista_id)
              
            