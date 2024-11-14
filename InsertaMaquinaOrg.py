import requests
import pyodbc
from pprint import pprint
import json
import os
from dotenv import load_dotenv

load_dotenv()

SQL_HOST = os.getenv('SQL_HOST')
SQL_USER = os.getenv('SQL_USER')
SQL_PASSWORD = os.getenv('SQL_PASSWORD')
SQL_DATABASE = os.getenv('SQL_DATABASE')

url = "https://partnerapi.deere.com/platform/organizations/"

clientId = os.getenv('API_CLIENTE_ID')
clientSecret = os.getenv('API_CLIENTE_SECRET')
accessToken = os.getenv('API_ACCESS_TOKEN')
scope = os.getenv('API_SCOPE')

cabezera = {
    "Accept": "application/vnd.deere.axiom.v3+json",
    "Authorization": "Bearer " + accessToken,
    "clientId": clientId,
    "clientSecret": clientSecret,
    "scope": scope,
    "x-deere-no-paging": "true",
}
parametros = {
    "embed": "measurementDefinition",
    "startDate": os.getenv('START_DATE'),
}

lista_id = []

response = requests.get(url, headers=cabezera, params=parametros)
if response.status_code == 200:
    result = json.loads(response.text)
    for org in result["values"]:
        for link in org['links']:
            if link['rel'] == 'machines':  # Cambiado 'self' a 'machines' para obtener las máquinas de la organización
                machinesUri = link['uri']
                respuesta = requests.get(machinesUri, headers=cabezera, params=parametros)
                if respuesta.status_code == 200:
                    maquinas = json.loads(respuesta.text)
                    for maquina in maquinas['values']:
                        connection = pyodbc.connect(f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_HOST};DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no')

                        with connection.cursor() as cursor:
                            vin = maquina.get('vin', 'N/A')  # Usar 'N/A' si no existe 'vin'
                            nombre = maquina['name']
                            id = maquina['id']
                            org_id = org['id']
                            org_nombre = org['name']
                         

                            dic = {"Vin": vin, "Nombre": nombre, "id": id, "Organizacion": org_id, "orgNombre": org_nombre}
                            lista_id.append(dic)

                            cursor.execute(
                                "INSERT INTO machines (vin, nombre, id_maquina, id_org, org_nombre) VALUES (?, ?, ?, ?, ?)",
                                (vin, nombre, id, org_id, org_nombre)
                            )

                        connection.commit()
                        connection.close()

    pprint(lista_id)
else:
    print(f"Error: {response.status_code}")
