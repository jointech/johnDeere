import requests
from pprint import pprint
import json
import os
from dotenv import load_dotenv
import pyodbc
from datetime import datetime

load_dotenv()

# Cargar las variables de entorno
SQL_HOST = os.getenv('SQL_HOST')
SQL_USER = os.getenv('SQL_USER')
SQL_PASSWORD = os.getenv('SQL_PASSWORD')
SQL_DATABASE = os.getenv('SQL_DATABASE')

startDate = datetime.now().isoformat(timespec='microseconds')+'Z'

# Configurar URL y encabezados de la API
url = os.getenv('API_URL')
clientId = os.getenv('API_CLIENTE_ID')
clientSecret = os.getenv('API_CLIENTE_SECRET')
accessToken = os.getenv('API_ACCESS_TOKEN')
scope = os.getenv('API_SCOPE')

headers = {
    "Accept": "application/vnd.deere.axiom.v3+json",
    "Authorization": "Bearer " + accessToken,
    "clientId": clientId,
    "clientSecret": clientSecret,
    "scope": scope,
    "x-deere-no-paging": "true",
}

params = {
    "embed": "measurementDefinition",
    "startDate": os.getenv('START_DATE'),
}

# Lista para almacenar los datos obtenidos
data_list = []

# Lista de IDs a verificar
ids_to_check = [
    "8B7660F4-0026-45A7-89CF-1D993DFD4232",
    "475CA6B1-999B-4031-A3AC-113E42C03508",
    "D9E98D84-CEC5-485B-948C-24BBCD5FB230",
    "2F718DA3-3AF9-42E5-93F8-BE2374D7D10F",
    "ED7F3C83-3C22-4770-BA59-D68C7EB31F05",
    "0D8C5E70-A18C-45AB-8558-D339F757BF9A",
    "E53E31F3-E89E-4CF4-BE11-6ED6AB5CF337",
    "4C701832-017A-41DA-8799-8916B97DE7DA"
   
]

# Hacer la solicitud a la API principal
response = requests.get(url, headers=headers, params=params)
if response.status_code == 200:
    result = response.json()
    for org in result.get("values", []):
        for link in org.get('links', []):
            if link['rel'] == 'measurements':
                connectionsUri = link['uri']
                
                # Hacer la solicitud a la URI de las medidas
                respuesta = requests.get(connectionsUri, headers=headers, params=params)
                if respuesta.status_code == 200:
                    resultado = respuesta.json()
                    for hr in resultado.get('values', []):
                        if hr['machineMeasurementDefinition']['id'].upper() in ids_to_check:
                            connection = pyodbc.connect(
                                f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_HOST};DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no'
                            )
                            with connection.cursor() as cursor:
                                guid = hr["machineMeasurementDefinition"]["id"]
                                vin = org['vin']
                                medida = hr["machineMeasurementDefinition"]["name"]
                                valor = str(hr['series']["intervals"][0]["buckets"]['buckets'][0]['value'])
                                fecha = hr['series']["intervals"][0]["buckets"]['buckets'][0]['actualStartDate']
                                
                                try:
                                    unidad = hr["machineMeasurementDefinition"]["unitOfMeasure"]
                                except KeyError:
                                    unidad = None  # Valor predeterminado si el campo no existe

                                data_dict = {"guid": guid, "vin": vin, "Medida": medida, "valor": valor, "fecha": fecha, "unidad": unidad}
                                data_list.append(data_dict)

                                cursor.execute(
                                    """INSERT INTO produccion (guid, vin, medida, valor, fecha, unidad)
                                    VALUES (?, ?, ?, ?, ?, ?)""",
                                    (guid, vin, medida, valor, fecha, unidad),
                                )
                            connection.commit()
                            connection.close()

    pprint(data_list)
else:
    print(f"Error al obtener datos de la API: {response.status_code}")
