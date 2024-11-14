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

def connect_to_sql():
    try:
        connection = pyodbc.connect(
            f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SQL_HOST};DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD};Encrypt=yes;TrustServerCertificate=no'
        )
        return connection
    except Exception as e:
        print(f"Error al conectar con la base de datos: {e}")
        return None

def insert_data(cursor, data):
    try:
        cursor.execute(
            """INSERT INTO produccion (guid, vin, medida, valor, unidad, StartDate, CargaAlta, CargaMedia, CargaBaja, Ralenti, KeyOn, latitud, longitud)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                data["guid"], data["vin"], data["Medida"], data.get("valor"), data.get("unidad"),
                data.get("StartDate"), data.get("CargaAlta"), data.get("CargaMedia"), data.get("CargaBaja"),
                data.get("Ralenti"), data.get("KeyOn"), data.get("latitud"), data.get("longitud")
            )
        )
    except Exception as e:
        print(f"Error al insertar datos en la base de datos: {e}")

def fetch_data_from_api():
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error al obtener datos de la API: {response.status_code}")
        return None

result = fetch_data_from_api()
if result:
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
                            data_dict = {}
                            data_dict["guid"] = hr["machineMeasurementDefinition"]["id"]
                            data_dict["vin"] = org['vin']
                            data_dict["Medida"] = hr["machineMeasurementDefinition"]["name"]
                            data_dict["StartDate"] = hr['series']["intervals"][0]["buckets"]['buckets'][0]['actualStartDate']
                            
                            try:
                                data_dict["unidad"] = hr["machineMeasurementDefinition"]["unitOfMeasure"]
                            except KeyError:
                                data_dict["unidad"] = None  # Valor predeterminado si el campo no existe
                            
                            if hr['machineMeasurementDefinition']['id'] == "2F718DA3-3AF9-42E5-93F8-BE2374D7D10F":
                                data_dict["CargaAlta"] = str(hr['series']["intervals"][0]["buckets"]['buckets'][2]['value'] / 3600)
                                data_dict["CargaMedia"] = str(hr['series']["intervals"][0]["buckets"]['buckets'][3]['value'] / 3600)
                                data_dict["CargaBaja"] = str(hr['series']["intervals"][0]["buckets"]['buckets'][0]['value'] / 3600)
                                data_dict["Ralenti"] = str(hr['series']["intervals"][0]["buckets"]['buckets'][1]['value'] / 3600)
                                data_dict["KeyOn"] = str(hr['series']["intervals"][0]["buckets"]['buckets'][4]['value'] / 3600)
                                data_dict["valor"] = None  # Set valor to None if this ID matches
                            else:
                                data_dict["valor"] = str(hr['series']["intervals"][0]["buckets"]['buckets'][0]['value'])

                            # Set default values for optional fields if not present
                            data_dict.setdefault("unidad", None)
                            data_dict.setdefault("valor", None)
                            data_dict.setdefault("CargaAlta", None)
                            data_dict.setdefault("CargaMedia", None)
                            data_dict.setdefault("CargaBaja", None)
                            data_dict.setdefault("Ralenti", None)
                            data_dict.setdefault("KeyOn", None)
                            data_dict.setdefault("latitud", None)
                            data_dict.setdefault("longitud", None)

                            data_list.append(data_dict)

                            connection = connect_to_sql()
                            if connection:
                                with connection.cursor() as cursor:
                                    insert_data(cursor, data_dict)
                                connection.commit()
                                connection.close()
                else:
                    print(f"Error al obtener datos de la API de medidas: {respuesta.status_code}")
            
            elif link['rel'] == 'locationHistory':
                locationUri = link['uri']
                
                # Hacer la solicitud a la URI de la ubicación
                respuesta = requests.get(locationUri, headers=headers, params=params)
                if respuesta.status_code == 200:
                    resultado = respuesta.json()
                    for location in resultado.get('values', []):
                        data_dict = {}
                        data_dict["guid"] = org.get("guid", "")
                        data_dict["vin"] = org['vin']
                        data_dict["Medida"] = "locationHistory"
                        #data_dict["StartDate"] = location['reportDate']

                        try:
                            data_dict["latitud"]  = location['point']['lat']
                            data_dict["longitud"] = location['point']['lon']
                        except KeyError:
                            data_dict["latitud"] = None
                            data_dict["longitud"] = None
                        
                        # Set default values for optional fields if not present
                        data_dict.setdefault("unidad", None)
                        data_dict.setdefault("valor", None)
                        data_dict.setdefault("CargaAlta", None)
                        data_dict.setdefault("CargaMedia", None)
                        data_dict.setdefault("CargaBaja", None)
                        data_dict.setdefault("Ralenti", None)
                        data_dict.setdefault("KeyOn", None)
                        data_dict.setdefault("latitud", None)
                        data_dict.setdefault("longitud", None)

                        data_list.append(data_dict)

                        connection = connect_to_sql()
                        if connection:
                            with connection.cursor() as cursor:
                                insert_data(cursor, data_dict)
                            connection.commit()
                            connection.close()
                else:
                    print(f"Error al obtener datos de la API de ubicación: {respuesta.status_code}")

    pprint(data_list)
else:
    print("No se obtuvieron resultados de la API.")
