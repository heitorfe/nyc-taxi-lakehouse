import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

# Diretório local para salvar arquivos (opcional)
LOCAL_DIR = "nyc_taxi_data"
os.makedirs(LOCAL_DIR, exist_ok=True)

# Parâmetros de dados
ANOS = range(2025, 2009, -1)
MESES = range(1, 13)
TIPOS = [
    ("yellow", "yellow_tripdata_{ano}-{mes:02d}.parquet"),
    ("green", "green_tripdata_{ano}-{mes:02d}.parquet"),
    ("fhv", "fhv_tripdata_{ano}-{mes:02d}.parquet"),
    ("fhvhv", "fhvhv_tripdata_{ano}-{mes:02d}.parquet"),
]

ADLS_CONN_STRING = os.getenv("ADLS_CONN_STRING")
print(ADLS_CONN_STRING)

def get_file_url(tipo_pattern, ano, mes):
    return f"https://d37ci6vzurychx.cloudfront.net/trip-data/{tipo_pattern.format(ano=ano, mes=mes)}"

def get_blob_path(tipo, ano, file_name):
    return f"source/{tipo}/{ano}/{file_name}"

def upload_file_to_blob(container_client, blob_path, url, skip_existing=True):
    blob_client = container_client.get_blob_client(blob_path)
    if skip_existing:
        try:
            blob_client.get_blob_properties()
            print(f"Pulado (já existe): {blob_path}")
            return
        except Exception:
            pass  # Blob não existe, pode enviar
    resp = requests.get(url, stream=True)
    if resp.status_code == 200:
        blob_client.upload_blob(data=resp.raw, overwrite=True)
        print(f"Enviado: {blob_path}")
    else:
        print(f"Arquivo não encontrado: {url}")

def process_mes(container_client, ano, mes, skip_existing=True):
    tasks = []
    for tipo, pattern in TIPOS:
        file_name = pattern.format(ano=ano, mes=mes)
        url = get_file_url(pattern, ano, mes)
        blob_path = get_blob_path(tipo, ano, file_name)
        tasks.append((container_client, blob_path, url, skip_existing))

    with ThreadPoolExecutor(max_workers=len(TIPOS)) as executor:
        futures = [executor.submit(upload_file_to_blob, *task) for task in tasks]
        for future in as_completed(futures):
            future.result()

def upload_nyc_taxi_data(
    connection_string,
    container_name="taxi",
    anos=ANOS,
    meses=MESES,
    skip_existing=True
):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    try:
        container_client.create_container()
    except Exception:
        pass  # Container já existe

    for ano in anos:
        with ThreadPoolExecutor(max_workers=4) as mes_executor:
            futures = [
                mes_executor.submit(process_mes, container_client, ano, mes, skip_existing)
                for mes in meses
            ]
            for future in as_completed(futures):
                future.result()

# Exemplo de uso:
upload_nyc_taxi_data(
    connection_string=ADLS_CONN_STRING,
    container_name="taxi"
)
