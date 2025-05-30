import os
import requests
from concurrent.futures import ThreadPoolExecutor
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

load_dotenv()


# Diretório para salvar os arquivos localmente
LOCAL_DIR = "nyc_taxi_data"
os.makedirs(LOCAL_DIR, exist_ok=True)

ANOS = range(2025, 2009, -1)  # 2024 até 2019
MESES = range(1, 13)
TIPOS = [
    ("yellow", "yellow_tripdata_{ano}-{mes:02d}.parquet"),
    ("green", "green_tripdata_{ano}-{mes:02d}.parquet"),
    ("fhv", "fhv_tripdata_{ano}-{mes:02d}.parquet"),
    ("fhvhv", "fhvhv_tripdata_{ano}-{mes:02d}.parquet"),
]

ADLS_CONN_STRING =  os.getenv(ADLS_CONN_STRING)

def baixar_arquivo_local(url):
    nome_arquivo = os.path.join(LOCAL_DIR, url.split("/")[-1])
    if not os.path.exists(nome_arquivo):
        resp = requests.get(url, stream=True)
        if resp.status_code == 200:
            with open(nome_arquivo, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Baixado: {nome_arquivo}")
        else:
            print(f"Arquivo não encontrado: {url}")
    else:
        print(f"Já existe: {nome_arquivo}")

def baixar_dados_taxi_local(anos=ANOS, meses=MESES):
    urls = []
    for ano in anos:
        for mes in meses:
            for _, pattern in TIPOS:
                url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{pattern.format(ano=ano, mes=mes)}"
                urls.append(url)
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(baixar_arquivo_local, urls)

def upload_arquivo_blob(container_client, blob_path, url):
    resp = requests.get(url, stream=True)
    if resp.status_code == 200:
        container_client.upload_blob(
            name=blob_path,
            data=resp.raw,
            overwrite=True
        )
        print(f"Enviado: {blob_path}")
    else:
        print(f"Arquivo não encontrado: {url}")

def upload_dados_taxi_blob(
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

    def process_ano(ano):
        tasks = []
        for mes in meses:
            for tipo, pattern in TIPOS:
                file_name = pattern.format(ano=ano, mes=mes)
                url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
                blob_path = f"source/{tipo}/{ano}/{file_name}"
                tasks.append((container_client, blob_path, url, skip_existing))

        def worker(args):
            container_client, blob_path, url, skip_existing = args
            if skip_existing:
                try:
                    container_client.get_blob_client(blob_path).get_blob_properties()
                    print(f"Pulado (já existe): {blob_path}")
                    return
                except Exception:
                    pass  # Blob não existe, pode enviar
            upload_arquivo_blob(container_client, blob_path, url)

        with ThreadPoolExecutor(max_workers=4) as executor:
            executor.map(worker, tasks)

    with ThreadPoolExecutor(max_workers=4) as ano_executor:
        ano_executor.map(process_ano, anos)

# Exemplo de uso (descomente para rodar):

# Para baixar localmente:
# baixar_dados_taxi_local()

# Para enviar para o Azure Blob Storage usando connection string:
upload_dados_taxi_blob(
    connection_string=ADLS_CONN_STRING,
    container_name="taxi"
)
