import requests
import google.cloud.bigquery as bigquery
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import pandas as pd

cred={'type': 'service_account', 'project_id': 'sys-67738525349545571962304921', 'private_key_id': '91927d3950bfa5e5249a0cc4f42d4625a99f4503', 'private_key': '-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC1BaE36Via8sak\noerV17EZzGlXeBjgsscbVnLuyg6ACDlY7CrCljjdOfmn+4gKbe1491WwJtxuhr/0\ne4od5jt2HrKTfR5yiFb3EujfcZrl7kA8OPnSp7Z5StZoos4AHAMJBcgGGMfbHVA4\nQ+ZPjXXSIGWJdh8/SV0Es51VUQi+Lms3YK0fzufME4qceS9nybYmm3ccrZSG51zn\nhK7SNmRZZCZIfOQGmXa/hqZw17B1TFKIR9PefkYtWI8yTOvsZ6crzttiouQzulOl\n5DQwmbfOeEWMKT6HoXPTa+V381S7WRAK8YF98+DVqerjBPt3DND+/YL2ZwA/zqJz\ne5iz2D7jAgMBAAECggEADbW48ZabLtUPRV27/ukgkR8hpU3DuJThrojcGIi2E21M\nBpeQX39oHB0tctMCiSOtNhmpZDd1P2u2Mwp+Oeh7fWUyyifSPANmbr0AZRfiDuL9\n+3GnPhSUpdgMqA0Yg/qbIj5NWWTcEhTExBYkZccFct4gQopvMGhagqYl1tXVzy1t\nNpE0Ge+uishasM0AE0c6eGppyfm7zb/Df0Q6vDfmGHSiO4WCwLjUm9BayGS68Pt1\n7MTOqO3B5Eo4KTcahCY41+iXvu8K2EZ1DRRS9xvBs0pBqK68fSuQSMSUzF18CHzb\n5tg/OgLWQxiSs8jOLUHox6k4dkF2NejqYh3plCsnQQKBgQDYjtcAWlbCalDKWjX8\noV3YOuwY1LrhsF8IuCnynkAlKKrleZHmbFubrYN6C1GDKo4QPM5j7IuHRXEz8Pfv\nsym9VJPeOqE0NBy8HQV3NtLzwypaNUlGi250uSEpddSS7p0AL7p5mhSet9aTSdBq\n6cSVbm13GFkjoFvwir+kEFRCwQKBgQDV/ea10Npmf8IBKO23d5Is5xMvHry8MzZI\noVpFWPym5HS5XEA06APDd3ccyCndDcJQAJld6C3CLbTxZdWhg2WGSpWnBarlSVQh\nUcRawHAMvSR0VGeUjOuQvNecutpEx5u6qs2AaT7kj2fQyOnH3Ux1w+GbtytdhOfI\nyBjIgsc+owKBgGrlZ1+3OChTjnm0Of3wMYCw5SYErBMHmoGVVq96SjONdX48mjZh\nun6IEeRGff//G40MVtygQOeO8agwBFL/31Sj0THbQwOfzadVtAL6vvqwldFdiEQY\nQ3e+go4SqdG1ky4qYSPxWMhX+sVNpGGB7xXMIqCtFiMt3vRHqP11SgKBAoGBAIDk\n3Xl4YoTIwV+XepA+6oI3cVu5hO9LXZAj+E67CfuwsgoQYfA8LEApjkp82pJ2visY\nIUjqF93VUB7zOtl9XsKj3D5tcIGJSK6FJOOQ9C0IJJQZXwagVyeoR6r09ZHmNYwb\nY4rMWgCrzFl7Gy2yw2JP6W20x98dtcs/k4X7F+5HAoGAXLQcxSEn3yDqYcqqihTd\nCwwj/C8PCVtExuY2nbd5K72G6DeJOzqq5XRAYM+ONPlofSBioVXtxTMvdKDTR8xY\njnuU/XunKfMBJfboGxXkdHL7LJNuqE8DRG3El+TFGA/aCjFSLAKZyYGmD7H2FXr4\nVYxLh0cycwfaBLeKUC9Sg2A=\n-----END PRIVATE KEY-----\n', 'client_email': 'python-2@sys-67738525349545571962304921.iam.gserviceaccount.com', 'client_id': '116817448104344134353', 'auth_uri': 'https://accounts.google.com/o/oauth2/auth', 'token_uri': 'https://oauth2.googleapis.com/token', 'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs', 'client_x509_cert_url': 'https://www.googleapis.com/robot/v1/metadata/x509/python-2%40sys-67738525349545571962304921.iam.gserviceaccount.com', 'universe_domain': 'googleapis.com'}
credentials = service_account.Credentials.from_service_account_info(cred)




def get_data_from_pipedrive(url, token, timestamp, max_attempts=3, params=None):
    all_data = []
    page = 0
    has_more = True
    url_request = url + "/api/v1/recents?"

    while has_more:
        request_params = {
            "start": page * 500,
            "limit": 500,
            "items": "activity",
            "api_token": token,
            "since_timestamp": timestamp
        }

        headers = {
            "Content-Type": "application/json"
        }

        if params:
            request_params.update(params)

        attempt = 0
        success = False

        # Loop de tentativas
        while attempt < max_attempts and not success:
            try:
                response = requests.get(url_request, headers=headers, params=request_params)
                response_data = response.json()  # Convertendo a resposta para JSON uma única vez

                if response_data['success']:
                    print(response_data["additional_data"]["pagination"]['start'])
                    data = response_data["data"]
                    all_data.extend(data)
                    has_more = response_data["additional_data"]["pagination"]["more_items_in_collection"]
                    success = True  # Marcar como sucesso para sair do loop de tentativas
                else:
                    print(f"Error fetching data: {response_data.get('error')}")
                    attempt += 1
            except Exception as e:
                print(f"Error during fetch attempt {attempt + 1}: {str(e)}")
                attempt += 1

        # Se todas as tentativas falharem, lançar um erro
        if not success:
            raise Exception(f"Failed to fetch data after {max_attempts} attempts")

        page += 1

    return all_data



def update_bigquery_table(data, datasetID ,table_id,lista):
    # Inicializa o cliente do BigQuery
    projectID = 'sys-67738525349545571962304921'
    cred={'type': 'service_account', 'project_id': 'sys-67738525349545571962304921', 'private_key_id': '91927d3950bfa5e5249a0cc4f42d4625a99f4503', 'private_key': '-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC1BaE36Via8sak\noerV17EZzGlXeBjgsscbVnLuyg6ACDlY7CrCljjdOfmn+4gKbe1491WwJtxuhr/0\ne4od5jt2HrKTfR5yiFb3EujfcZrl7kA8OPnSp7Z5StZoos4AHAMJBcgGGMfbHVA4\nQ+ZPjXXSIGWJdh8/SV0Es51VUQi+Lms3YK0fzufME4qceS9nybYmm3ccrZSG51zn\nhK7SNmRZZCZIfOQGmXa/hqZw17B1TFKIR9PefkYtWI8yTOvsZ6crzttiouQzulOl\n5DQwmbfOeEWMKT6HoXPTa+V381S7WRAK8YF98+DVqerjBPt3DND+/YL2ZwA/zqJz\ne5iz2D7jAgMBAAECggEADbW48ZabLtUPRV27/ukgkR8hpU3DuJThrojcGIi2E21M\nBpeQX39oHB0tctMCiSOtNhmpZDd1P2u2Mwp+Oeh7fWUyyifSPANmbr0AZRfiDuL9\n+3GnPhSUpdgMqA0Yg/qbIj5NWWTcEhTExBYkZccFct4gQopvMGhagqYl1tXVzy1t\nNpE0Ge+uishasM0AE0c6eGppyfm7zb/Df0Q6vDfmGHSiO4WCwLjUm9BayGS68Pt1\n7MTOqO3B5Eo4KTcahCY41+iXvu8K2EZ1DRRS9xvBs0pBqK68fSuQSMSUzF18CHzb\n5tg/OgLWQxiSs8jOLUHox6k4dkF2NejqYh3plCsnQQKBgQDYjtcAWlbCalDKWjX8\noV3YOuwY1LrhsF8IuCnynkAlKKrleZHmbFubrYN6C1GDKo4QPM5j7IuHRXEz8Pfv\nsym9VJPeOqE0NBy8HQV3NtLzwypaNUlGi250uSEpddSS7p0AL7p5mhSet9aTSdBq\n6cSVbm13GFkjoFvwir+kEFRCwQKBgQDV/ea10Npmf8IBKO23d5Is5xMvHry8MzZI\noVpFWPym5HS5XEA06APDd3ccyCndDcJQAJld6C3CLbTxZdWhg2WGSpWnBarlSVQh\nUcRawHAMvSR0VGeUjOuQvNecutpEx5u6qs2AaT7kj2fQyOnH3Ux1w+GbtytdhOfI\nyBjIgsc+owKBgGrlZ1+3OChTjnm0Of3wMYCw5SYErBMHmoGVVq96SjONdX48mjZh\nun6IEeRGff//G40MVtygQOeO8agwBFL/31Sj0THbQwOfzadVtAL6vvqwldFdiEQY\nQ3e+go4SqdG1ky4qYSPxWMhX+sVNpGGB7xXMIqCtFiMt3vRHqP11SgKBAoGBAIDk\n3Xl4YoTIwV+XepA+6oI3cVu5hO9LXZAj+E67CfuwsgoQYfA8LEApjkp82pJ2visY\nIUjqF93VUB7zOtl9XsKj3D5tcIGJSK6FJOOQ9C0IJJQZXwagVyeoR6r09ZHmNYwb\nY4rMWgCrzFl7Gy2yw2JP6W20x98dtcs/k4X7F+5HAoGAXLQcxSEn3yDqYcqqihTd\nCwwj/C8PCVtExuY2nbd5K72G6DeJOzqq5XRAYM+ONPlofSBioVXtxTMvdKDTR8xY\njnuU/XunKfMBJfboGxXkdHL7LJNuqE8DRG3El+TFGA/aCjFSLAKZyYGmD7H2FXr4\nVYxLh0cycwfaBLeKUC9Sg2A=\n-----END PRIVATE KEY-----\n', 'client_email': 'python-2@sys-67738525349545571962304921.iam.gserviceaccount.com', 'client_id': '116817448104344134353', 'auth_uri': 'https://accounts.google.com/o/oauth2/auth', 'token_uri': 'https://oauth2.googleapis.com/token', 'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs', 'client_x509_cert_url': 'https://www.googleapis.com/robot/v1/metadata/x509/python-2%40sys-67738525349545571962304921.iam.gserviceaccount.com', 'universe_domain': 'googleapis.com'}
    credentials = service_account.Credentials.from_service_account_info(cred)
    client = bigquery.Client(project=projectID, credentials=credentials)

    
    # datasetID and table_id are passed as arguments

    dataset = {
        "dataset_id": datasetID,
        "table_id": table_id,
        "data": data
    }

    # Trate 'data' conforme necessário para inserir no BigQuery

    # Query para deletar registros antigos
    delete_query = f"""
    DELETE FROM `{projectID}.{datasetID}.{table_id}`
    WHERE id in {lista}
    """

    #print(delete_query)
    client.query(delete_query).result()

    # Inserindo os novos registros
    # Vou assumir que você tem uma lista de registros e que eles estão em formato de dicionários

    #table_id_full = f"{projectID}.{datasetID}.{table_id}"
    
    df = pd.DataFrame(data)

    # for i in df.columns:print(i)

    # Carregar os dados do DataFrame diretamente no BigQuery
    table_ref = f"{projectID}.{dataset['dataset_id']}.{dataset['table_id']}"
    load_config = bigquery.LoadJobConfig()
    #load_config.source_format = bigquery.SourceFormat.PARQUET
    #load_config.max_bad_records = 100
    #load_config.autodetect = True
    #load_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=load_config)
        job.result()  # Wait for the loading job to complete
        return {"message": "Data successfully inserted into BigQuery"}
    except Exception as e:
        return {"message": "Failed to insert data into BigQuery", "error": str(e)}







def extrair_valores(lista_dicionarios, chave):
    return [d[chave] for d in lista_dicionarios if chave in d]

def lista_para_texto(lista):
    return f"({', '.join(map(str, lista))})"





def main():
    # Definindo os valores
    url = "https://feedz.pipedrive.com"
    token = "eb0a9b51a720dd07f0921beedac1bf441b2cc476"
    timestamp = "2023-10-20 00:00:00"
    table_id = "pipedrive_activities"
    datasetID = 'TESTESCRIPT'

    # Buscando dados do Pipedrive
    data = get_data_from_pipedrive(url, token, timestamp)
    atualiza=[]
    for item in data:
        if item['data']['active_flag']==True:
            atualiza.append(item['data'])

    lista=lista_para_texto(extrair_valores(atualiza,'id'))


    
    # Tratando os dados


    # Atualizando a tabela no BigQuery
    update_bigquery_table(atualiza, datasetID,table_id,lista)

if __name__ == "__main__":
    main()
