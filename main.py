import requests
import google.cloud.bigquery as bigquery
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import pandas as pd
import unicodedata
import re

cred={'type': 'service_account', 'project_id': 'sys-67738525349545571962304921', 'private_key_id': '91927d3950bfa5e5249a0cc4f42d4625a99f4503', 'private_key': '-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC1BaE36Via8sak\noerV17EZzGlXeBjgsscbVnLuyg6ACDlY7CrCljjdOfmn+4gKbe1491WwJtxuhr/0\ne4od5jt2HrKTfR5yiFb3EujfcZrl7kA8OPnSp7Z5StZoos4AHAMJBcgGGMfbHVA4\nQ+ZPjXXSIGWJdh8/SV0Es51VUQi+Lms3YK0fzufME4qceS9nybYmm3ccrZSG51zn\nhK7SNmRZZCZIfOQGmXa/hqZw17B1TFKIR9PefkYtWI8yTOvsZ6crzttiouQzulOl\n5DQwmbfOeEWMKT6HoXPTa+V381S7WRAK8YF98+DVqerjBPt3DND+/YL2ZwA/zqJz\ne5iz2D7jAgMBAAECggEADbW48ZabLtUPRV27/ukgkR8hpU3DuJThrojcGIi2E21M\nBpeQX39oHB0tctMCiSOtNhmpZDd1P2u2Mwp+Oeh7fWUyyifSPANmbr0AZRfiDuL9\n+3GnPhSUpdgMqA0Yg/qbIj5NWWTcEhTExBYkZccFct4gQopvMGhagqYl1tXVzy1t\nNpE0Ge+uishasM0AE0c6eGppyfm7zb/Df0Q6vDfmGHSiO4WCwLjUm9BayGS68Pt1\n7MTOqO3B5Eo4KTcahCY41+iXvu8K2EZ1DRRS9xvBs0pBqK68fSuQSMSUzF18CHzb\n5tg/OgLWQxiSs8jOLUHox6k4dkF2NejqYh3plCsnQQKBgQDYjtcAWlbCalDKWjX8\noV3YOuwY1LrhsF8IuCnynkAlKKrleZHmbFubrYN6C1GDKo4QPM5j7IuHRXEz8Pfv\nsym9VJPeOqE0NBy8HQV3NtLzwypaNUlGi250uSEpddSS7p0AL7p5mhSet9aTSdBq\n6cSVbm13GFkjoFvwir+kEFRCwQKBgQDV/ea10Npmf8IBKO23d5Is5xMvHry8MzZI\noVpFWPym5HS5XEA06APDd3ccyCndDcJQAJld6C3CLbTxZdWhg2WGSpWnBarlSVQh\nUcRawHAMvSR0VGeUjOuQvNecutpEx5u6qs2AaT7kj2fQyOnH3Ux1w+GbtytdhOfI\nyBjIgsc+owKBgGrlZ1+3OChTjnm0Of3wMYCw5SYErBMHmoGVVq96SjONdX48mjZh\nun6IEeRGff//G40MVtygQOeO8agwBFL/31Sj0THbQwOfzadVtAL6vvqwldFdiEQY\nQ3e+go4SqdG1ky4qYSPxWMhX+sVNpGGB7xXMIqCtFiMt3vRHqP11SgKBAoGBAIDk\n3Xl4YoTIwV+XepA+6oI3cVu5hO9LXZAj+E67CfuwsgoQYfA8LEApjkp82pJ2visY\nIUjqF93VUB7zOtl9XsKj3D5tcIGJSK6FJOOQ9C0IJJQZXwagVyeoR6r09ZHmNYwb\nY4rMWgCrzFl7Gy2yw2JP6W20x98dtcs/k4X7F+5HAoGAXLQcxSEn3yDqYcqqihTd\nCwwj/C8PCVtExuY2nbd5K72G6DeJOzqq5XRAYM+ONPlofSBioVXtxTMvdKDTR8xY\njnuU/XunKfMBJfboGxXkdHL7LJNuqE8DRG3El+TFGA/aCjFSLAKZyYGmD7H2FXr4\nVYxLh0cycwfaBLeKUC9Sg2A=\n-----END PRIVATE KEY-----\n', 'client_email': 'python-2@sys-67738525349545571962304921.iam.gserviceaccount.com', 'client_id': '116817448104344134353', 'auth_uri': 'https://accounts.google.com/o/oauth2/auth', 'token_uri': 'https://oauth2.googleapis.com/token', 'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs', 'client_x509_cert_url': 'https://www.googleapis.com/robot/v1/metadata/x509/python-2%40sys-67738525349545571962304921.iam.gserviceaccount.com', 'universe_domain': 'googleapis.com'}
credentials = service_account.Credentials.from_service_account_info(cred)


def get_data_from_pipedrive(url, token, timestamp,item, max_attempts=3, params=None):
    all_data = []
    page = 0
    has_more = True
    url_request = url + "/api/v1/recents?"

    while has_more:
        request_params = {
            "start": page * 500,
            "limit": 500,
            "items": item,
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

    # try:
    job = client.load_table_from_dataframe(df, table_ref, job_config=load_config)
    job.result()  # Wait for the loading job to complete
    return {"message": "Data successfully inserted into BigQuery"}
    # except Exception as e:
    #     return {"message": "Failed to insert data into BigQuery", "error": str(e)}

def fetch_dealsfields_from_pipedrive(base_url, api_token):
    endpoint = f"{base_url}/v1/dealFields"
    headers = {
        "Content-Type": "application/json"
    }
    params={"api_token": api_token,"limit":500}

    response = requests.get(endpoint, headers=headers,params=params)
    
    if response.status_code != 200:
        print(f"Erro: {response.status_code}. Mensagem: {response.text}")
        return None
    
    data = response.json()
    
    return data.get("data", [])

def construir_tabela_auxiliar(field_data):
    tabela = {}
    
    for item in field_data:
        key = item['key']
        name = item['name']
        tipo = item['field_type']

        if tipo == 'enum':
            tabela[key] = {
                'novo_nome': name,
                'opcoes': {opt['id']: opt['label'] for opt in item['options']}
            }
        else:
            tabela[key] = {
                'novo_nome': name
            }
    return tabela

def ajusta_campos(lista_deals, tabela_auxiliar):
    for deals in lista_deals:
        chaves_para_remover = []
        atualizacoes = {}
        
        for key in deals:
            # Verifica se o valor associado à chave é um dicionário e tem a chave 'id' ou 'value' dentro dele
            if isinstance(deals[key], dict):
                if 'id' in deals[key]:
                    deals[key] = deals[key]['id']
                    continue  # após atualizar, vá para a próxima iteração
                elif 'value' in deals[key]:
                    deals[key] = deals[key]['value']
                    continue  # após atualizar, vá para a próxima iteração

            # Se a chave atual é 'id' ou não está na tabela auxiliar, continue sem alterações
            if key == 'id' or key not in tabela_auxiliar:
                continue

            mapeamento = tabela_auxiliar[key]
            # Ajusta o nome da coluna de acordo com as regras do BigQuery
            novo_nome = ajustar_nome_coluna(mapeamento['novo_nome'])

            if 'opcoes' in mapeamento:
                # Adicionando verificação para None
                valor = int(deals[key]) if deals[key] is not None else None
                if valor is not None:
                    atualizacoes[novo_nome] = mapeamento['opcoes'].get(valor, deals[key])
                else:
                    atualizacoes[novo_nome] = None
            else:
                atualizacoes[novo_nome] = deals[key]
                
            # Marca a chave antiga para ser removida após atualizar todas as chaves
            chaves_para_remover.append(key)
        
        # Atualiza o dicionário e remove as chaves antigas
        deals.update(atualizacoes)
        for chave in chaves_para_remover:
            del deals[chave]

def remove_keys_from_list_of_dicts(lst, keys_to_remove):
    for d in lst:
        for key in keys_to_remove:
            d.pop(key, None)

def remover_acentos(txt):
    nfkd = unicodedata.normalize('NFKD', txt)
    return u"".join([c for c in nfkd if not unicodedata.combining(c)])

def ajustar_nome_coluna(nome):
    # Substitui espaços por underscores
    nome_ajustado = nome.replace(" ", "_")
    
    # Remove acentuação
    nome_ajustado = remover_acentos(nome_ajustado)
    
    # Remove caracteres especiais restantes
    nome_ajustado = re.sub(r'[^a-zA-Z0-9_]', '', nome_ajustado)
    
    # Converte para minúsculo
    nome_ajustado = nome_ajustado.lower()
    
    return nome_ajustado

def extrair_valores(lista_dicionarios, chave):
    return [d[chave] for d in lista_dicionarios if chave in d]

def lista_para_texto(lista):
    return f"({', '.join(map(str, lista))})"

def recent_deals():
    # Definindo os valores
    url = "https://feedz.pipedrive.com"
    token = "eb0a9b51a720dd07f0921beedac1bf441b2cc476"
    timestamp = "2023-10-23 00:00:00"
    table_id = "pipedrive_deals"
    datasetID = 'TESTESCRIPT'
    item="deal"

    # Buscando dados do Pipedrive
    data = get_data_from_pipedrive(url, token, timestamp,item)
    atualiza=[]
    atualiza_query=[]
    for item in data:
        atualiza_query.append(item['data'])
        if item['data']['deleted']==False:
            atualiza.append(item['data'])

    lista=lista_para_texto(extrair_valores(atualiza_query,'id'))
    #print(atualiza)
    #print(dealsfields)

    dealsfields=fetch_dealsfields_from_pipedrive(url,token)

    for item in dealsfields:
        if item['mandatory_flag']:
            del item['mandatory_flag']
    #print(dealsfields)
    tabela_deals_fields = construir_tabela_auxiliar(dealsfields)



    ajusta_campos(atualiza,tabela_deals_fields)

    keys_to_remove = ['receita_perdida', 'lead_scoring']
    remove_keys_from_list_of_dicts(atualiza, keys_to_remove)
    
    # Tratando os dados
    df = pd.DataFrame(atualiza)
    df.to_csv('output.csv', index=False)

    # Atualizando a tabela no BigQuery
    update_bigquery_table(atualiza, datasetID,table_id,lista)

def recent_atividades():
    # Definindo os valores
    url = "https://feedz.pipedrive.com"
    token = "eb0a9b51a720dd07f0921beedac1bf441b2cc476"
    timestamp = "2023-10-23 00:00:00"
    table_id = "pipedrive_activities"
    datasetID = 'TESTESCRIPT'
    item="activity"

    # Buscando dados do Pipedrive
    data = get_data_from_pipedrive(url, token, timestamp,item)
    atualiza=[]
    atualiza_query=[]
    for item in data:
        atualiza_query.append(item['data'])
        if item['data']['active_flag']==True:
            atualiza.append(item['data'])

    lista=lista_para_texto(extrair_valores(atualiza_query,'id'))
    # print(len(atualiza))
    # print(len(atualiza_query))

    # df = pd.DataFrame(atualiza)
    # df.to_csv('output.csv', index=False)


    
    # Tratando os dados


    # Atualizando a tabela no BigQuery
    update_bigquery_table(atualiza, datasetID,table_id,lista)

if __name__ == "__main__":
    recent_deals()
    recent_atividades()