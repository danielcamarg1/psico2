import os
import json
import pandas as pd
import tempfile
from datetime import datetime
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from flask import Flask, jsonify, request

app = Flask(__name__)

# 🔒 Token seguro para acesso à API
TOKEN_ACESSO = "f9a3d2a5c8b14e2a94db394a7c8e48fa"

@app.route("/")  # 🟢 Para o UptimeRobot
def rota_raiz():
    return "🟢 Servidor ativo", 200

@app.route("/executar")
def rota_executar():
    try:
        executar()
        return "✅ Script executado com sucesso"
    except Exception as e:
        return f"❌ Erro na execução: {str(e)}"

@app.route("/dados")
def rota_dados():
    token_usuario = request.args.get("token")
    if token_usuario != TOKEN_ACESSO:
        return jsonify({"erro": "Token inválido"}), 403

    try:
        SCOPES = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets'
        ]
        SERVICE_ACCOUNT_INFO = json.loads(os.environ["GOOGLE_CREDENTIALS"])
        creds = Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)
        gc = gspread.authorize(creds)

        nome_planilha = "Base Consolidada Prontuários"
        planilha = gc.open(nome_planilha)
        aba = planilha.get_worksheet(0)
        valores = aba.get_all_values()

        if not valores:
            return jsonify([])

        df = pd.DataFrame(valores[1:], columns=valores[0])
        return jsonify(df.to_dict(orient='records'))

    except Exception as e:
        return jsonify({"erro": str(e)}), 500

def executar():
    SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
    GOOGLE_CREDENTIALS = os.environ.get('GOOGLE_CREDENTIALS')
    creds_dict = json.loads(GOOGLE_CREDENTIALS)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)

    gc = gspread.authorize(creds)
    drive_service = build('drive', 'v3', credentials=creds)

    print("🔐 Autenticação com a conta de serviço concluída.")

    DADOS_FOLDER_ID = '1mxpERYY4ormmLjxYUOyEKQoePhjLetJp'
    PRONTUARIOS_FOLDER_ID = '1MnwxFAo15ZsOvTjBJcVEgIQcOCUbmekd'

    def calcular_idade(data_nascimento_str):
        try:
            nascimento = pd.to_datetime(data_nascimento_str, errors='coerce', dayfirst=True)
            hoje = pd.to_datetime('today')
            idade = hoje.year - nascimento.year - ((hoje.month, hoje.day) < (nascimento.month, nascimento.day))
            return idade
        except:
            return None

    def baixar_arquivos_xls(folder_id):
        arquivos = []
        response = drive_service.files().list(
            q=f"'{folder_id}' in parents and (mimeType contains 'spreadsheet' or name contains '.xls')",
            spaces='drive',
            fields="files(id, name)"
        ).execute()
        print(f"📂 {len(response.get('files', []))} arquivos encontrados na pasta {folder_id}")
        for file in response.get('files', []):
            request = drive_service.files().get_media(fileId=file['id'])
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xls')
            with open(temp_file.name, 'wb') as f:
                downloader = drive_service._http.request(request.uri)
                f.write(downloader[1])
            arquivos.append((file['name'], temp_file.name))
        return arquivos

    dados_colunas = ['Nome Completo', 'Data de Nascimento', 'Sexo', 'Cidade', 'Profissão']
    dados_df_list = []
    for nome, caminho in baixar_arquivos_xls(DADOS_FOLDER_ID):
        try:
            df_temp = pd.read_excel(caminho, nrows=1)
            colunas_validas = [col for col in df_temp.columns if col in dados_colunas]
            if not colunas_validas:
                continue
            df = pd.read_excel(caminho, usecols=colunas_validas)
            dados_df_list.append(df)
        except Exception as e:
            print(f"Erro lendo {nome}: {e}")

    dados_df = pd.concat(dados_df_list, ignore_index=True)
    dados_df.rename(columns={
        'Nome Completo': 'Nome',
        'Data de Nascimento': 'Data_Nascimento',
        'Sexo': 'Sexo',
        'Cidade': 'Cidade',
        'Profissão': 'Profissao'
    }, inplace=True)
    dados_df['Idade'] = dados_df['Data_Nascimento'].apply(calcular_idade)

    pront_colunas = ['Nome do Paciente', 'Diagnóstico', 'Plano de Tratamento', 'Avaliação da Demanda', 'Registro de Encerramento']
    prontuarios_df_list = []
    for nome, caminho in baixar_arquivos_xls(PRONTUARIOS_FOLDER_ID):
        try:
            df_temp = pd.read_excel(caminho, nrows=1)
            colunas_validas = [col for col in df_temp.columns if col in pront_colunas]
            if not colunas_validas:
                continue
            df = pd.read_excel(caminho, usecols=colunas_validas)
            prontuarios_df_list.append(df)
        except Exception as e:
            print(f"Erro lendo {nome}: {e}")

    prontuarios_df = pd.concat(prontuarios_df_list, ignore_index=True)
    prontuarios_df.rename(columns={'Nome do Paciente': 'Nome'}, inplace=True)

    base_consolidada = pd.merge(prontuarios_df, dados_df, on='Nome', how='left')

    nome_planilha = "Base Consolidada Prontuários"
    planilha = gc.open(nome_planilha)
    aba = planilha.get_worksheet(0) or planilha.add_worksheet(title="Consolidado", rows="1000", cols="20")
    aba.clear()
    set_with_dataframe(aba, base_consolidada)

    print(f"✅ Planilha atualizada com sucesso: https://docs.google.com/spreadsheets/d/{planilha.id}/edit")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
