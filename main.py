import requests
import pandas as pd
import json
import unicodedata
import os
import re
import time
from datetime import datetime, timedelta
from telegram import Bot

# ================= CONFIG =================

MAX_PAGINAS = 80
DIAS_BUSCA = 30
VALOR_MINIMO = 0
UF_FILTRO = []

VALOR_BONUS_GRANDE = 1000000
BONUS_GRANDE = 3

TIMEOUT_REQUEST = 30
MAX_RETRIES = 3

MODALIDADES = {
    1: "Concorrência",
    2: "Tomada de Preços",
    3: "Convite",
    6: "Pregão",
    7: "Dispensa",
    8: "Inexigibilidade",
    9: "RDC"
}

BASE_URL = "https://pncp.gov.br/pncp-consulta/v1/contratacoes/publicacao"

BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ================= FUNÇÕES =================

def normalizar(texto):
    if pd.isna(texto):
        return ""
    texto = unicodedata.normalize("NFKD", str(texto))
    return texto.encode("ASCII", "ignore").decode("ASCII").lower()

def request_com_retry(url, params):
    for tentativa in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT_REQUEST)
            if r.status_code == 200:
                return r
        except Exception:
            pass
        time.sleep(2)
    return None

def carregar_historico():
    if os.path.exists("historico_ids.json"):
        with open("historico_ids.json", "r") as f:
            return set(json.load(f))
    return set()

def salvar_historico(ids):
    with open("historico_ids.json", "w") as f:
        json.dump(list(ids), f)

def carregar_palavras():
    df = pd.read_excel("palavras_chave.xlsx")
    df.columns = df.columns.str.strip().str.lower()

    if "empresa" not in df.columns or "palavra" not in df.columns:
        raise Exception("Colunas 'empresa' e 'palavra' obrigatórias.")

    df["palavra_norm"] = df["palavra"].apply(normalizar)
    df = df[df["palavra_norm"].str.strip() != ""]

    mapa = (
        df.groupby("empresa")["palavra_norm"]
        .apply(list)
        .to_dict()
    )

    print(f"🏢 Empresas carregadas: {len(mapa)}")
    return mapa

def enviar_telegram(arquivo, mensagem):
    bot = Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=mensagem, parse_mode="HTML")
    with open(arquivo, "rb") as f:
        bot.send_document(chat_id=CHAT_ID, document=f)

def limpar_excel(valor):
    if isinstance(valor, str):
        return re.sub(r"[\x00-\x1F\x7F]", "", valor)
    return valor

def match_avancado(descricao, palavras):
    score = 0
    for p in palavras:
        termos = p.split()

        if all(t in descricao for t in termos):
            score += 2
        elif any(t in descricao for t in termos):
            score += 1

    return score

def classificar(score):
    if score >= 8:
        return "🔥 ALTÍSSIMA"
    elif score >= 5:
        return "🚀 ALTA"
    elif score >= 3:
        return "⚡ MÉDIA"
    else:
        return "🟢 BAIXA"

# ================= EXECUÇÃO =================

mapa_palavras = carregar_palavras()
ids_vistos = carregar_historico()
novos_ids = set(ids_vistos)

data_final = datetime.now()
data_inicial = data_final - timedelta(days=DIAS_BUSCA)

resultados = []
inicio_execucao = datetime.now()

for codigo_modalidade, nome_modalidade in MODALIDADES.items():

    print(f"\n🔎 Processando modalidade: {nome_modalidade}")

    pagina = 1
    total_modalidade = 0

    while pagina <= MAX_PAGINAS:

        params = {
            "dataInicial": data_inicial.strftime("%Y%m%dT00:00:00"),
            "dataFinal": data_final.strftime("%Y%m%dT23:59:59"),
            "codigoModalidadeContratacao": codigo_modalidade,
            "pagina": pagina,
            "tamanhoPagina": 50
        }

        r = request_com_retry(BASE_URL, params)

        if not r:
            print("⚠ Falha na API.")
            break

        dados = r.json()
        lista = dados.get("data", [])
        total_paginas = dados.get("totalPaginas", 1)

        if not lista:
            break

        for item in lista:

            total_modalidade += 1

            descricao_original = item.get("objetoCompra", "")
            descricao = normalizar(descricao_original)
            valor = item.get("valorTotalEstimado") or 0
            uf = item.get("unidadeOrgao", {}).get("ufSigla", "")
            numero = str(item.get("numeroControlePNCP"))

            if UF_FILTRO and uf not in UF_FILTRO:
                continue

            if valor < VALOR_MINIMO:
                continue

            for empresa, palavras in mapa_palavras.items():

                score = match_avancado(descricao, palavras)

                if valor > VALOR_BONUS_GRANDE:
                    score += BONUS_GRANDE

                if score == 0:
                    continue

                status = "🆕 NOVA" if numero not in ids_vistos else "✔ JÁ ANALISADA"

                novos_ids.add(numero)

                resultados.append({
                    "empresa": empresa,
                    "modalidade": nome_modalidade,
                    "numero": numero,
                    "orgao": item.get("orgaoEntidade", {}).get("razaoSocial", ""),
                    "uf": uf,
                    "objeto": descricao_original,
                    "valor": valor,
                    "score": score,
                    "prioridade": classificar(score),
                    "status": status,
                    "link_pncp": f'=HYPERLINK("https://pncp.gov.br/app/editais/{numero}";"Abrir PNCP")',
                    "link_orgao": f'=HYPERLINK("{item.get("linkSistemaOrigem","")}";"Sistema Órgão")'
                })

        if pagina >= total_paginas:
            break

        pagina += 1

    print(f"📊 Registros varridos na modalidade: {total_modalidade}")

# ================= EXPORTAÇÃO =================

df = pd.DataFrame(resultados)

if not df.empty:

    df = df.drop_duplicates(subset=["empresa","numero"])

    for col in df.columns:
        df[col] = df[col].map(limpar_excel)

    df = df.sort_values(["empresa","score"], ascending=[True,False])

    nome_arquivo = f"relatorio_pncp_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"

    with pd.ExcelWriter(nome_arquivo, engine="openpyxl") as writer:

        df.to_excel(writer, sheet_name="GERAL", index=False)

        for empresa in df["empresa"].unique():
            df_empresa = df[df["empresa"] == empresa]
            df_empresa.to_excel(writer, sheet_name=str(empresa)[:31], index=False)

    salvar_historico(novos_ids)

    total_geral = len(df)
    total_novas = len(df[df["status"]=="🆕 NOVA"])

    mensagem = f"""
<b>📊 RADAR PNCP ENTERPRISE</b>

🔎 Total oportunidades: <b>{total_geral}</b>
🆕 Novas oportunidades: <b>{total_novas}</b>

📅 Período analisado: últimos {DIAS_BUSCA} dias
"""

    enviar_telegram(nome_arquivo, mensagem)

    print("✅ Relatório enviado com sucesso.")

else:
    print("Nenhuma oportunidade encontrada.")

tempo_total = (datetime.now() - inicio_execucao).total_seconds()
print(f"\n⏱ Tempo total execução: {round(tempo_total,2)} segundos")
