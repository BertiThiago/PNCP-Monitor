import requests
import pandas as pd
import json
import unicodedata
import os
from datetime import datetime, timedelta
from telegram import Bot

# ================= CONFIG =================
MAX_PAGINAS = 50
DIAS_BUSCA = 30
VALOR_MINIMO = 0
UF_FILTRO = []

MODALIDADES = {
    1: "Concorrência",
    2: "Tomada de Preços",
    3: "Convite",
    6: "Pregão",
    7: "Dispensa",
    8: "Inexigibilidade",
    9: "RDC"
}

url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"

BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ================= FUNÇÕES =================

def normalizar(texto):
    texto = texto.lower()
    texto = unicodedata.normalize('NFD', texto)
    texto = texto.encode('ascii', 'ignore').decode('utf-8')
    return texto

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
    df.columns = df.columns.str.strip()

    mapa = {}

    for _, row in df.iterrows():
        palavra = normalizar(str(row["palavra"]))
        empresa = str(row["empresa"]).strip()

        if empresa not in mapa:
            mapa[empresa] = []

        mapa[empresa].append(palavra)

    return mapa


def enviar_telegram(arquivo, mensagem):
    bot = Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=mensagem, parse_mode="HTML")
    with open(arquivo, "rb") as f:
        bot.send_document(chat_id=CHAT_ID, document=f)

# ================= EXECUÇÃO =================

mapa_palavras = carregar_palavras()
ids_vistos = carregar_historico()
novos_ids = set(ids_vistos)

data_final = datetime.today()
data_inicial = data_final - timedelta(days=DIAS_BUSCA)

resultados_por_empresa = {}
total_novas = 0
total_geral = 0

for codigo_modalidade, nome_modalidade in MODALIDADES.items():

    pagina = 1

    while pagina <= MAX_PAGINAS:

        params = {
    "pagina": pagina,
    "tamanhoPagina": 50,
    "codigoModalidadeContratacao": codigo_modalidade,
    "dataInicial": data_inicial.strftime("%Y%m%d"),
    "dataFinal": data_final.strftime("%Y%m%d")
}


        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json"
        }

        response = requests.get(url, params=params, headers=headers)

        print("Status code:", response.status_code)
        print("Resposta bruta:", response.text[:500])

        if response.status_code != 200:
            break

        dados = response.json()
        lista = dados.get("data", [])

        if not lista:
            break

        for item in lista:

            numero = str(item.get("numeroControlePNCP"))
            descricao_original = item.get("objetoCompra", "")
            descricao = normalizar(descricao_original)
            valor = item.get("valorTotalEstimado") or 0
            uf = item.get("unidadeOrgao", {}).get("ufSigla", "")

            if UF_FILTRO and uf not in UF_FILTRO:
                continue

            if valor < VALOR_MINIMO:
                continue

            for empresa, palavras in mapa_palavras.items():

                matches = [p for p in palavras if p in descricao]
                score = len(matches)

                if score == 0:
                    continue

                total_geral += 1

                status = "🆕 NOVA" if numero not in ids_vistos else "✔ JÁ ANALISADA"

                if numero not in ids_vistos:
                    total_novas += 1

                novos_ids.add(numero)

                resultados_por_empresa.setdefault(empresa, []).append({
                    "Modalidade": nome_modalidade,
                    "Número PNCP": numero,
                    "Órgão": item.get("orgaoEntidade", {}).get("razaoSocial", ""),
                    "UF": uf,
                    "objeto": descricao_original,
                    "Valor Estimado": valor,
                    "Score": score,
                    "Status": status,
                    "Link PNCP": f'=HYPERLINK("https://pncp.gov.br/app/editais/{numero}";"Abrir PNCP")',
                    "Link Órgão": f'=HYPERLINK("{item.get("linkSistemaOrigem","")}";"Sistema Órgão")'
                })

        pagina += 1

df = pd.DataFrame()  # 👈 garante que sempre exista

if resultados_por_empresa:

    nome_arquivo = f"relatorio_pncp_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"

    with pd.ExcelWriter(nome_arquivo, engine="openpyxl") as writer:

        resumo = []

        for empresa, dados in resultados_por_empresa.items():
            df = pd.DataFrame(dados)
            df.sort_values(by="Score", ascending=False, inplace=True)
            df.to_excel(writer, sheet_name=empresa[:30], index=False)

            resumo.append({
                "Empresa": empresa,
                "Total": len(df),
                "Novas": len(df[df["Status"] == "🆕 NOVA"])
            })

        df_resumo = pd.DataFrame(resumo)
        df_resumo.to_excel(writer, sheet_name="Resumo", index=False)

    salvar_historico(novos_ids)

    mensagem = f"""
<b>📊 RELATÓRIO PNCP</b>

🔎 Total oportunidades: <b>{total_geral}</b>
🆕 Novas oportunidades: <b>{total_novas}</b>

📅 Período analisado: últimos {DIAS_BUSCA} dias
"""

    enviar_telegram(nome_arquivo, mensagem)

else:
    print("Nenhuma oportunidade encontrada.")

