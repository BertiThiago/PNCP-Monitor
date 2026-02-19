import requests
import pandas as pd
import json
import unicodedata
import os
import re
# Regex oficial usada pelo openpyxl para caracteres inválidos
ILLEGAL_CHARACTERS_RE = re.compile(r'[\000-\010]|[\013-\014]|[\016-\037]')

def limpar_excel(valor):
    if isinstance(valor, str):
        return ILLEGAL_CHARACTERS_RE.sub("", valor)
    return valor

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

    colunas_obrigatorias = ["empresa", "palavra", "peso", "obrigatoria", "tipo"]

    for c in colunas_obrigatorias:
        if c not in df.columns:
            raise Exception(f"Coluna obrigatória ausente: {c}")

    df["palavra_norm"] = df["palavra"].apply(normalizar)
    df = df[df["palavra_norm"].str.strip() != ""]

    print(f"🏢 Empresas carregadas: {df['empresa'].nunique()}")

    return df

def enviar_telegram(arquivo, mensagem):
    bot = Bot(token=BOT_TOKEN)
    bot.send_message(chat_id=CHAT_ID, text=mensagem, parse_mode="HTML")
    with open(arquivo, "rb") as f:
        bot.send_document(chat_id=CHAT_ID, document=f)

def match_estrategico(descricao, df_empresa):

    score = 0
    encontrou_obrigatoria = False
    tecnicas_encontradas = 0

    for _, row in df_empresa.iterrows():

        palavra = row["palavra_norm"]
        peso = int(row["peso"])
        obrigatoria = str(row["obrigatoria"]).lower() == "sim"
        tipo = str(row["tipo"]).lower()

        termos = palavra.split()

        if all(t in descricao for t in termos):

            score += peso

            if obrigatoria:
                encontrou_obrigatoria = True

            if tipo == "tecnica":
                tecnicas_encontradas += 1

    aprovado = (
        encontrou_obrigatoria
        or score >= 6
        or tecnicas_encontradas >= 2
    )

    return aprovado, score


def classificar_score(score):
    if score >= 8:
        return "🔥 ALTÍSSIMA"
    elif score >= 5:
        return "🚀 ALTA"
    elif score >= 3:
        return "⚡ MÉDIA"
    else:
        return "🟢 BAIXA"

def formatar_data(data_str):
    if not data_str:
        return ""
    try:
        return datetime.fromisoformat(data_str.replace("Z","")).strftime("%d/%m/%Y %H:%M")
    except:
        return data_str

def calcular_dias_restantes(data_enc):
    if not data_enc:
        return None
    try:
        data_obj = datetime.fromisoformat(data_enc.replace("Z",""))
        return (data_obj - datetime.now()).days
    except:
        return None

def classificar_urgencia(dias):
    if dias is None:
        return ""
    if dias < 0:
        return "⚫ ENCERRADA"
    elif dias <= 5:
        return "🔴 URGENTE"
    elif dias <= 10:
        return "🟠 ATENÇÃO"
    else:
        return "🟢 NO PRAZO"

# ================= EXECUÇÃO =================

mapa_palavras = carregar_palavras()
ids_vistos = carregar_historico()
novos_ids = set(ids_vistos)

data_final = datetime.now()
data_inicial = data_final - timedelta(days=DIAS_BUSCA)

resultados = []
resultados_por_empresa = {}

for codigo_modalidade, nome_modalidade in MODALIDADES.items():

    print(f"\n🔎 Processando modalidade: {nome_modalidade}")
    pagina = 1

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
            break

        dados = r.json()
        lista = dados.get("data", [])
        total_paginas = dados.get("totalPaginas", 1)

        if not lista:
            break

        for item in lista:

            descricao_original = item.get("objetoCompra", "")
            descricao = normalizar(descricao_original)
            valor_raw = item.get("valorTotalEstimado")

            try:
                valor = float(valor_raw)
            except:
                valor = 0

            uf = item.get("unidadeOrgao", {}).get("ufSigla", "")
            numero = str(item.get("numeroControlePNCP") or "")

            data_publicacao_raw = item.get("dataPublicacaoPncp", "")
            data_encerramento_raw = item.get("dataEncerramentoProposta", "")
            dias_restantes = calcular_dias_restantes(data_encerramento_raw)

            if UF_FILTRO and uf not in UF_FILTRO:
                continue

            if valor < VALOR_MINIMO:
                continue

            for empresa in mapa_palavras["empresa"].unique():

                df_empresa = mapa_palavras[mapa_palavras["empresa"] == empresa]
            
                aprovado, score = match_estrategico(descricao, df_empresa)
            
                if not aprovado:
                    continue

                if valor > VALOR_BONUS_GRANDE:
                    score += BONUS_GRANDE

                if score == 0:
                    continue

                palavras_genericas_proibidas = [
                    "evento cultural",
                    "apresentacao artistica",
                    "festa",
                    "carnaval",
                    "show musical",
                    "orquestra"
                ]

                if any(p in descricao for p in palavras_genericas_proibidas):
                    continue

                status = "🆕 NOVA" if numero not in ids_vistos else "✔ JÁ ANALISADA"
                novos_ids.add(numero)

                registro = {
                    "empresa": empresa,
                    "modalidade": nome_modalidade,
                    "numero": numero,
                    "data_publicacao": formatar_data(data_publicacao_raw),
                    "data_encerramento": formatar_data(data_encerramento_raw),
                    "dias_restantes": dias_restantes,
                    "urgencia_prazo": classificar_urgencia(dias_restantes),
                    "orgao": item.get("orgaoEntidade", {}).get("razaoSocial", ""),
                    "uf": uf,
                    "objeto": descricao_original,
                    "valor": valor,
                    "score": score,
                    "prioridade_score": classificar_score(score),
                    "status": status,
                    "link_pncp": f"https://pncp.gov.br/app/editais/{numero}",
                    "link_orgao": item.get("linkSistemaOrigem","")
                }

                resultados.append(registro)
                resultados_por_empresa.setdefault(empresa, []).append(registro)

        if pagina >= total_paginas:
            break

        pagina += 1

# ================= EXPORTAÇÃO =================

from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

tem_dados = any(
    any(item.get("numero") for item in dados)
    for dados in resultados_por_empresa.values()
)

if tem_dados:

    nome_arquivo = f"relatorio_pncp_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"

    with pd.ExcelWriter(nome_arquivo, engine="openpyxl") as writer:

        resumo = []
        ids_ja_usados = set()

        for empresa, dados in resultados_por_empresa.items():

            dados_filtrados = []

            for item in dados:
                identificador = item.get("numero")
                if identificador and identificador not in ids_ja_usados:
                    ids_ja_usados.add(identificador)
                    dados_filtrados.append(item)

            df = pd.DataFrame(dados_filtrados)

            # Remove caracteres inválidos para Excel
            for coluna in df.columns:
                df[coluna] = df[coluna].apply(limpar_excel)

            if df.empty:
                continue

            if "score" in df.columns:
                df.sort_values(by="score", ascending=False, inplace=True)

            sheet_name = empresa[:31]
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            ws = writer.sheets[sheet_name]

            for col in range(1, len(df.columns) + 1):
                cell = ws.cell(row=1, column=col)
                cell.font = Font(bold=True, color="FFFFFF")
                cell.fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
                cell.alignment = Alignment(horizontal="center", vertical="center")

            ws.freeze_panes = "A2"

            resumo.append({
                "Empresa": empresa,
                "Total": len(df),
                "Alta Prioridade": len(df[df["prioridade_score"].str.contains("ALTA", na=False)]),
                "Media Prioridade": len(df[df["prioridade_score"].str.contains("MÉDIA", na=False)]),
                "Baixa Prioridade": len(df[df["prioridade_score"].str.contains("BAIXA", na=False)])
            })

        df_resumo = pd.DataFrame(resumo)
        df_resumo.to_excel(writer, sheet_name="Resumo", index=False)

    salvar_historico(novos_ids)

    total_geral = len(resultados)
    total_urgentes = len([r for r in resultados if r["urgencia_prazo"] == "🔴 URGENTE"])

    mensagem = f"""
<b>📊 RADAR PNCP ENTERPRISE</b>

🔎 Total oportunidades: <b>{total_geral}</b>
🔴 Urgentes (≤5 dias): <b>{total_urgentes}</b>

📅 Período analisado: últimos {DIAS_BUSCA} dias
"""

    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram não configurado.")
    else:
        enviar_telegram(nome_arquivo, mensagem)

