"""
============================================================
PIPELINE: ERP Excel → SQL Server
============================================================
Como usar:
  1. Coloque os arquivos baixados do ERP na pasta definida em PASTA_ARQUIVOS
  2. Execute:  python 02_carga_erp.py
  3. Abra o Power BI e clique em Atualizar

Dependências:
  pip install pandas openpyxl pyodbc sqlalchemy
============================================================
"""

import os
import glob
import logging

import pandas as pd
from sqlalchemy import create_engine, text

# ─────────────────────────────────────────────────────────────
# CONFIGURAÇÕES
# ─────────────────────────────────────────────────────────────

# Pasta onde ficam os arquivos baixados do ERP
PASTA_ARQUIVOS = r"C:\Users\rhfec\Downloads\Automação Python\PASTA_ARQUIVOS"

# Conexão com o SQL Server
CONN_STRING = (
    "mssql+pyodbc:///?odbc_connect="
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=.\\SQLEXPRESS;"
    "DATABASE=ERP_Franquias;"
    "Trusted_Connection=yes;"
)

# ─────────────────────────────────────────────────────────────
# LOG
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("carga_erp.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def conectar():
    engine = create_engine(CONN_STRING, fast_executemany=True)
    log.info("Conexão com SQL Server estabelecida.")
    return engine


def converter_data_excel(valor):
    if pd.isna(valor):
        return None
    if isinstance(valor, (int, float)):
        return pd.Timestamp("1899-12-30") + pd.Timedelta(days=int(valor))
    try:
        return pd.to_datetime(valor, dayfirst=True)
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────
# LEITURA DO ARQUIVO DE VENDAS DIÁRIAS
# ─────────────────────────────────────────────────────────────

def ler_vendas(caminho: str) -> pd.DataFrame:
    log.info(f"Lendo vendas: {caminho}")
    df_raw = pd.read_excel(caminho, sheet_name="Relatório", header=None)

    linha_header = None
    for i, row in df_raw.iterrows():
        if any(str(c).strip().upper() == "ID LOJA" for c in row):
            linha_header = i
            break

    if linha_header is None:
        raise ValueError(f"Cabeçalho 'ID LOJA' não encontrado em {caminho}")

    df = pd.read_excel(caminho, sheet_name="Relatório", header=linha_header)
    df.columns = [str(c).strip() for c in df.columns]
    df.dropna(how="all", inplace=True)

    mapa_colunas = {
        "ID LOJA":                "id_loja",
        "Loja":                   "loja",
        "Data":                   "data",
        "Tickets":                "tickets",
        "Total Bruto":            "total_bruto",
        "Total Líquido":          "total_liquido",
        "Ticket Médio":           "ticket_medio",
        "Total Faturado":         "total_faturado",
        "Faturado Interno":       "fat_interno",
        "Faturado Delivery":      "fat_delivery",
        "Total Desconto":         "total_desconto",
        "Total Gorjeta":          "total_gorjeta",
        "Total Vendas da Loja":   "total_vendas",
        "Total Ticket Cancelado": "ticket_cancelado",
        "Total Pedido Cancelado": "pedido_cancelado",
    }
    df.rename(columns=mapa_colunas, inplace=True)

    colunas = list(mapa_colunas.values())
    df = df[[c for c in colunas if c in df.columns]].copy()

    df["data"] = df["data"].apply(converter_data_excel)
    df["data"] = pd.to_datetime(df["data"], errors="coerce").dt.date
    df["id_loja"] = df["id_loja"].astype(str).str.strip()
    df.dropna(subset=["data"], inplace=True)

    log.info(f"  → {len(df)} linha(s) de vendas lida(s).")
    return df


# ─────────────────────────────────────────────────────────────
# LEITURA DO ARQUIVO DE PRODUTOS (PLU)
# ─────────────────────────────────────────────────────────────

def ler_produtos(caminho: str, data_ref=None) -> pd.DataFrame:
    log.info(f"Lendo produtos: {caminho}")

    # Extrai a data da aba 'Dados de Origem'
    if data_ref is None:
        try:
            df_origem = pd.read_excel(
                caminho, sheet_name="Dados de Origem", header=None
            )
            for _, row in df_origem.iterrows():
                vals = [str(v) for v in row if pd.notna(v)]
                for v in vals:
                    if "até" in v.lower() or "/" in v:
                        parte = v.split("até")[0].strip()
                        try:
                            data_ref = pd.to_datetime(parte, dayfirst=True).date()
                            break
                        except Exception:
                            pass
                if data_ref:
                    break
        except Exception:
            pass

    if data_ref is None:
        raise ValueError(
            f"Não foi possível determinar a data do arquivo {caminho}. "
            "Verifique a aba 'Dados de Origem'."
        )

    df_raw = pd.read_excel(caminho, sheet_name="Relatório", header=None)

    linha_header = None
    for i, row in df_raw.iterrows():
        if any(str(c).strip().upper() == "PLU" for c in row):
            linha_header = i
            break

    if linha_header is None:
        raise ValueError(f"Cabeçalho 'PLU' não encontrado em {caminho}")

    df = pd.read_excel(caminho, sheet_name="Relatório", header=linha_header)
    df.dropna(how="all", inplace=True)

    # ── Renomeia colunas usando POSIÇÃO (evita problema com nomes duplicados) ──
    # Estrutura esperada:
    # 0=PLU, 1=Categoria, 2=Nome, 3=Qtd%, 4=Líquido%, 5=Loja, 6=Qtd, 7=Valor total, 8=Desconto, 9=Impostos, 10=Líquido
    nomes_posicao = {
        0: "plu",
        1: "categoria",
        2: "nome",
        3: "qtd_percentual",
        4: "liq_percentual",
        5: "loja",
        6: "quantidade",
        7: "valor_total",
        8: "desconto",
        9: "impostos",
        10: "liquido",
    }
    colunas_atuais = list(df.columns)
    novos_nomes = []
    for i, c in enumerate(colunas_atuais):
        if i in nomes_posicao:
            novos_nomes.append(nomes_posicao[i])
        else:
            novos_nomes.append(str(c))
    df.columns = novos_nomes

    # Mantém apenas as colunas que nos interessam
    colunas_finais = list(nomes_posicao.values())
    df = df[[c for c in colunas_finais if c in df.columns]].copy()

    # Extrai id_loja do campo loja (ex: "50033265 - Franquia PH FCD SÃO LUÍS")
    df["id_loja"] = (
        df["loja"].astype(str)
        .str.extract(r"^(\d+)")[0]
        .str.strip()
    )

    df["data"] = data_ref

    # Remove linhas sem PLU válido
    df = df[pd.to_numeric(df["plu"], errors="coerce").notna()].copy()
    df["plu"] = df["plu"].astype(int)

    log.info(f"  → {len(df)} linha(s) de produtos lida(s)  |  data: {data_ref}")
    return df


# ─────────────────────────────────────────────────────────────
# CARGA NO BANCO — INSERT apenas do que ainda não existe
# ─────────────────────────────────────────────────────────────

def carregar_vendas(engine, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    with engine.connect() as conn:
        resultado = conn.execute(
            text("SELECT id_loja, CAST(data AS VARCHAR) FROM vendas_diarias")
        ).fetchall()
        existentes = {(str(r[0]), str(r[1])) for r in resultado}

    novas = df[
        ~df.apply(
            lambda r: (str(r["id_loja"]), str(r["data"])) in existentes,
            axis=1,
        )
    ].copy()

    if novas.empty:
        log.info("  Vendas: nenhuma linha nova para inserir.")
        return 0

    novas.to_sql("vendas_diarias", engine, if_exists="append", index=False, method="multi")
    log.info(f"  Vendas: {len(novas)} nova(s) linha(s) inserida(s).")
    return len(novas)


def carregar_produtos(engine, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    with engine.connect() as conn:
        resultado = conn.execute(
            text("SELECT id_loja, CAST(data AS VARCHAR), plu FROM vendas_produtos")
        ).fetchall()
        existentes = {(str(r[0]), str(r[1]), int(r[2])) for r in resultado}

    novas = df[
        ~df.apply(
            lambda r: (str(r["id_loja"]), str(r["data"]), int(r["plu"])) in existentes,
            axis=1,
        )
    ].copy()

    if novas.empty:
        log.info("  Produtos: nenhuma linha nova para inserir.")
        return 0

    novas.to_sql("vendas_produtos", engine, if_exists="append", index=False, method="multi")
    log.info(f"  Produtos: {len(novas)} nova(s) linha(s) inserida(s).")
    return len(novas)


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("INÍCIO DA CARGA ERP → SQL SERVER")
    log.info("=" * 60)

    engine = conectar()

    arquivos = glob.glob(os.path.join(PASTA_ARQUIVOS, "*.xlsx"))
    if not arquivos:
        log.warning(f"Nenhum arquivo .xlsx encontrado em: {PASTA_ARQUIVOS}")
        return

    total_vendas = 0
    total_produtos = 0
    erros = []

    for arquivo in sorted(arquivos):
        nome = os.path.basename(arquivo).lower()
        try:
            if "venda" in nome:
                df = ler_vendas(arquivo)
                total_vendas += carregar_vendas(engine, df)

            elif "produto" in nome or "plu" in nome:
                df = ler_produtos(arquivo)
                total_produtos += carregar_produtos(engine, df)

            else:
                log.warning(f"Arquivo ignorado (nome não reconhecido): {nome}")

        except Exception as e:
            log.error(f"ERRO ao processar {arquivo}: {e}")
            erros.append((arquivo, str(e)))

    log.info("=" * 60)
    log.info("CARGA CONCLUÍDA")
    log.info(f"  Linhas de vendas inseridas  : {total_vendas}")
    log.info(f"  Linhas de produtos inseridas: {total_produtos}")
    if erros:
        log.warning(f"  Arquivos com erro: {len(erros)}")
        for arq, msg in erros:
            log.warning(f"    {arq}: {msg}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()