# -*- coding: utf-8 -*-
"""
Extrator de Dados - Dashboard Promocoes Report
===============================================
Extrai dados de cupons, clientes e resgates do Snowflake
para o dashboard de acompanhamento de promocoes.
"""
import os
import sys
import json
import configparser
from datetime import datetime, date, timedelta

import pandas as pd
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


SHOPPING_NAMES = {1: "CS", 2: "BS", 3: "NK", 4: "NR", 5: "GS", 6: "NS"}
SHOPPING_FULL = {
    1: "Continente Shopping", 2: "Balneario Shopping", 3: "Neumarkt Shopping",
    4: "Norte Shopping", 5: "Garten Shopping", 6: "Nacoes Shopping",
}


def carregar_chave_privada(key_path):
    with open(key_path, "rb") as f:
        p_key = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def conectar_snowflake(config):
    pkb = carregar_chave_privada(config.get("snowflake", "private_key_path"))
    params = {
        "account": config.get("snowflake", "account"),
        "user": config.get("snowflake", "user"),
        "warehouse": config.get("snowflake", "warehouse"),
        "database": config.get("snowflake", "database"),
        "schema": "BRONZE",
        "private_key": pkb,
    }
    role = config.get("snowflake", "role", fallback=None)
    if role:
        params["role"] = role
    print(f"[INFO] Conectando ao Snowflake ({params['database']})...")
    conn = snowflake.connector.connect(**params)
    print("[OK] Conectado")
    return conn


def query_to_df(cursor, sql, desc=None):
    if desc:
        print(f"[INFO] {desc}...")
    cursor.execute(sql)
    cols = [c[0].lower() for c in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=cols)
    if desc:
        print(f"  -> {len(df):,} registros")
    return df


def main():
    print("=" * 60)
    print("EXTRATOR - DASHBOARD PROMOCOES REPORT")
    print("=" * 60)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(script_dir)
    dados_dir = os.path.join(base_dir, "dados")
    os.makedirs(dados_dir, exist_ok=True)

    config = configparser.ConfigParser()
    config.read(os.path.join(script_dir, "config_snowflake.ini"))

    conn = conectar_snowflake(config)
    cur = conn.cursor()

    try:
        # ============================================================
        # 1. Info da promocao ativa
        # ============================================================
        df_promo = query_to_df(cur, """
            SELECT ID, TITULO, STATUS, DATA_INICIO, DATA_FIM, DATA_SORTEIO,
                   PONTOS_NECESSARIOS
            FROM BRONZE.BRZ_AJFANS_PROMOCAO
            WHERE STATUS = 'ATIVO'
            ORDER BY DATA_INICIO DESC
            LIMIT 1
        """, "Buscando promocao ativa")

        if len(df_promo) == 0:
            print("[ERRO] Nenhuma promocao ativa encontrada!")
            sys.exit(1)

        promo = df_promo.iloc[0]
        promo_id = int(promo["id"])
        promo_titulo = promo["titulo"]
        promo_inicio = pd.to_datetime(promo["data_inicio"]).tz_localize(None).normalize()
        promo_fim = pd.to_datetime(promo["data_fim"]).tz_localize(None).normalize()
        promo_sorteio = pd.to_datetime(promo["data_sorteio"]).tz_localize(None).normalize()
        pontos_necessarios = int(promo["pontos_necessarios"])

        print(f"  Promocao: {promo_titulo} (ID={promo_id})")
        print(f"  Periodo: {promo_inicio.date()} a {promo_fim.date()}")
        print(f"  Sorteio: {promo_sorteio.date()}")
        print(f"  Pontos por numero: {pontos_necessarios}")

        # Salvar info da promo
        promo_info = {
            "id": promo_id,
            "titulo": promo_titulo,
            "data_inicio": str(promo_inicio.date()),
            "data_fim": str(promo_fim.date()),
            "data_sorteio": str(promo_sorteio.date()),
            "pontos_por_numero": pontos_necessarios,
            "atualizado_em": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "dados_ate": str((date.today() - timedelta(days=1))),
        }
        with open(os.path.join(dados_dir, "promocao_info.json"), "w", encoding="utf-8") as f:
            json.dump(promo_info, f, ensure_ascii=False, indent=2)

        # ============================================================
        # 2. Cupons no periodo da promocao (dados ate ontem)
        # ============================================================
        data_ate = str(date.today() - timedelta(days=1))

        df_cupons = query_to_df(cur, f"""
            SELECT
                fc.id AS cupom_id,
                fc.cliente_id,
                fc.shopping_id,
                s.nome AS shopping_nome,
                fc.cnpj_loja,
                sl.nome AS loja_nome,
                sl.segmento AS segmento_loja,
                fc.valor_compra AS valor,
                fc.data_envio,
                fc.status
            FROM BRONZE.BRZ_AJFANS_FIDELIDADE_CUPOM fc
            LEFT JOIN BRONZE.BRZ_AJFANS_SHOPPING s ON s.id = fc.shopping_id
            INNER JOIN (
                SELECT cnpj, MAX(nome) AS nome, MAX(segmento) AS segmento
                FROM BRONZE.BRZ_AJFANS_SHOPPING_LOJA
                WHERE cnpj IS NOT NULL AND cnpj <> ''
                GROUP BY cnpj
            ) sl ON sl.cnpj = fc.cnpj_loja
            WHERE fc.data_envio >= '{promo_inicio.date()}'
              AND fc.data_envio <= '{data_ate} 23:59:59'
              AND fc.status = 'Validado'
            ORDER BY fc.data_envio
        """, f"Extraindo cupons da promocao ({promo_inicio.date()} a {data_ate})")

        df_cupons["data_envio"] = pd.to_datetime(df_cupons["data_envio"]).dt.tz_localize(None)
        df_cupons["valor"] = pd.to_numeric(df_cupons["valor"], errors="coerce").fillna(0)

        # ============================================================
        # 3. Primeiro cupom de cada cliente (para novo vs recorrente)
        # ============================================================
        df_primeiro = query_to_df(cur, """
            SELECT cliente_id, MIN(data_envio) AS primeiro_cupom
            FROM BRONZE.BRZ_AJFANS_FIDELIDADE_CUPOM
            WHERE status = 'Validado'
            GROUP BY cliente_id
        """, "Buscando primeiro cupom de cada cliente")

        df_primeiro["primeiro_cupom"] = pd.to_datetime(df_primeiro["primeiro_cupom"]).dt.tz_localize(None)

        # ============================================================
        # 4. Lojas por shopping (total cadastradas)
        # ============================================================
        df_lojas = query_to_df(cur, """
            SELECT shopping_id, COUNT(DISTINCT cnpj) AS total_lojas
            FROM BRONZE.BRZ_AJFANS_SHOPPING_LOJA
            WHERE cnpj IS NOT NULL AND cnpj <> ''
            GROUP BY shopping_id
            ORDER BY shopping_id
        """, "Contando lojas por shopping")

        # ============================================================
        # 5. Resgates de pontos (numeros da sorte)
        # ============================================================
        df_resgates = query_to_df(cur, f"""
            SELECT
                id, cliente_id, promocao_id, shopping_id,
                pontos_totais, pontos_unitarios, quantidade_numeros,
                cliente_saldo_anterior, cliente_saldo_posterior,
                status, time AS data_resgate
            FROM BRONZE.BRZ_AJFANS_PROMOCAO_RESGATE
            WHERE promocao_id = {promo_id}
            ORDER BY time
        """, "Extraindo resgates de pontos/numeros da sorte")

        if len(df_resgates) > 0:
            df_resgates["data_resgate"] = pd.to_datetime(df_resgates["data_resgate"]).dt.tz_localize(None)

        # ============================================================
        # 6. Serie temporal (60 dias antes do fim da promo ou hoje)
        # ============================================================
        data_serie_inicio = (date.today() - timedelta(days=60)).strftime("%Y-%m-%d")

        df_serie = query_to_df(cur, f"""
            SELECT
                DATE(fc.data_envio) AS data,
                fc.shopping_id,
                COUNT(*) AS cupons,
                COUNT(DISTINCT fc.cliente_id) AS clientes,
                SUM(fc.valor_compra) AS valor_total
            FROM BRONZE.BRZ_AJFANS_FIDELIDADE_CUPOM fc
            WHERE fc.data_envio >= '{data_serie_inicio}'
              AND fc.data_envio <= '{data_ate} 23:59:59'
              AND fc.status = 'Validado'
            GROUP BY DATE(fc.data_envio), fc.shopping_id
            ORDER BY data, fc.shopping_id
        """, f"Extraindo serie temporal (60 dias: {data_serie_inicio} a {data_ate})")

        df_serie["data"] = pd.to_datetime(df_serie["data"])
        df_serie["valor_total"] = pd.to_numeric(df_serie["valor_total"], errors="coerce").fillna(0)

        # ============================================================
        # 7. Total cadastrados no app por shopping
        # ============================================================
        df_cadastrados = query_to_df(cur, """
            SELECT
                c.id AS cliente_id,
                c.status,
                c.data_cadastro
            FROM BRONZE.BRZ_AJFANS_CLIENTES c
            WHERE c.status = 'ATIVO'
        """, "Buscando total de cadastrados ativos no app")

        # --- Novos cadastros no app durante o periodo da promocao ---
        df_cadastrados["data_cadastro"] = pd.to_datetime(df_cadastrados["data_cadastro"])
        promo_inicio_date = promo_inicio.normalize()
        data_ate_date = pd.to_datetime(data_ate)
        novos_cadastros = df_cadastrados[
            (df_cadastrados["data_cadastro"] >= promo_inicio_date) &
            (df_cadastrados["data_cadastro"] <= data_ate_date)
        ]["cliente_id"].unique()
        print(f"  Novos cadastros no periodo: {len(novos_cadastros)}")

        # Atribuir shopping ao novo cadastro pelo ultimo acesso no app
        novos_cad_por_shopping = pd.Series(dtype=int)
        if len(novos_cadastros) > 0:
            ids_str = ",".join(str(int(i)) for i in novos_cadastros)
            df_acesso = query_to_df(cur, f"""
                SELECT cliente_id, shopping_id, time,
                       ROW_NUMBER() OVER (PARTITION BY cliente_id ORDER BY time DESC) AS rn
                FROM BRONZE.BRZ_AJFANS_LOG_ACESSO_APP
                WHERE cliente_id IN ({ids_str})
            """, "Buscando ultimo acesso dos novos cadastros")
            df_acesso = df_acesso[df_acesso["rn"] == 1]
            novos_cad_por_shopping = df_acesso.groupby("shopping_id")["cliente_id"].nunique()

    finally:
        conn.close()
        print("\n[OK] Conexao fechada")

    # ==============================================================
    # PROCESSAR E SALVAR DADOS
    # ==============================================================
    print("\n" + "=" * 60)
    print("PROCESSANDO DADOS")
    print("=" * 60)

    # --- Classificar clientes novos vs recorrentes (por cupom) ---
    clientes_promo = df_cupons[["cliente_id"]].drop_duplicates()
    clientes_promo = clientes_promo.merge(df_primeiro, on="cliente_id", how="left")
    clientes_promo["tipo"] = clientes_promo["primeiro_cupom"].apply(
        lambda x: "Novo" if x >= promo_inicio else "Recorrente"
    )

    # --- KPIs por shopping ---
    kpis = []
    for sid in sorted(SHOPPING_NAMES.keys()):
        sigla = SHOPPING_NAMES[sid]
        nome = SHOPPING_FULL[sid]
        sub = df_cupons[df_cupons["shopping_id"] == sid]
        cli_sub = clientes_promo[clientes_promo["cliente_id"].isin(sub["cliente_id"])]

        novos = cli_sub[cli_sub["tipo"] == "Novo"]["cliente_id"].nunique()
        recorrentes = cli_sub[cli_sub["tipo"] == "Recorrente"]["cliente_id"].nunique()
        novos_cadastro = int(novos_cad_por_shopping.get(sid, 0))
        clientes_total = sub["cliente_id"].nunique()
        cupons_total = len(sub)
        valor_total = sub["valor"].sum()
        tm_cliente = valor_total / clientes_total if clientes_total > 0 else 0
        tm_cupom = valor_total / cupons_total if cupons_total > 0 else 0

        # Lojas
        lojas_promo_row = df_lojas[df_lojas["shopping_id"] == sid]
        lojas_na_promo = int(lojas_promo_row["total_lojas"].iloc[0]) if len(lojas_promo_row) > 0 else 0
        lojas_com_cupom = sub["cnpj_loja"].nunique()
        taxa_conversao_lojas = lojas_com_cupom / lojas_na_promo * 100 if lojas_na_promo > 0 else 0

        # Resgates
        res_sub = df_resgates[df_resgates["shopping_id"] == sid] if len(df_resgates) > 0 else pd.DataFrame()
        pontos_usados = int(res_sub["pontos_totais"].sum()) if len(res_sub) > 0 else 0
        numeros_gerados = int(res_sub["quantidade_numeros"].sum()) if len(res_sub) > 0 else 0
        clientes_resgataram = res_sub["cliente_id"].nunique() if len(res_sub) > 0 else 0

        kpis.append({
            "shopping_id": sid,
            "shopping_sigla": sigla,
            "shopping_nome": nome,
            "clientes_novos_cadastro": novos_cadastro,
            "clientes_novos_cupom": novos,
            "clientes_recorrentes": recorrentes,
            "clientes_totais": clientes_total,
            "cupons_lancados": cupons_total,
            "valor_total": round(valor_total, 2),
            "tm_cliente": round(tm_cliente, 2),
            "tm_cupom": round(tm_cupom, 2),
            "lojas_na_promocao": lojas_na_promo,
            "lojas_com_cupons": lojas_com_cupom,
            "taxa_conversao_lojas": round(taxa_conversao_lojas, 1),
            "pontos_utilizados": pontos_usados,
            "numeros_sorte": numeros_gerados,
            "clientes_resgataram": clientes_resgataram,
        })

    # Linha TOTAL
    total_novos_cadastro = len(novos_cadastros)  # total real (sem duplicar por shopping)
    total_novos_cupom = sum(k["clientes_novos_cupom"] for k in kpis)
    total_recorrentes = sum(k["clientes_recorrentes"] for k in kpis)
    total_clientes = df_cupons["cliente_id"].nunique()
    total_cupons = len(df_cupons)
    total_valor = df_cupons["valor"].sum()
    total_lojas_promo = sum(k["lojas_na_promocao"] for k in kpis)
    total_lojas_cupom = sum(k["lojas_com_cupons"] for k in kpis)
    total_pontos = sum(k["pontos_utilizados"] for k in kpis)
    total_numeros = sum(k["numeros_sorte"] for k in kpis)
    total_cli_resgate = df_resgates["cliente_id"].nunique() if len(df_resgates) > 0 else 0

    kpis.append({
        "shopping_id": 0,
        "shopping_sigla": "TOTAL",
        "shopping_nome": "AJ (totais)",
        "clientes_novos_cadastro": total_novos_cadastro,
        "clientes_novos_cupom": total_novos_cupom,
        "clientes_recorrentes": total_recorrentes,
        "clientes_totais": total_clientes,
        "cupons_lancados": total_cupons,
        "valor_total": round(total_valor, 2),
        "tm_cliente": round(total_valor / total_clientes, 2) if total_clientes > 0 else 0,
        "tm_cupom": round(total_valor / total_cupons, 2) if total_cupons > 0 else 0,
        "lojas_na_promocao": total_lojas_promo,
        "lojas_com_cupons": total_lojas_cupom,
        "taxa_conversao_lojas": round(total_lojas_cupom / total_lojas_promo * 100, 1) if total_lojas_promo > 0 else 0,
        "pontos_utilizados": total_pontos,
        "numeros_sorte": total_numeros,
        "clientes_resgataram": total_cli_resgate,
    })

    df_kpis = pd.DataFrame(kpis)
    df_kpis.to_csv(os.path.join(dados_dir, "kpis_promocao.csv"), index=False, encoding="utf-8-sig")
    print(f"[OK] kpis_promocao.csv: {len(df_kpis)} linhas")

    # --- Serie temporal ---
    df_serie["shopping_sigla"] = df_serie["shopping_id"].map(SHOPPING_NAMES)
    df_serie["na_promocao"] = df_serie["data"] >= promo_inicio
    df_serie.to_csv(os.path.join(dados_dir, "serie_temporal.csv"), index=False, encoding="utf-8-sig")
    print(f"[OK] serie_temporal.csv: {len(df_serie)} linhas")

    # Serie temporal total (todos shoppings agregados)
    df_serie_total = df_serie.groupby("data").agg(
        cupons=("cupons", "sum"),
        clientes=("clientes", "sum"),
        valor_total=("valor_total", "sum"),
    ).reset_index()
    df_serie_total["na_promocao"] = df_serie_total["data"] >= promo_inicio
    df_serie_total.to_csv(os.path.join(dados_dir, "serie_temporal_total.csv"), index=False, encoding="utf-8-sig")
    print(f"[OK] serie_temporal_total.csv: {len(df_serie_total)} linhas")

    # --- Resgates detalhado ---
    if len(df_resgates) > 0:
        df_resgates["shopping_sigla"] = df_resgates["shopping_id"].map(SHOPPING_NAMES)
        df_resgates.to_csv(os.path.join(dados_dir, "resgates_pontos.csv"), index=False, encoding="utf-8-sig")
        print(f"[OK] resgates_pontos.csv: {len(df_resgates)} linhas")

        # Resgates por dia
        df_resg_dia = df_resgates.groupby(df_resgates["data_resgate"].dt.date).agg(
            resgates=("id", "count"),
            clientes_unicos=("cliente_id", "nunique"),
            pontos_totais=("pontos_totais", "sum"),
            numeros_totais=("quantidade_numeros", "sum"),
        ).reset_index()
        df_resg_dia.columns = ["data", "resgates", "clientes_unicos", "pontos_totais", "numeros_totais"]
        df_resg_dia.to_csv(os.path.join(dados_dir, "resgates_por_dia.csv"), index=False, encoding="utf-8-sig")
        print(f"[OK] resgates_por_dia.csv: {len(df_resg_dia)} linhas")

    # --- Resumo ---
    print("\n" + "=" * 60)
    print(f"RESUMO - {promo_titulo}")
    print(f"Periodo: {promo_inicio.date()} a {data_ate}")
    print("=" * 60)
    t = df_kpis[df_kpis["shopping_sigla"] == "TOTAL"].iloc[0]
    print(f"  Clientes: {int(t['clientes_totais']):,} ({int(t['clientes_novos_cupom']):,} novos cupom + {int(t['clientes_recorrentes']):,} recorrentes)")
    print(f"  Novos cadastros no app: {int(t['clientes_novos_cadastro']):,}")
    print(f"  Cupons: {int(t['cupons_lancados']):,}")
    print(f"  Valor: R$ {t['valor_total']:,.2f}")
    print(f"  TM Cliente: R$ {t['tm_cliente']:,.2f}")
    print(f"  TM Cupom: R$ {t['tm_cupom']:,.2f}")
    print(f"  Lojas: {int(t['lojas_com_cupons'])} com cupons / {int(t['lojas_na_promocao'])} total ({t['taxa_conversao_lojas']}%)")
    print(f"  Pontos utilizados: {int(t['pontos_utilizados']):,}")
    print(f"  Numeros da sorte: {int(t['numeros_sorte']):,}")
    print(f"  Clientes que resgataram: {int(t['clientes_resgataram']):,}")
    print(f"\nArquivos salvos em: {dados_dir}")


if __name__ == "__main__":
    main()
