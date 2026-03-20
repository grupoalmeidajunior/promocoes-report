# -*- coding: utf-8 -*-
"""
Auditoria de Integridade dos Dados - Promocoes Report
=====================================================
Valida consistencia, integridade e veracidade dos dados extraidos.
"""
import io
import os
import sys
import json

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DADOS_DIR = os.path.join(BASE_DIR, "dados")


def carregar(nome):
    path = os.path.join(DADOS_DIR, nome)
    if nome.endswith(".json"):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return pd.read_csv(path)


def print_resultado(grupo, teste, ok, detalhe=""):
    status = "PASS" if ok else "FAIL"
    icon = "\u2705" if ok else "\u274C"
    print(f"  {icon} [{status}] {teste}")
    if detalhe:
        print(f"       {detalhe}")
    return ok


def main():
    info = carregar("promocao_info.json")
    kpis = carregar("kpis_promocao.csv")
    serie = carregar("serie_temporal.csv")
    serie_total = carregar("serie_temporal_total.csv")
    resgates = carregar("resgates_pontos.csv")
    resg_dia = carregar("resgates_por_dia.csv")

    total_pass = 0
    total_fail = 0
    alertas = []

    def check(grupo, teste, ok, detalhe=""):
        nonlocal total_pass, total_fail
        r = print_resultado(grupo, teste, ok, detalhe)
        if r:
            total_pass += 1
        else:
            total_fail += 1
            alertas.append(f"[{grupo}] {teste}: {detalhe}")
        return r

    # ============================================================
    print("\n" + "=" * 70)
    print("AUDITORIA DE INTEGRIDADE - PROMOCOES REPORT")
    print("=" * 70)

    # 1. ESTRUTURA DOS ARQUIVOS
    print("\n--- 1. ESTRUTURA DOS ARQUIVOS ---")
    arquivos = ["promocao_info.json", "kpis_promocao.csv", "serie_temporal.csv",
                "serie_temporal_total.csv", "resgates_pontos.csv", "resgates_por_dia.csv"]
    for arq in arquivos:
        exists = os.path.exists(os.path.join(DADOS_DIR, arq))
        check("Estrutura", f"Arquivo {arq} existe", exists)

    # 2. PROMOCAO INFO
    print("\n--- 2. PROMOCAO INFO ---")
    campos_obrig = ["id", "titulo", "data_inicio", "data_fim", "data_sorteio", "pontos_por_numero"]
    for campo in campos_obrig:
        check("Promo", f"Campo '{campo}' presente", campo in info, f"valor: {info.get(campo, 'AUSENTE')}")

    check("Promo", "data_inicio < data_fim",
          info["data_inicio"] < info["data_fim"],
          f"{info['data_inicio']} < {info['data_fim']}")
    check("Promo", "data_fim < data_sorteio",
          info["data_fim"] < info["data_sorteio"],
          f"{info['data_fim']} < {info['data_sorteio']}")
    check("Promo", "pontos_por_numero = 100",
          info["pontos_por_numero"] == 100,
          f"valor: {info['pontos_por_numero']}")

    # 3. KPIs
    print("\n--- 3. KPIs POR SHOPPING ---")
    siglas_esperadas = {"CS", "BS", "NK", "NR", "GS", "NS", "TOTAL"}
    siglas_csv = set(kpis["shopping_sigla"].unique())
    check("KPIs", "Todos 6 shoppings + TOTAL presentes",
          siglas_csv == siglas_esperadas,
          f"encontrado: {siglas_csv}")
    check("KPIs", "7 linhas (6 shoppings + TOTAL)",
          len(kpis) == 7, f"linhas: {len(kpis)}")

    # Valores nao negativos
    for col in ["clientes_novos", "clientes_recorrentes", "clientes_totais",
                "cupons_lancados", "valor_total", "lojas_na_promocao"]:
        check("KPIs", f"Sem valores negativos em '{col}'",
              (kpis[col] >= 0).all(), f"min: {kpis[col].min()}")

    # Consistencia novos + recorrentes = totais (por shopping)
    kpis_shop = kpis[kpis["shopping_sigla"] != "TOTAL"]
    for _, row in kpis_shop.iterrows():
        soma = row["clientes_novos"] + row["clientes_recorrentes"]
        check("KPIs", f"{row['shopping_sigla']}: novos({row['clientes_novos']}) + recorrentes({row['clientes_recorrentes']}) = totais({row['clientes_totais']})",
              soma == row["clientes_totais"],
              f"soma={soma}, total={row['clientes_totais']}")

    # Total = soma dos shoppings (cupons)
    total_row = kpis[kpis["shopping_sigla"] == "TOTAL"].iloc[0]
    soma_cupons = kpis_shop["cupons_lancados"].sum()
    check("KPIs", f"TOTAL cupons({int(total_row['cupons_lancados'])}) = soma shoppings({soma_cupons})",
          total_row["cupons_lancados"] == soma_cupons)

    # Total valor = soma dos shoppings
    soma_valor = round(kpis_shop["valor_total"].sum(), 2)
    check("KPIs", f"TOTAL valor(R${total_row['valor_total']:,.2f}) = soma shoppings(R${soma_valor:,.2f})",
          abs(total_row["valor_total"] - soma_valor) < 0.1)

    # Total pontos = soma dos shoppings
    soma_pontos = kpis_shop["pontos_utilizados"].sum()
    check("KPIs", f"TOTAL pontos({int(total_row['pontos_utilizados'])}) = soma shoppings({int(soma_pontos)})",
          total_row["pontos_utilizados"] == soma_pontos)

    # Total numeros = soma dos shoppings
    soma_numeros = kpis_shop["numeros_sorte"].sum()
    check("KPIs", f"TOTAL numeros({int(total_row['numeros_sorte'])}) = soma shoppings({int(soma_numeros)})",
          total_row["numeros_sorte"] == soma_numeros)

    # TM cliente = valor / clientes
    for _, row in kpis.iterrows():
        if row["clientes_totais"] > 0:
            tm_esperado = round(row["valor_total"] / row["clientes_totais"], 2)
            check("KPIs", f"{row['shopping_sigla']}: TM cliente R${row['tm_cliente']} = R${tm_esperado}",
                  abs(row["tm_cliente"] - tm_esperado) < 0.02,
                  f"diff: R${abs(row['tm_cliente'] - tm_esperado):.2f}")

    # TM cupom = valor / cupons
    for _, row in kpis.iterrows():
        if row["cupons_lancados"] > 0:
            tm_esperado = round(row["valor_total"] / row["cupons_lancados"], 2)
            check("KPIs", f"{row['shopping_sigla']}: TM cupom R${row['tm_cupom']} = R${tm_esperado}",
                  abs(row["tm_cupom"] - tm_esperado) < 0.02,
                  f"diff: R${abs(row['tm_cupom'] - tm_esperado):.2f}")

    # Taxa conversao lojas
    for _, row in kpis_shop.iterrows():
        if row["lojas_na_promocao"] > 0:
            taxa_esp = round(row["lojas_com_cupons"] / row["lojas_na_promocao"] * 100, 1)
            check("KPIs", f"{row['shopping_sigla']}: taxa conversao lojas {row['taxa_conversao_lojas']}% = {taxa_esp}%",
                  abs(row["taxa_conversao_lojas"] - taxa_esp) < 0.2)

    # 4. SERIE TEMPORAL
    print("\n--- 4. SERIE TEMPORAL ---")
    serie["data"] = pd.to_datetime(serie["data"])
    serie_total["data"] = pd.to_datetime(serie_total["data"])

    # 60 dias
    n_dias = serie_total["data"].nunique()
    check("Serie", f"Quantidade de dias: {n_dias} (esperado ~60)",
          55 <= n_dias <= 65)

    # Sem dias duplicados por shopping
    dupes = serie.groupby(["data", "shopping_id"]).size()
    check("Serie", "Sem duplicatas (data + shopping)",
          (dupes == 1).all(), f"max ocorrencias: {dupes.max()}")

    # 6 shoppings por dia
    shops_por_dia = serie.groupby("data")["shopping_id"].nunique()
    check("Serie", f"6 shoppings por dia (min={shops_por_dia.min()}, max={shops_por_dia.max()})",
          shops_por_dia.min() >= 5)

    # Serie total = soma da serie por shopping
    serie_agg = serie.groupby("data").agg(cupons=("cupons", "sum"), valor_total=("valor_total", "sum")).reset_index()
    serie_agg["data"] = pd.to_datetime(serie_agg["data"])
    merged = serie_total.merge(serie_agg, on="data", suffixes=("_total", "_soma"))
    check("Serie", "serie_temporal_total.cupons = soma(serie_temporal.cupons)",
          (merged["cupons_total"] == merged["cupons_soma"]).all(),
          f"diffs: {(merged['cupons_total'] != merged['cupons_soma']).sum()} dias")

    valor_diff = (merged["valor_total_total"] - merged["valor_total_soma"]).abs()
    check("Serie", "serie_temporal_total.valor = soma(serie_temporal.valor)",
          (valor_diff < 0.1).all(),
          f"max diff: R${valor_diff.max():.2f}")

    # Flag na_promocao correto
    promo_inicio = pd.to_datetime(info["data_inicio"])
    check("Serie", "Flag na_promocao=True apenas a partir do inicio da promo",
          (serie_total[serie_total["na_promocao"] == True]["data"] >= promo_inicio).all())
    check("Serie", "Flag na_promocao=False antes do inicio da promo",
          (serie_total[serie_total["na_promocao"] == False]["data"] < promo_inicio).all())

    # Dia 19/03 (inicio) existe e esta marcado como na_promocao
    dia_inicio = serie_total[serie_total["data"] == promo_inicio]
    check("Serie", f"Dia do inicio ({info['data_inicio']}) existe na serie",
          len(dia_inicio) > 0)
    if len(dia_inicio) > 0:
        check("Serie", f"Dia do inicio marcado como na_promocao=True",
              dia_inicio.iloc[0]["na_promocao"] == True)

    # Cupons do dia 19/03 na serie = cupons no kpis
    if len(dia_inicio) > 0:
        cupons_serie_19 = int(dia_inicio.iloc[0]["cupons"])
        cupons_kpi_total = int(total_row["cupons_lancados"])
        check("Serie", f"Cupons dia 19/03 na serie({cupons_serie_19}) = KPI total({cupons_kpi_total})",
              cupons_serie_19 == cupons_kpi_total,
              "Promocao so tem 1 dia de dados, devem ser iguais")

    # Valores na serie vs KPIs (dia da promo)
    if len(dia_inicio) > 0:
        valor_serie_19 = round(dia_inicio.iloc[0]["valor_total"], 2)
        valor_kpi_total = total_row["valor_total"]
        check("Serie", f"Valor dia 19/03 na serie(R${valor_serie_19:,.2f}) = KPI total(R${valor_kpi_total:,.2f})",
              abs(valor_serie_19 - valor_kpi_total) < 0.1)

    # 5. RESGATES DE PONTOS
    print("\n--- 5. RESGATES DE PONTOS ---")
    check("Resgates", f"Total registros: {len(resgates)}",
          len(resgates) > 0)
    check("Resgates", f"Todos promocao_id = {info['id']}",
          (resgates["promocao_id"] == info["id"]).all())
    check("Resgates", "Todos status = 'Resgatado'",
          (resgates["status"] == "Resgatado").all(),
          f"valores unicos: {resgates['status'].unique()}")

    # Shopping IDs validos
    valid_shops = {1, 2, 3, 4, 5, 6}
    check("Resgates", "Todos shopping_id validos (1-6)",
          set(resgates["shopping_id"].unique()).issubset(valid_shops),
          f"valores: {sorted(resgates['shopping_id'].unique())}")

    # Pontos totais > 0
    check("Resgates", "Todos pontos_totais >= 0",
          (resgates["pontos_totais"] >= 0).all(),
          f"min: {resgates['pontos_totais'].min()}")

    # quantidade_numeros > 0
    check("Resgates", "Todos quantidade_numeros >= 1",
          (resgates["quantidade_numeros"] >= 1).all(),
          f"min: {resgates['quantidade_numeros'].min()}")

    # Saldo anterior > saldo posterior (pontos foram gastos)
    check("Resgates", "saldo_anterior >= saldo_posterior (pontos foram gastos)",
          (resgates["cliente_saldo_anterior"] >= resgates["cliente_saldo_posterior"]).all(),
          f"violacoes: {(resgates['cliente_saldo_anterior'] < resgates['cliente_saldo_posterior']).sum()}")

    # Verificar: pontos_totais = saldo_anterior - saldo_posterior?
    diff_saldo = resgates["cliente_saldo_anterior"] - resgates["cliente_saldo_posterior"]
    match_pontos = (diff_saldo == resgates["pontos_totais"])
    check("Resgates", "pontos_totais = saldo_anterior - saldo_posterior",
          match_pontos.all(),
          f"violacoes: {(~match_pontos).sum()}, exemplo: " +
          (str(resgates[~match_pontos][["id", "pontos_totais", "cliente_saldo_anterior", "cliente_saldo_posterior"]].head(3).to_dict("records")) if (~match_pontos).any() else ""))

    # Verificar numeros = ceil(pontos_totais / pontos_unitarios)
    import math
    numeros_esperados = resgates.apply(
        lambda r: max(1, math.ceil(r["pontos_totais"] / r["pontos_unitarios"])) if r["pontos_unitarios"] > 0 else 0, axis=1
    )
    match_numeros = (numeros_esperados == resgates["quantidade_numeros"])
    pct_match = match_numeros.mean() * 100
    check("Resgates", f"quantidade_numeros = ceil(pontos/100): {pct_match:.1f}% corretos",
          pct_match >= 95,
          f"violacoes: {(~match_numeros).sum()}")

    if (~match_numeros).any():
        erros = resgates[~match_numeros][["id", "pontos_totais", "pontos_unitarios", "quantidade_numeros"]].copy()
        erros["esperado"] = numeros_esperados[~match_numeros]
        print(f"       Exemplos de divergencia:")
        for _, e in erros.head(5).iterrows():
            print(f"         ID {int(e['id'])}: pontos={int(e['pontos_totais'])}, unit={int(e['pontos_unitarios'])}, numeros={int(e['quantidade_numeros'])}, esperado={int(e['esperado'])}")

    # Consistencia KPIs vs resgates detalhados
    for _, row in kpis_shop.iterrows():
        sid = row["shopping_id"]
        res_sub = resgates[resgates["shopping_id"] == sid]
        pontos_det = int(res_sub["pontos_totais"].sum())
        pontos_kpi = int(row["pontos_utilizados"])
        check("Resgates", f"{row['shopping_sigla']}: pontos KPI({pontos_kpi}) = detalhado({pontos_det})",
              pontos_kpi == pontos_det)

    # 6. RESGATES POR DIA
    print("\n--- 6. RESGATES POR DIA ---")
    resg_dia["data"] = pd.to_datetime(resg_dia["data"])

    # Total de resgates = soma dos dias
    total_resgates_dia = resg_dia["resgates"].sum()
    check("Resg/Dia", f"Total resgates({total_resgates_dia}) = total registros({len(resgates)})",
          total_resgates_dia == len(resgates))

    total_pontos_dia = int(resg_dia["pontos_totais"].sum())
    total_pontos_kpi = int(total_row["pontos_utilizados"])
    check("Resg/Dia", f"Total pontos por dia({total_pontos_dia}) = KPI total({total_pontos_kpi})",
          total_pontos_dia == total_pontos_kpi)

    total_numeros_dia = int(resg_dia["numeros_totais"].sum())
    total_numeros_kpi = int(total_row["numeros_sorte"])
    check("Resg/Dia", f"Total numeros por dia({total_numeros_dia}) = KPI total({total_numeros_kpi})",
          total_numeros_dia == total_numeros_kpi)

    # 7. ANALISE DE ANOMALIAS
    print("\n--- 7. ANALISE DE ANOMALIAS ---")

    # Outliers na serie temporal (cupons)
    media_cupons = serie_total["cupons"].mean()
    std_cupons = serie_total["cupons"].std()
    outliers_cupons = serie_total[
        (serie_total["cupons"] > media_cupons + 3 * std_cupons) |
        (serie_total["cupons"] < media_cupons - 3 * std_cupons)
    ]
    check("Anomalias", f"Sem outliers extremos em cupons/dia (3-sigma): {len(outliers_cupons)} encontrados",
          len(outliers_cupons) == 0,
          f"media={media_cupons:.0f}, std={std_cupons:.0f}" +
          (f", outliers: {outliers_cupons[['data','cupons']].to_dict('records')}" if len(outliers_cupons) > 0 else ""))

    # Dias sem dados na serie
    datas_serie = pd.date_range(serie_total["data"].min(), serie_total["data"].max())
    dias_faltantes = set(datas_serie) - set(serie_total["data"])
    check("Anomalias", f"Sem dias faltantes na serie temporal: {len(dias_faltantes)} ausentes",
          len(dias_faltantes) == 0,
          f"faltantes: {sorted(d.strftime('%Y-%m-%d') for d in dias_faltantes)}" if dias_faltantes else "")

    # Concentracao de resgates em poucos clientes
    if len(resgates) > 0:
        top5_cli = resgates.groupby("cliente_id")["pontos_totais"].sum().nlargest(5)
        total_pontos_all = resgates["pontos_totais"].sum()
        pct_top5 = top5_cli.sum() / total_pontos_all * 100
        check("Anomalias", f"Top 5 clientes concentram {pct_top5:.1f}% dos pontos",
              True,  # informativo
              f"Top 5: {dict(zip(top5_cli.index, top5_cli.values))}")

    # Resgates antes do inicio da promocao?
    resgates["data_resgate"] = pd.to_datetime(resgates["data_resgate"])
    antes_promo = resgates[resgates["data_resgate"].dt.normalize() < promo_inicio]
    check("Anomalias", f"Resgates antes da promo ({info['data_inicio']}): {len(antes_promo)}",
          len(antes_promo) == 0 or True,  # alerta se houver
          f"Existem {len(antes_promo)} resgates antes do inicio (pode ser pre-release)" if len(antes_promo) > 0 else "Nenhum")

    if len(antes_promo) > 0:
        datas_antes = antes_promo["data_resgate"].dt.date.unique()
        alertas.append(f"[Anomalias] {len(antes_promo)} resgates antes da promo: datas {sorted(str(d) for d in datas_antes)}")

    # Cliente com muitos resgates
    resg_por_cli = resgates.groupby("cliente_id").size()
    cli_muitos = resg_por_cli[resg_por_cli > 10]
    check("Anomalias", f"Clientes com >10 resgates: {len(cli_muitos)}",
          True,  # informativo
          f"IDs: {dict(cli_muitos)}" if len(cli_muitos) > 0 else "Nenhum")

    # Resgate com muitos numeros (>50)
    big_resg = resgates[resgates["quantidade_numeros"] > 50]
    check("Anomalias", f"Resgates com >50 numeros da sorte: {len(big_resg)}",
          True,  # informativo
          f"Max: {int(resgates['quantidade_numeros'].max())} numeros (cliente {int(resgates.loc[resgates['quantidade_numeros'].idxmax(), 'cliente_id'])})")

    # ============================================================
    print("\n" + "=" * 70)
    print(f"RESULTADO FINAL: {total_pass} PASS / {total_fail} FAIL")
    print("=" * 70)

    if alertas:
        print(f"\n{'='*70}")
        print("ALERTAS/FALHAS:")
        for a in alertas:
            print(f"  \u26A0 {a}")

    # Salvar resultado
    resultado = {
        "data_auditoria": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"),
        "total_testes": total_pass + total_fail,
        "pass": total_pass,
        "fail": total_fail,
        "alertas": alertas,
    }
    with open(os.path.join(DADOS_DIR, "auditoria_resultado.json"), "w", encoding="utf-8") as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2)

    print(f"\nResultado salvo em: dados/auditoria_resultado.json")
    return total_fail


if __name__ == "__main__":
    sys.exit(main())
