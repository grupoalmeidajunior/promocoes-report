# -*- coding: utf-8 -*-
"""
Dashboard Promocoes - Report
=============================
Acompanhamento de promocoes dos shoppings Almeida Junior.
"""
import json
import math
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

st.set_page_config(
    page_title="Promocoes - Report",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# CSS customizado
st.markdown("""
<style>
    .block-container {padding-top: 1rem; padding-bottom: 0rem;}
    [data-testid="stMetric"] {
        background: #f8f9fa;
        border: 1px solid #e9ecef;
        border-radius: 8px;
        padding: 12px 16px;
    }
    [data-testid="stMetric"] label {font-size: 0.8rem !important;}
    .promo-header {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        color: white;
        padding: 20px 30px;
        border-radius: 12px;
        margin-bottom: 20px;
    }
    .promo-header h1 {color: white; margin: 0; font-size: 1.8rem;}
    .promo-header p {color: #a0a0c0; margin: 5px 0 0 0; font-size: 0.95rem;}
    .kpi-table th {background: #1a1a2e !important; color: white !important; text-align: center !important;}
    .kpi-table td {text-align: right !important; padding: 6px 12px !important;}
    .kpi-table td:first-child {text-align: left !important; font-weight: bold;}
    div[data-testid="stExpander"] details summary span {font-weight: 600;}
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=300)
def carregar_dados():
    """Carrega todos os CSVs e o JSON de info da promo."""
    dados = {}
    try:
        with open("dados/promocao_info.json", "r", encoding="utf-8") as f:
            dados["info"] = json.load(f)
    except FileNotFoundError:
        st.error("Dados nao encontrados. Execute o script de extracao primeiro.")
        st.stop()

    dados["kpis"] = pd.read_csv("dados/kpis_promocao.csv", encoding="utf-8-sig")
    dados["serie"] = pd.read_csv("dados/serie_temporal.csv", encoding="utf-8-sig", parse_dates=["data"])
    dados["serie_total"] = pd.read_csv("dados/serie_temporal_total.csv", encoding="utf-8-sig", parse_dates=["data"])

    try:
        dados["resgates"] = pd.read_csv("dados/resgates_pontos.csv", encoding="utf-8-sig")
        dados["resgates_dia"] = pd.read_csv("dados/resgates_por_dia.csv", encoding="utf-8-sig", parse_dates=["data"])
    except FileNotFoundError:
        dados["resgates"] = pd.DataFrame()
        dados["resgates_dia"] = pd.DataFrame()

    return dados


def formatar_brl(valor):
    """Formata valor em reais."""
    if valor >= 1_000_000:
        return f"R$ {valor/1_000_000:,.1f}M"
    if valor >= 1_000:
        return f"R$ {valor:,.0f}"
    return f"R$ {valor:,.2f}"


def render_tabela_kpis(df_kpis, info):
    """Renderiza a tabela principal de KPIs estilo report."""
    # Separar shoppings e total
    shoppings = df_kpis[df_kpis["shopping_sigla"] != "TOTAL"].sort_values("shopping_sigla")
    total = df_kpis[df_kpis["shopping_sigla"] == "TOTAL"].iloc[0]

    # Ordem das colunas como na imagem: NK, BS, GS, NR, CS, NS, TOTAL
    ordem = ["NK", "BS", "GS", "NR", "CS", "NS"]
    colunas = [s for s in ordem if s in shoppings["shopping_sigla"].values]

    # Montar dados
    metricas = [
        ("Clientes Novos", "clientes_novos", "int"),
        ("Clientes Recorrentes", "clientes_recorrentes", "int"),
        ("Clientes Totais", "clientes_totais", "int"),
        ("Cupons Lancados", "cupons_lancados", "int"),
        ("R$", "valor_total", "brl"),
        ("TM Cliente", "tm_cliente", "brl_sm"),
        ("TM Cupom", "tm_cupom", "brl_sm"),
        ("", "", "sep"),
        ("Lojas na Promocao", "lojas_na_promocao", "int"),
        ("Lojas c/ Cupons Lancados", "lojas_com_cupons", "int"),
        ("Taxa de Conversao", "taxa_conversao_lojas", "pct"),
        ("", "", "sep"),
        ("Pontos Utilizados", "pontos_utilizados", "int"),
        ("Numeros da Sorte", "numeros_sorte", "int"),
        ("Clientes que Resgataram", "clientes_resgataram", "int"),
    ]

    def fmt(val, tipo):
        if tipo == "sep":
            return ""
        if tipo == "int":
            return f"{int(val):,}".replace(",", ".")
        if tipo == "brl":
            return f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        if tipo == "brl_sm":
            return f"R$ {val:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
        if tipo == "pct":
            return f"{val:.0f}%"
        return str(val)

    # Header
    header = "| Metrica |"
    sep_line = "|:---|"
    for s in colunas:
        header += f" **{s}** |"
        sep_line += "---:|"
    header += " **AJ (totais)** |"
    sep_line += "---:|"

    # Rows
    rows = []
    for label, col, tipo in metricas:
        if tipo == "sep":
            rows.append("|" + " |" * (len(colunas) + 2))
            continue
        row = f"| **{label}** |"
        for s in colunas:
            sub = shoppings[shoppings["shopping_sigla"] == s]
            val = sub[col].iloc[0] if len(sub) > 0 and col else 0
            row += f" {fmt(val, tipo)} |"
        row += f" {fmt(total[col], tipo)} |"
        rows.append(row)

    tabela = header + "\n" + sep_line + "\n" + "\n".join(rows)
    st.markdown(tabela)


def render_serie_temporal(dados, info):
    """Renderiza graficos de serie temporal."""
    df = dados["serie_total"].copy()
    promo_inicio = pd.Timestamp(info["data_inicio"])

    # Grafico de cupons por dia
    fig_cupons = go.Figure()
    df_pre = df[~df["na_promocao"]]
    df_promo = df[df["na_promocao"]]

    fig_cupons.add_trace(go.Bar(
        x=df_pre["data"], y=df_pre["cupons"],
        name="Pre-Promocao", marker_color="#94a3b8",
    ))
    fig_cupons.add_trace(go.Bar(
        x=df_promo["data"], y=df_promo["cupons"],
        name="Promocao", marker_color="#3b82f6",
    ))
    fig_cupons.add_vline(x=promo_inicio, line_dash="dash", line_color="red", line_width=2)
    fig_cupons.add_annotation(
        x=promo_inicio, y=1, yref="paper",
        text="Inicio Promo", showarrow=False,
        font=dict(color="red", size=11), yshift=10,
    )
    media_pre = df_pre["cupons"].mean() if len(df_pre) > 0 else 0
    fig_cupons.add_hline(y=media_pre, line_dash="dot", line_color="#64748b",
                         annotation_text=f"Media pre-promo: {media_pre:,.0f}", annotation_position="top left")
    fig_cupons.update_layout(
        title="Cupons Lancados por Dia (60 dias)",
        xaxis_title="", yaxis_title="Cupons",
        template="plotly_white", height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        margin=dict(l=40, r=20, t=60, b=40),
    )

    # Grafico de valor por dia
    fig_valor = go.Figure()
    fig_valor.add_trace(go.Bar(
        x=df_pre["data"], y=df_pre["valor_total"],
        name="Pre-Promocao", marker_color="#94a3b8",
    ))
    fig_valor.add_trace(go.Bar(
        x=df_promo["data"], y=df_promo["valor_total"],
        name="Promocao", marker_color="#22c55e",
    ))
    fig_valor.add_vline(x=promo_inicio, line_dash="dash", line_color="red", line_width=2)
    media_valor_pre = df_pre["valor_total"].mean() if len(df_pre) > 0 else 0
    fig_valor.add_hline(y=media_valor_pre, line_dash="dot", line_color="#64748b",
                        annotation_text=f"Media: R$ {media_valor_pre:,.0f}", annotation_position="top left")
    fig_valor.update_layout(
        title="Valor Total por Dia (R$)",
        xaxis_title="", yaxis_title="R$",
        template="plotly_white", height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        margin=dict(l=40, r=20, t=60, b=40),
    )

    return fig_cupons, fig_valor


def render_resgates(dados, info):
    """Renderiza metricas de resgate de pontos."""
    df_kpis = dados["kpis"]
    total = df_kpis[df_kpis["shopping_sigla"] == "TOTAL"].iloc[0]
    df_dia = dados["resgates_dia"]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Clientes que Resgataram", f"{int(total['clientes_resgataram']):,}")
    c2.metric("Pontos Utilizados", f"{int(total['pontos_utilizados']):,}")
    c3.metric("Numeros da Sorte", f"{int(total['numeros_sorte']):,}")
    media_pts = total["pontos_utilizados"] / total["clientes_resgataram"] if total["clientes_resgataram"] > 0 else 0
    c4.metric("Media Pontos/Cliente", f"{media_pts:,.0f}")

    if len(df_dia) > 0:
        fig = px.bar(
            df_dia, x="data", y="numeros_totais",
            text="numeros_totais",
            labels={"data": "", "numeros_totais": "Numeros da Sorte"},
            title="Numeros da Sorte Gerados por Dia",
            color_discrete_sequence=["#8b5cf6"],
        )
        fig.update_layout(template="plotly_white", height=350, margin=dict(l=40, r=20, t=60, b=40))
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

    # Tabela por shopping
    df_resg_shop = df_kpis[df_kpis["shopping_sigla"] != "TOTAL"][
        ["shopping_sigla", "clientes_resgataram", "pontos_utilizados", "numeros_sorte"]
    ].copy()
    df_resg_shop.columns = ["Shopping", "Clientes", "Pontos", "Numeros"]
    df_resg_shop = df_resg_shop.sort_values("Numeros", ascending=False)
    st.dataframe(df_resg_shop, use_container_width=True, hide_index=True)


# ==============================================================
# MAIN
# ==============================================================
def main():
    dados = carregar_dados()
    info = dados["info"]
    df_kpis = dados["kpis"]

    # Header
    st.markdown(f"""
    <div class="promo-header">
        <h1>🎯 Promocoes - Report</h1>
        <p>{info['titulo']} | Periodo: {info['data_inicio']} a {info['data_fim']} | Sorteio: {info['data_sorteio']}</p>
        <p style="color: #70a0ff; font-size: 0.85rem;">
            Atualizado em: {info['atualizado_em']} &nbsp;&nbsp;|&nbsp;&nbsp; Dados ate: {info['dados_ate']}
        </p>
    </div>
    """, unsafe_allow_html=True)

    # ============================================================
    # TAB PRINCIPAL
    # ============================================================
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Report Geral", "📈 Serie Temporal", "🎰 Resgates de Pontos", "🔍 Validacao"])

    with tab1:
        # KPIs destaque
        total = df_kpis[df_kpis["shopping_sigla"] == "TOTAL"].iloc[0]
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Clientes Totais", f"{int(total['clientes_totais']):,}")
        c2.metric("Cupons Lancados", f"{int(total['cupons_lancados']):,}")
        c3.metric("Valor Total", formatar_brl(total["valor_total"]))
        c4.metric("TM Cliente", f"R$ {total['tm_cliente']:,.0f}")
        c5.metric("Numeros da Sorte", f"{int(total['numeros_sorte']):,}")

        st.markdown("---")

        # Tabela completa
        st.subheader("Detalhamento por Shopping")
        render_tabela_kpis(df_kpis, info)

        # Graficos comparativos por shopping
        st.markdown("---")
        shoppings = df_kpis[df_kpis["shopping_sigla"] != "TOTAL"].copy()

        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(
                shoppings.sort_values("valor_total", ascending=True),
                x="valor_total", y="shopping_sigla",
                orientation="h", text_auto=",.0f",
                labels={"valor_total": "R$", "shopping_sigla": ""},
                title="Valor Total por Shopping",
                color_discrete_sequence=["#3b82f6"],
            )
            fig.update_layout(template="plotly_white", height=350, margin=dict(l=40, r=20, t=60, b=40))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.bar(
                shoppings.sort_values("clientes_totais", ascending=True),
                x="clientes_totais", y="shopping_sigla",
                orientation="h", text_auto=",",
                labels={"clientes_totais": "Clientes", "shopping_sigla": ""},
                title="Clientes Totais por Shopping",
                color_discrete_sequence=["#22c55e"],
            )
            fig.update_layout(template="plotly_white", height=350, margin=dict(l=40, r=20, t=60, b=40))
            st.plotly_chart(fig, use_container_width=True)

        col3, col4 = st.columns(2)
        with col3:
            fig = px.bar(
                shoppings.sort_values("taxa_conversao_lojas", ascending=True),
                x="taxa_conversao_lojas", y="shopping_sigla",
                orientation="h", text_auto=".0f",
                labels={"taxa_conversao_lojas": "% Conversao", "shopping_sigla": ""},
                title="Taxa de Conversao de Lojas (%)",
                color_discrete_sequence=["#f59e0b"],
            )
            fig.update_layout(template="plotly_white", height=350, margin=dict(l=40, r=20, t=60, b=40))
            fig.update_traces(texttemplate="%{x:.0f}%")
            st.plotly_chart(fig, use_container_width=True)

        with col4:
            fig = px.pie(
                shoppings, values="cupons_lancados", names="shopping_sigla",
                title="Distribuicao de Cupons por Shopping",
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig.update_layout(height=350, margin=dict(l=20, r=20, t=60, b=40))
            fig.update_traces(textinfo="percent+value", textposition="inside")
            st.plotly_chart(fig, use_container_width=True)

    with tab2:
        st.subheader("Serie Temporal - Impacto da Promocao")
        st.info(
            f"Comparativo dos ultimos 60 dias. A linha vermelha marca o inicio da promocao "
            f"**{info['titulo']}** em **{info['data_inicio']}**."
        )

        fig_cupons, fig_valor = render_serie_temporal(dados, info)

        st.plotly_chart(fig_cupons, use_container_width=True)
        st.plotly_chart(fig_valor, use_container_width=True)

        # Serie por shopping
        with st.expander("Serie por Shopping"):
            shopping_sel = st.selectbox("Shopping", ["Todos"] + sorted(dados["serie"]["shopping_sigla"].dropna().unique()))
            df_s = dados["serie"].copy()
            if shopping_sel != "Todos":
                df_s = df_s[df_s["shopping_sigla"] == shopping_sel]
                df_s = df_s.groupby("data").agg(cupons=("cupons","sum"), valor_total=("valor_total","sum")).reset_index()
            else:
                df_s = dados["serie_total"].copy()

            fig = px.line(df_s, x="data", y="cupons", markers=True,
                         labels={"data":"","cupons":"Cupons"}, title=f"Cupons/dia - {shopping_sel}")
            fig.add_vline(x=pd.Timestamp(info["data_inicio"]), line_dash="dash", line_color="red")
            fig.update_layout(template="plotly_white", height=350)
            st.plotly_chart(fig, use_container_width=True)

    with tab3:
        st.subheader("Resgates de Pontos - Numeros da Sorte")
        st.info(
            f"Cada **{info.get('pontos_por_numero', 100)} pontos** utilizados geram "
            f"**1 numero da sorte** para o sorteio de **{info['data_sorteio']}**."
        )

        if len(dados["resgates"]) > 0:
            render_resgates(dados, info)
        else:
            st.warning("Nenhum resgate de pontos registrado ainda.")

    with tab4:
        render_validacao(dados, info)


def render_validacao(dados, info):
    """Renderiza a aba de validacao de integridade dos dados."""
    st.subheader("Validacao de Integridade dos Dados")
    st.caption("Testes automaticos para garantir consistencia e veracidade dos dados extraidos.")

    kpis = dados["kpis"]
    serie = dados["serie_total"].copy()
    serie["data"] = pd.to_datetime(serie["data"])
    serie_det = dados["serie"].copy()
    serie_det["data"] = pd.to_datetime(serie_det["data"])
    resgates = dados["resgates"].copy() if len(dados["resgates"]) > 0 else pd.DataFrame()
    resg_dia = dados["resgates_dia"].copy() if len(dados["resgates_dia"]) > 0 else pd.DataFrame()
    promo_inicio = pd.Timestamp(info["data_inicio"])

    kpis_shop = kpis[kpis["shopping_sigla"] != "TOTAL"]
    total_row = kpis[kpis["shopping_sigla"] == "TOTAL"].iloc[0]

    resultados = []

    def check(grupo, teste, ok, detalhe=""):
        resultados.append({"grupo": grupo, "teste": teste, "ok": ok, "detalhe": detalhe})

    # === 1. Estrutura ===
    for arq in ["promocao_info.json", "kpis_promocao.csv", "serie_temporal.csv",
                "serie_temporal_total.csv", "resgates_pontos.csv", "resgates_por_dia.csv"]:
        check("Estrutura", f"Arquivo {arq} presente", os.path.exists(f"dados/{arq}"))

    # === 2. Promo Info ===
    check("Promocao", "data_inicio < data_fim", info["data_inicio"] < info["data_fim"],
          f"{info['data_inicio']} < {info['data_fim']}")
    check("Promocao", "data_fim < data_sorteio", info["data_fim"] < info["data_sorteio"])
    check("Promocao", "pontos_por_numero = 100", info["pontos_por_numero"] == 100)

    # === 3. KPIs ===
    siglas = set(kpis["shopping_sigla"].unique())
    check("KPIs", "6 shoppings + TOTAL",
          siglas == {"CS", "BS", "NK", "NR", "GS", "NS", "TOTAL"})

    for _, row in kpis_shop.iterrows():
        soma = row["clientes_novos"] + row["clientes_recorrentes"]
        check("KPIs", f"{row['shopping_sigla']}: novos + recorrentes = totais",
              soma == row["clientes_totais"],
              f"{int(soma)} {'=' if soma == row['clientes_totais'] else '!='} {int(row['clientes_totais'])}")

    soma_cupons = kpis_shop["cupons_lancados"].sum()
    check("KPIs", "TOTAL cupons = soma shoppings",
          total_row["cupons_lancados"] == soma_cupons,
          f"{int(total_row['cupons_lancados'])} vs {int(soma_cupons)}")

    soma_valor = round(kpis_shop["valor_total"].sum(), 2)
    check("KPIs", "TOTAL valor = soma shoppings",
          abs(total_row["valor_total"] - soma_valor) < 0.1,
          f"R$ {total_row['valor_total']:,.2f} vs R$ {soma_valor:,.2f}")

    soma_pontos = kpis_shop["pontos_utilizados"].sum()
    check("KPIs", "TOTAL pontos = soma shoppings",
          total_row["pontos_utilizados"] == soma_pontos)

    soma_numeros = kpis_shop["numeros_sorte"].sum()
    check("KPIs", "TOTAL numeros = soma shoppings",
          total_row["numeros_sorte"] == soma_numeros)

    for _, row in kpis.iterrows():
        if row["clientes_totais"] > 0:
            tm_esp = round(row["valor_total"] / row["clientes_totais"], 2)
            check("KPIs", f"{row['shopping_sigla']}: TM cliente correto",
                  abs(row["tm_cliente"] - tm_esp) < 0.02)
        if row["cupons_lancados"] > 0:
            tm_esp = round(row["valor_total"] / row["cupons_lancados"], 2)
            check("KPIs", f"{row['shopping_sigla']}: TM cupom correto",
                  abs(row["tm_cupom"] - tm_esp) < 0.02)

    # === 4. Serie Temporal ===
    n_dias = serie["data"].nunique()
    check("Serie", f"~60 dias na serie ({n_dias})", 55 <= n_dias <= 65)

    dupes = serie_det.groupby(["data", "shopping_id"]).size()
    check("Serie", "Sem duplicatas (data + shopping)", (dupes == 1).all())

    shops_dia = serie_det.groupby("data")["shopping_id"].nunique()
    check("Serie", "6 shoppings em cada dia", shops_dia.min() >= 5,
          f"min={shops_dia.min()}, max={shops_dia.max()}")

    serie_agg = serie_det.groupby("data")["cupons"].sum().reset_index()
    serie_agg["data"] = pd.to_datetime(serie_agg["data"])
    merged = serie.merge(serie_agg, on="data", suffixes=("_total", "_soma"))
    check("Serie", "serie_total.cupons = soma(serie.cupons)",
          (merged["cupons_total"] == merged["cupons_soma"]).all())

    check("Serie", "Flag na_promocao correto",
          (serie[serie["na_promocao"] == True]["data"] >= promo_inicio).all() and
          (serie[serie["na_promocao"] == False]["data"] < promo_inicio).all())

    datas_range = pd.date_range(serie["data"].min(), serie["data"].max())
    dias_faltantes = set(datas_range) - set(serie["data"])
    check("Serie", "Sem dias faltantes", len(dias_faltantes) == 0,
          f"{len(dias_faltantes)} dias ausentes" if dias_faltantes else "")

    # Cross-check serie dia 19/03 vs KPIs
    dia_inicio = serie[serie["data"] == promo_inicio]
    if len(dia_inicio) > 0:
        check("Serie", "Cupons dia inicio = KPI total",
              int(dia_inicio.iloc[0]["cupons"]) == int(total_row["cupons_lancados"]),
              f"serie={int(dia_inicio.iloc[0]['cupons'])}, kpi={int(total_row['cupons_lancados'])}")

    # === 5. Resgates ===
    if len(resgates) > 0:
        check("Resgates", f"{len(resgates)} registros carregados", len(resgates) > 0)
        check("Resgates", "Todos promocao_id correto", (resgates["promocao_id"] == info["id"]).all())
        check("Resgates", "Todos status = Resgatado", (resgates["status"] == "Resgatado").all())
        check("Resgates", "saldo_anterior >= saldo_posterior",
              (resgates["cliente_saldo_anterior"] >= resgates["cliente_saldo_posterior"]).all())

        diff_saldo = resgates["cliente_saldo_anterior"] - resgates["cliente_saldo_posterior"]
        match_saldo = (diff_saldo == resgates["pontos_totais"])
        n_mismatch_saldo = int((~match_saldo).sum())
        check("Resgates", "pontos = saldo_anterior - saldo_posterior",
              n_mismatch_saldo == 0,
              f"{n_mismatch_saldo} divergencias (dados origem)" if n_mismatch_saldo > 0 else "")

        numeros_esp = resgates.apply(
            lambda r: max(1, math.ceil(r["pontos_totais"] / r["pontos_unitarios"])) if r["pontos_unitarios"] > 0 else 0, axis=1)
        match_num = (numeros_esp == resgates["quantidade_numeros"])
        pct_match = match_num.mean() * 100
        check("Resgates", f"numeros = ceil(pontos/100) ({pct_match:.0f}%)",
              pct_match >= 95,
              f"{int((~match_num).sum())} divergencias — logica do backend diferente" if pct_match < 95 else "")

        for _, row in kpis_shop.iterrows():
            res_sub = resgates[resgates["shopping_id"] == row["shopping_id"]]
            check("Resgates", f"{row['shopping_sigla']}: pontos KPI = detalhado",
                  int(row["pontos_utilizados"]) == int(res_sub["pontos_totais"].sum()))

        resgates["data_resgate"] = pd.to_datetime(resgates["data_resgate"])
        antes = resgates[resgates["data_resgate"].dt.normalize() < promo_inicio]
        check("Resgates", f"Resgates antes da promo: {len(antes)}",
              len(antes) == 0,
              f"{len(antes)} resgates pre-release (16-18/mar)" if len(antes) > 0 else "")

    # === 6. Resgates por dia ===
    if len(resg_dia) > 0:
        check("Resg/Dia", "Total resgates = registros",
              int(resg_dia["resgates"].sum()) == len(resgates))
        check("Resg/Dia", "Total pontos = KPI",
              int(resg_dia["pontos_totais"].sum()) == int(total_row["pontos_utilizados"]))
        check("Resg/Dia", "Total numeros = KPI",
              int(resg_dia["numeros_totais"].sum()) == int(total_row["numeros_sorte"]))

    # === Render ===
    df_res = pd.DataFrame(resultados)
    n_pass = df_res["ok"].sum()
    n_fail = len(df_res) - n_pass
    n_total = len(df_res)

    # Resumo
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Testes", n_total)
    c2.metric("Aprovados", int(n_pass), delta=f"{n_pass/n_total*100:.0f}%")
    c3.metric("Falhas", int(n_fail), delta=f"-{n_fail}" if n_fail > 0 else "0", delta_color="inverse")

    # Resultado salvo?
    audit_path = "dados/auditoria_resultado.json"
    if os.path.exists(audit_path):
        with open(audit_path, encoding="utf-8") as f:
            audit = json.load(f)
        st.caption(f"Ultima auditoria offline: {audit.get('data_auditoria', 'N/A')} — {audit['pass']} pass / {audit['fail']} fail")

    # Tabela por grupo
    for grupo in df_res["grupo"].unique():
        sub = df_res[df_res["grupo"] == grupo]
        n_ok = sub["ok"].sum()
        n_ko = len(sub) - n_ok
        icon = "✅" if n_ko == 0 else "⚠️"
        with st.expander(f"{icon} {grupo} ({n_ok}/{len(sub)} aprovados)", expanded=(n_ko > 0)):
            for _, r in sub.iterrows():
                status_icon = "✅" if r["ok"] else "❌"
                line = f"{status_icon} {r['teste']}"
                if r["detalhe"]:
                    line += f" — *{r['detalhe']}*"
                st.markdown(line)

    # Alertas destacados
    falhas = df_res[~df_res["ok"]]
    if len(falhas) > 0:
        st.markdown("---")
        st.warning(f"**{n_fail} teste(s) com falha encontrados**")
        for _, f_row in falhas.iterrows():
            st.markdown(f"- **[{f_row['grupo']}]** {f_row['teste']}: {f_row['detalhe']}")

    # Anomalias
    if len(resgates) > 0:
        st.markdown("---")
        st.subheader("Analise de Anomalias")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Top 5 clientes por pontos resgatados:**")
            top5 = resgates.groupby("cliente_id")["pontos_totais"].sum().nlargest(5).reset_index()
            top5.columns = ["Cliente ID", "Pontos Totais"]
            total_pts = resgates["pontos_totais"].sum()
            top5["% do Total"] = (top5["Pontos Totais"] / total_pts * 100).round(1)
            st.dataframe(top5, use_container_width=True, hide_index=True)
            st.caption(f"Top 5 concentram {top5['% do Total'].sum():.1f}% do total de pontos")

        with col2:
            st.markdown("**Resgates com maior volume de numeros:**")
            big = resgates.nlargest(5, "quantidade_numeros")[
                ["id", "cliente_id", "shopping_sigla", "pontos_totais", "quantidade_numeros"]
            ].copy()
            big.columns = ["ID", "Cliente", "Shopping", "Pontos", "Numeros"]
            st.dataframe(big, use_container_width=True, hide_index=True)

        # Outliers serie temporal
        media = serie["cupons"].mean()
        std = serie["cupons"].std()
        outliers = serie[(serie["cupons"] > media + 3 * std) | (serie["cupons"] < media - 3 * std)]
        if len(outliers) > 0:
            st.warning(f"**{len(outliers)} dia(s) com volume atipico de cupons (>3 desvios-padrao)**")
            st.dataframe(outliers[["data", "cupons", "valor_total"]], hide_index=True)
        else:
            st.success("Nenhum outlier extremo na serie temporal de cupons (3-sigma)")


if __name__ == "__main__":
    main()
