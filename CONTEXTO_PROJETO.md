# Promoções Report - Contexto do Projeto

## Visão Geral
Dashboard Streamlit para acompanhamento em tempo real de promoções dos shoppings Almeida Junior.
Extrai dados do Snowflake (camada Bronze) e apresenta KPIs, séries temporais, resgates de pontos e validação de integridade.

**Repositório:** https://github.com/grupoalmeidajunior/promocoes-report
**Deploy:** Streamlit Cloud
**Porta local:** 8502

## Promoção Atual
- **iPhone 17** (ID=1)
- Período: 2026-03-19 a 2026-04-19
- Sorteio: 2026-04-24
- Regra: 100 pontos = 1 número da sorte

## Estrutura de Arquivos

```
Promocoes_Report/
├── dashboard_promocoes.py          # Dashboard principal (4 abas)
├── scripts/
│   ├── extrair_dados_promocao.py   # Extração Snowflake → CSVs
│   ├── auditar_dados.py            # Auditoria (89+ testes, 7 grupos)
│   ├── notificar_whatsapp.py       # Notificação WhatsApp via Baileys
│   └── config_snowflake.ini        # Config conexão Snowflake
├── dados/
│   ├── promocao_info.json          # Metadados da promoção ativa
│   ├── kpis_promocao.csv           # KPIs por shopping + TOTAL
│   ├── serie_temporal.csv          # Cupons/dia por shopping (60 dias)
│   ├── serie_temporal_total.csv    # Cupons/dia agregado
│   ├── resgates_pontos.csv         # Detalhe de cada resgate
│   ├── resgates_por_dia.csv        # Resgates agregados por dia
│   └── auditoria_resultado.json    # Resultado da última auditoria
├── .github/workflows/
│   └── atualizar_dados.yml         # GitHub Actions (extração + WhatsApp)
└── .streamlit/
    └── config.toml
```

## Dashboard - 4 Abas

1. **Report Geral** — KPIs destaque (5 cards) + tabela detalhada por shopping + gráficos comparativos
2. **Série Temporal** — Cupons e valor por dia (60 dias), linha de início da promoção, média pré-promo
3. **Resgates de Pontos** — 5 KPIs (total resgates, clientes únicos, pontos, números da sorte, média) + gráfico por dia + tabela por shopping
4. **Validação** — Testes automáticos de integridade (estrutura, promoção, KPIs, série, resgates, anomalias)

## Tabelas Snowflake (Bronze)

| Tabela | Uso |
|--------|-----|
| `BRZ_AJFANS_PROMOCAO` | Info da promoção ativa (título, datas, pontos) |
| `BRZ_AJFANS_FIDELIDADE_CUPOM` | Cupons validados (cliente, shopping, loja, valor) |
| `BRZ_AJFANS_CLIENTES` | Cadastro de clientes (DATA_CADASTRO, status) |
| `BRZ_AJFANS_SHOPPING_LOJA` | Lojas por shopping (CNPJ, nome, segmento) |
| `BRZ_AJFANS_PROMOCAO_RESGATE` | Resgates de pontos por números da sorte |
| `BRZ_AJFANS_LOG_ACESSO_APP` | Log de acesso ao app (atribuição de shopping) |

## Métricas e Regras de Negócio

### Clientes
- **Clientes Novos Cadastro**: `DATA_CADASTRO` durante o período da promoção. Shopping atribuído pelo último acesso no app (`LOG_ACESSO_APP` com `shopping_id IS NOT NULL`).
- **Clientes Novos Cupons**: primeiro cupom validado de todos os tempos caiu dentro do período da promoção.
- **Clientes Recorrentes**: já tinham cupons validados antes do início da promoção.
- **Clientes Totais**: únicos que lançaram pelo menos 1 cupom validado. Total geral pode ser menor que soma dos shoppings (clientes em +1 shopping).

### Cupons
- Somente `status = 'Validado'`
- Filtro: `data_envio` entre início da promoção e dados_ate (ontem 23:59:59)

### Pontos e Números da Sorte
- **pontos_utilizados = quantidade_numeros × 100** (NÃO usar coluna `pontos_totais` do BD — apresenta erro)
- Cada 100 pontos = 1 número da sorte

### Lojas
- Taxa de conversão = lojas com cupons / total de lojas do shopping

## Ordem dos Shoppings na Tabela
CS - BS - NK - NR - GS - NS - TOTAL

## Pipeline (GitHub Actions)

- **Trigger:** `workflow_dispatch` + cron (a definir)
- **Job 1 (ubuntu-latest):** extração Snowflake → CSVs → commit/push
- **Job 2 (self-hosted):** notificação WhatsApp via gateway Baileys (localhost:3001)
- **Self-hosted runner:** `C:\actions-runner-promocoes` (serviço Windows, instalar via `instalar_servico.bat` como admin)

## Decisões Técnicas

- `DATA_CADASTRO` (DATE) para cadastros, NÃO `TIME` (TIMESTAMP_TZ que é data de sync do Airbyte)
- Shopping de novos cadastros via `LOG_ACESSO_APP` (último acesso com `shopping_id IS NOT NULL`)
- Asterisco (*) no total de clientes únicos quando soma por shopping diverge (clientes multi-shopping)
- Tooltips com `<abbr title="">❓</abbr>` na tabela markdown + `help=` nos `st.metric`
- CSS: KPIs centralizados, min-height 120px, delta sem ícone de seta
- Cache removido temporariamente (`@st.cache_data` comentado) para evitar dados stale em dev

## Sessão Atual (20/03/2026)

### Alterações realizadas:
1. Configuração de secrets Snowflake no GitHub repo
2. Self-hosted runner (`C:\actions-runner-promocoes`) + workflow com WhatsApp
3. Auditoria completa (89+ testes) + aba Validação no dashboard
4. Separação clientes novos: Cadastro (DATA_CADASTRO) vs Cupons (primeiro cupom)
5. Atribuição de shopping via LOG_ACESSO_APP (63/63 atribuídos)
6. Tooltips explicativos em todas as métricas
7. Correção acentuação (ã, ç, é, ê, í, ó, ú) em todo o dashboard e auditoria
8. Fix pontos_utilizados: `quantidade_numeros × 100` em vez de `pontos_totais`
9. Asterisco no total de clientes com tooltip explicando multi-shopping
10. Reordenação shoppings: CS, BS, NK, NR, GS, NS
11. KPIs centralizados e altura uniforme (min-height 120px)
12. Aba Resgates: 5 KPIs com tooltips + delta clientes únicos vs por shopping

### Pendências:
- Instalar runner service como admin (`instalar_servico.bat`)
- Testar workflow via `workflow_dispatch`
- Rodar extração após instalar runner para atualizar `dados_ate` com "(final do dia)"
- Definir cron schedule no workflow

## Sessão 21/04/2026 — Correção de valores do dashboard

Dashboard vinha mostrando **30.224 cupons / R$ 7,25 M** quando os valores reais da promoção são **40.064 cupons / R$ 10,03 M**. Três causas somadas:

### Problemas corrigidos

1. **Extração parada desde 13/04** — user `NICHOLASMACHADO` foi desabilitado no Snowflake. Trocado para `SVC_DASHBOARDS_AJFANS` (service account com mesma chave RSA) em `scripts/config_snowflake.ini`.

2. **INNER JOIN descartava cupons** — a query de cupons fazia `INNER JOIN` com `BRZ_AJFANS_SHOPPING_LOJA` pelo CNPJ, descartando cupons cujo CNPJ não estava cadastrado. Impacto: 79 cupons / R$ 7.234 (77 deles em NS). Trocado para `LEFT JOIN`.

3. **`data_ate` avançava após fim da promo** — `data_ate = date.today() - 1` ia além de `promo_fim`. Corrigido para `min(ontem, promo_fim.date())`, e o rótulo `dados_ate` no `promocao_info.json` passa a refletir isso.

4. **`data_inicio` alterada no backend em 16/04** — alguém mudou o registro em `BRZ_AJFANS_PROMOCAO` de `2026-03-19` para `2026-04-16`, o que descartava 4 semanas de histórico. Adicionado override explícito no script para blindar contra isso:
   ```python
   PROMO_INICIO_OVERRIDE = {1: "2026-03-19"}  # promo_id: data
   ```

### Resultado final (19/03 a 19/04 23:59:59)

| Shopping | Cupons | Valor | Clientes |
|----------|-------:|------:|---------:|
| CS | 9.635 | R$ 2.050.338,32 | 2.282 |
| BS | 6.221 | R$ 2.384.974,09 | 1.823 |
| NK | 9.861 | R$ 2.419.450,15 | 2.327 |
| NR | 5.303 | R$ 932.422,40 | 1.227 |
| GS | 5.125 | R$ 1.210.212,77 | 1.375 |
| NS | 3.919 | R$ 1.032.365,42 | 1.188 |
| **TOTAL** | **40.064** | **R$ 10.029.763,15** | **9.836** |
