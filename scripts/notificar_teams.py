"""
Notificacao Microsoft Teams para pipelines de dados.

Substitui o notificar_whatsapp.py anterior, que dependia do gateway Baileys
rodando em localhost:3001 e, portanto, de self-hosted runner. Este script
faz uma chamada HTTP simples ao Power Automate Workflow webhook, rodando
diretamente em ubuntu-latest.

Uso:
    python scripts/notificar_teams.py --dashboard "Perfil de Cliente" --status sucesso --detalhes "15 CSVs atualizados"
    python scripts/notificar_teams.py --dashboard "Promocoes Report" --status erro
    python scripts/notificar_teams.py --dashboard "Perfil de Cliente" --status alerta --detalhes "Secret quebrado: GOOGLE_PLAY_KEY_B64"

Variaveis de ambiente:
    TEAMS_WEBHOOK_URL (obrigatorio) - URL do Power Automate Workflow do Teams.
"""
import os
import sys
import argparse
import json
from datetime import datetime, timedelta, timezone

try:
    import requests
except ImportError:
    print("[AVISO] requests nao instalado, notificacao Teams indisponivel")
    sys.exit(0)


WEBHOOK_URL = os.environ.get("TEAMS_WEBHOOK_URL", "")

# Configuracao por dashboard: emoji, URL, org/repo no GitHub
DASHBOARD_CONFIG = {
    "Perfil de Cliente": {
        "emoji": "\U0001F464",   # 👤
        "cor": "0078D4",         # azul MS
        "url": "https://dashboard-perfil-cliente.streamlit.app",
        "repo": "grupoalmeidajunior/dashboard-perfil-cliente",
    },
    "Shopping Relacionamento": {
        "emoji": "\U0001F6CD",   # 🛍
        "cor": "107C10",         # verde MS
        "url": "https://shopping-relacionamento.streamlit.app",
        "repo": "grupoalmeidajunior/shopping-relacionamento",
    },
    "Promocoes Report": {
        "emoji": "\U0001F3AF",   # 🎯
        "cor": "8764B8",         # roxo MS
        "url": "https://promocoes-report.streamlit.app",
        "repo": "grupoalmeidajunior/promocoes-report",
    },
    "Media Monitoring": {
        "emoji": "\U0001F4CA",   # 📊
        "cor": "038387",         # teal MS
        "url": "https://dashboard-media-monitoring.streamlit.app",
        "repo": "grupoalmeidajunior/dashboard-media-monitoring",
    },
    "Normas Regulamentadoras": {
        "emoji": "\U0001F4DC",   # 📜
        "cor": "CA5010",         # laranja MS
        "url": "https://dashboard-normas-regulamentadoras.streamlit.app",
        "repo": "carlosgravi/dashboard-normas-regulamentadoras",
    },
}

# Prefixo de titulo por status
STATUS_CONFIG = {
    "sucesso":  {"prefixo": "✅ Atualizacao concluida",       "cor": "107C10"},
    "erro":     {"prefixo": "❌ ERRO",                         "cor": "D13438"},
    "inicio":   {"prefixo": "\U0001F504 Atualizacao iniciada",     "cor": "0078D4"},
    "alerta":   {"prefixo": "⚠ ALERTA",                       "cor": "CA5010"},
}


def _timestamp_br() -> str:
    """Retorna horario de Brasilia formatado."""
    return (datetime.now(timezone.utc) - timedelta(hours=3)).strftime("%d/%m/%Y %H:%M")


def montar_card(dashboard: str, status: str, detalhes: str | None) -> dict:
    """Monta um AdaptiveCard para o Teams."""
    cfg_dash = DASHBOARD_CONFIG.get(dashboard, {
        "emoji": "\U0001F4C8", "cor": "605E5C", "url": "", "repo": "",
    })
    cfg_status = STATUS_CONFIG.get(status, {
        "prefixo": status.upper(), "cor": cfg_dash["cor"],
    })

    titulo = f"{cfg_dash['emoji']} {cfg_status['prefixo']} - {dashboard}"

    body: list[dict] = [
        {
            "type": "TextBlock",
            "text": titulo,
            "weight": "Bolder",
            "size": "Medium",
            "wrap": True,
        },
        {
            "type": "TextBlock",
            "text": f"\U0001F4C5 {_timestamp_br()}",
            "isSubtle": True,
            "spacing": "Small",
            "wrap": True,
        },
    ]

    if status == "sucesso":
        body.append({
            "type": "TextBlock",
            "text": "Pipeline executado com sucesso",
            "wrap": True,
        })
    elif status == "erro":
        body.append({
            "type": "TextBlock",
            "text": "Falha na atualizacao automatica. Verifique os logs do GitHub Actions.",
            "wrap": True,
            "color": "Attention",
        })
    elif status == "inicio":
        body.append({
            "type": "TextBlock",
            "text": "Pipeline em execucao...",
            "wrap": True,
        })
    elif status == "alerta":
        body.append({
            "type": "TextBlock",
            "text": "Manutencao necessaria",
            "wrap": True,
            "color": "Warning",
        })

    if detalhes:
        body.append({
            "type": "TextBlock",
            "text": detalhes,
            "wrap": True,
            "spacing": "Medium",
        })

    # Acoes (botoes)
    actions: list[dict] = []
    if cfg_dash["url"]:
        actions.append({
            "type": "Action.OpenUrl",
            "title": "Abrir Dashboard",
            "url": cfg_dash["url"],
        })
    if cfg_dash["repo"]:
        destino = "actions" if status in ("erro", "alerta") else ""
        url_repo = f"https://github.com/{cfg_dash['repo']}"
        if destino:
            url_repo += f"/{destino}"
        if status == "alerta":
            url_repo = f"https://github.com/{cfg_dash['repo']}/settings/secrets/actions"
        actions.append({
            "type": "Action.OpenUrl",
            "title": "GitHub" if status not in ("erro", "alerta") else ("Logs" if status == "erro" else "Secrets"),
            "url": url_repo,
        })

    card = {
        "type": "AdaptiveCard",
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.4",
        "body": body,
    }
    if actions:
        card["actions"] = actions

    return card


def enviar(card: dict) -> bool:
    """Envia AdaptiveCard ao webhook do Power Automate Workflow."""
    if not WEBHOOK_URL:
        print("[AVISO] TEAMS_WEBHOOK_URL nao configurado")
        return False

    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": card,
            }
        ],
    }

    try:
        resp = requests.post(WEBHOOK_URL, json=payload, timeout=15)
    except requests.RequestException as exc:
        print(f"[ERRO] Teams: {exc}")
        return False

    # Power Automate devolve 202 (Accepted) para webhooks de workflow
    if resp.status_code in (200, 202):
        print("Teams: mensagem enviada")
        return True
    print(f"[ERRO] Teams HTTP {resp.status_code}: {resp.text[:200]}")
    return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Notificacao Teams para pipelines")
    parser.add_argument("--dashboard", required=True, help="Nome do dashboard")
    parser.add_argument(
        "--status",
        required=True,
        choices=["sucesso", "erro", "inicio", "alerta"],
        help="Status da execucao",
    )
    parser.add_argument("--detalhes", default=None, help="Detalhes adicionais")
    args = parser.parse_args()

    card = montar_card(args.dashboard, args.status, args.detalhes)
    enviar(card)
