"""
Script de notificacao WhatsApp para pipelines de dados.
Envia mensagens formatadas via WhatsApp Gateway local (Baileys).

Uso:
    python scripts/notificar_whatsapp.py --dashboard "Perfil de Cliente" --status sucesso --detalhes "15 CSVs atualizados"
    python scripts/notificar_whatsapp.py --dashboard "Perfil de Cliente" --status erro
    python scripts/notificar_whatsapp.py --dashboard "Shopping Relacionamento" --status sucesso --detalhes "42 CSVs sincronizados"
"""
import os
import sys
import argparse
from datetime import datetime, timedelta

try:
    import requests
except ImportError:
    print("[AVISO] requests nao instalado, notificacao WhatsApp indisponivel")
    sys.exit(0)

WA_GATEWAY_URL = os.environ.get("WA_GATEWAY_URL", "http://localhost:3001")
WA_GROUP_ID = os.environ.get("WA_GROUP_ID", "")

# Emojis por dashboard
DASHBOARD_CONFIG = {
    "Promocoes Report": {
        "emoji": "\U0001F3AF",       # 🎯
        "url": "https://promocoes-report.streamlit.app",
        "repo": "carlosgravi/promocoes-report",
    },
    "Perfil de Cliente": {
        "emoji": "\U0001F464",       # 👤
        "url": "https://dashboard-perfil-cliente.streamlit.app",
        "repo": "carlosgravi/dashboard-perfil-cliente",
    },
    "Shopping Relacionamento": {
        "emoji": "\U0001F6CD",       # 🛍
        "url": "https://shopping-relacionamento.streamlit.app",
        "repo": "carlosgravi/shopping-relacionamento",
    },
    "Normas Regulamentadoras": {
        "emoji": "\U0001F4DC",       # 📜
        "url": "https://dashboard-normas-regulamentadoras.streamlit.app",
        "repo": "carlosgravi/dashboard-normas-regulamentadoras",
    },
}


def enviar_wa(mensagem):
    """Envia mensagem via WhatsApp Gateway."""
    if not WA_GROUP_ID:
        print("[AVISO] WA_GROUP_ID nao configurado")
        return False

    try:
        status_resp = requests.get(f"{WA_GATEWAY_URL}/status", timeout=5)
        status_data = status_resp.json()
        if status_data.get("status") != "connected":
            print(f"[AVISO] Gateway nao conectado (status: {status_data.get('status')})")
            return False
    except requests.RequestException:
        print("[AVISO] Gateway indisponivel")
        return False

    try:
        resp = requests.post(
            f"{WA_GATEWAY_URL}/send",
            json={"chatId": WA_GROUP_ID, "message": mensagem},
            timeout=15,
        )
        if resp.status_code == 200:
            print("WhatsApp: mensagem enviada")
            return True
        else:
            print(f"[ERRO] WhatsApp: {resp.json().get('error', resp.status_code)}")
            return False
    except requests.RequestException as e:
        print(f"[ERRO] WhatsApp: {e}")
        return False


def montar_mensagem(dashboard, status, detalhes=None):
    """Monta mensagem formatada para WhatsApp."""
    config = DASHBOARD_CONFIG.get(dashboard, {"emoji": "\U0001F4CA", "url": "", "repo": ""})
    emoji_dash = config["emoji"]
    url = config["url"]
    repo = config["repo"]

    # Horario de Brasilia
    agora = (datetime.utcnow() - timedelta(hours=3)).strftime("%d/%m/%Y %H:%M")

    if status == "sucesso":
        linhas = [
            f"\u2705 *Atualizacao Concluida — {dashboard}*",
            f"",
            f"\U0001F4C5 {agora}",
            f"",
            f"\U0001F504 Pipeline executado com sucesso",
        ]
        if detalhes:
            linhas.append(f"{emoji_dash} {detalhes}")
        linhas.append(f"")
        if url:
            linhas.append(f"\U0001F310 Dashboard atualizado e disponivel")
            linhas.append(f"\U0001F517 {url}")

    elif status == "erro":
        linhas = [
            f"\u274C *ERRO — {dashboard}*",
            f"",
            f"\U0001F4C5 {agora}",
            f"",
            f"\u26A0 Falha na atualizacao automatica",
            f"\U0001F6E0 Verifique os logs do GitHub Actions",
        ]
        if repo:
            linhas.append(f"")
            linhas.append(f"\U0001F517 github.com/{repo}/actions")

    elif status == "inicio":
        linhas = [
            f"\U0001F504 *Atualizacao Iniciada — {dashboard}*",
            f"",
            f"\U0001F4C5 {agora}",
            f"",
            f"\u23F3 Pipeline em execucao...",
        ]

    else:
        linhas = [f"{emoji_dash} *{dashboard}* — {status}", f"", f"\U0001F4C5 {agora}"]
        if detalhes:
            linhas.append(f"")
            linhas.append(detalhes)

    return "\n".join(linhas)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Notificacao WhatsApp para pipelines")
    parser.add_argument("--dashboard", required=True, help="Nome do dashboard")
    parser.add_argument("--status", required=True, choices=["sucesso", "erro", "inicio"],
                        help="Status da execucao")
    parser.add_argument("--detalhes", default=None, help="Detalhes adicionais")
    args = parser.parse_args()

    mensagem = montar_mensagem(args.dashboard, args.status, args.detalhes)
    enviar_wa(mensagem)
