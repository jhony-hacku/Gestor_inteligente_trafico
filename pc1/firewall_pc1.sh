#!/usr/bin/env bash
# =============================================================================
#  firewall_pc1.sh — Configuración UFW para PC1 (10.43.99.192)
#  Gestor Inteligente de Tráfico Urbano
# =============================================================================
#
#  Puertos abiertos en PC1 y quién puede conectarse:
#  ┌──────┬──────────────────────────────────────────────────────────┐
#  │ Puerto │ Descripción                                              │ Fuente permitida │
#  ├──────┼──────────────────────────────────────────────────────────┤
#  │ 22    │ SSH (administración)                                     │ Cualquier IP     │
#  │ 5559  │ XSUB Broker (entrada sensores)                           │ Solo 127.0.0.1   │
#  │ 5560  │ XPUB Broker (salida → PC2)                               │ 10.43.100.91     │
#  │ 5565  │ Heartbeat REP (PC2 verifica PC1)                         │ 10.43.100.91     │
#  └──────┴──────────────────────────────────────────────────────────┘
#
#  Uso:
#      sudo bash firewall_pc1.sh
#
# =============================================================================

set -euo pipefail

PC1_IP="10.43.99.192"
PC2_IP="10.43.100.91"
PC3_IP="10.43.99.78"

echo ""
echo "============================================================"
echo " Configurando UFW — PC1 ($PC1_IP)"
echo "============================================================"
echo ""

# Verificar que se ejecuta como root
if [[ $EUID -ne 0 ]]; then
    echo "[ERROR] Este script requiere privilegios de root."
    echo "        Usa: sudo bash firewall_pc1.sh"
    exit 1
fi

# Asegurar que UFW está instalado
if ! command -v ufw &> /dev/null; then
    echo "[INFO] Instalando UFW..."
    apt-get update && apt-get install -y ufw
fi

# ── PASO 1: Asegurar acceso SSH ─────────────────────────────────────────────
# Lo primero es garantizar que no nos quedemos fuera de la consola remota.
echo "[UFW] Asegurando puerto SSH (22)..."
ufw allow 22/tcp comment "SSH - administracion"

# ── PASO 2: Limpieza preventiva de reglas viejas de ZeroMQ ──────────────────
# Para evitar duplicados caóticos, eliminamos las reglas existentes de estos puertos
# antes de inyectar las nuevas en orden correcto.
echo "[UFW] Limpiando reglas previas de los puertos del sistema de tráfico..."
ufw delete allow 5559/tcp || true
ufw delete deny 5559/tcp || true
ufw delete allow 5560/tcp || true
ufw delete deny 5560/tcp || true
ufw delete allow 5565/tcp || true
ufw delete deny 5565/tcp || true

# ── PASO 3: Inyección de reglas específicas en orden estricto ───────────────
# Usamos 'insert 1' para empujar las reglas al inicio de la tabla.
# Como las últimas que insertamos quedan más arriba, las metemos en orden inverso
# para que el ALLOW siempre quede por encima del DENY correspondiente.

# --- Puerto 5565: Heartbeat REP ---
echo "[UFW] Configurando Puerto 5565 (Heartbeat REP) para PC2..."
ufw insert 1 deny to any port 5565 proto tcp comment "Heartbeat REP - bloquea resto"
ufw insert 1 allow from "$PC2_IP" to any port 5565 proto tcp comment "Heartbeat REP - permite PC2"

# --- Puerto 5560: XPUB Broker ---
echo "[UFW] Configurando Puerto 5560 (XPUB Broker) para PC2..."
ufw insert 1 deny to any port 5560 proto tcp comment "XPUB Broker - bloquea resto"
ufw insert 1 allow from "$PC2_IP" to any port 5560 proto tcp comment "XPUB Broker - permite PC2"

# --- Puerto 5559: XSUB Broker ---
echo "[UFW] Configurando Puerto 5559 (XSUB Broker) para Localhost..."
ufw insert 1 deny to any port 5559 proto tcp comment "XSUB Broker - bloquea resto"
ufw insert 1 allow from 127.0.0.1 to any port 5559 proto tcp comment "XSUB Broker - permite localhost"


# ── PASO 4: Habilitar UFW ───────────────────────────────────────────────────
echo ""
echo "[UFW] Aplicando cambios y habilitando firewall..."
ufw --force enable

# ── RESUMEN ─────────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo "  UFW activo en PC1 — Reglas aplicadas ordenadas:"
echo "============================================================"
ufw status numbered
echo ""
echo "  [OK] Configuración del Gestor Inteligente de Tráfico completada en PC1."
echo ""