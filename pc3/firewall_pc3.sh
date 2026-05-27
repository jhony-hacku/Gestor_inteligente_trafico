#!/usr/bin/env bash
# =============================================================================
#  firewall_pc3.sh — Configuración UFW para PC3 (10.43.99.78)
#  Gestor Inteligente de Tráfico Urbano
# =============================================================================
#
#  PC3 es el nodo de persistencia y monitoreo. Recibe datos de PC2
#  y expone su servidor de heartbeat para que PC2 verifique su salud.
#
#  Puertos abiertos en PC3 y quién puede conectarse:
#  ┌──────┬───────────────────────────────────────────────────────────┐
#  │ Puerto │ Descripción                           │ Fuente permitida │
#  ├──────┬───────────────────────────────────────────────────────────┤
#  │ 22    │ SSH (administración)                   │ Cualquier IP     │
#  │ 5561  │ Receptor PULL (datos de PC2)           │ 10.43.100.91     │
#  │ 5566  │ Heartbeat REP (PC2 verifica PC3)       │ 10.43.100.91     │
#  └──────┴───────────────────────────────────────────────────────────┘
#
#  NOTA: PC3 inicia conexiones SALIENTES hacia:
#    • PC2:5563 (envía comandos de control CurveZMQ)
#    • PC2:5564 (consultas de failover)
#  Estas conexiones salientes están permitidas por "allow outgoing".
#
#  Uso:
#      sudo bash firewall_pc3.sh
#
# =============================================================================

set -euo pipefail

PC1_IP="10.43.99.192"
PC2_IP="10.43.100.91"
PC3_IP="10.43.99.78"

echo ""
echo "============================================================"
echo "  Configurando UFW — PC3 ($PC3_IP)"
echo "============================================================"
echo ""

if [[ $EUID -ne 0 ]]; then
    echo "[ERROR] Este script requiere privilegios de root."
    echo "        Usa: sudo bash firewall_pc3.sh"
    exit 1
fi

if ! command -v ufw &> /dev/null; then
    echo "[INFO] Instalando UFW..."
    apt-get install -y ufw
fi

# ── PASO 1: No interferir con la política por defecto ────────────────────────
echo "[UFW] Manteniendo configuración global y servicios de terceros intactos..."
# Eliminamos el reset y las politicas por defecto para no romper RDP/Zabbix

# ── PASO 2: Reglas específicas del sistema de tráfico (PC3) ─────────────────
# Las reglas de UFW se leen en orden. Primero permitimos la IP correcta,
# luego denegamos el resto del tráfico a ese puerto específico.

# Puerto 5561 — Receptor PULL: PC2 empuja eventos de tráfico procesados
echo "[UFW] Protegiendo Puerto 5561 (PULL receptor datos) → solo PC2 ($PC2_IP)..."
ufw allow from "$PC2_IP" to any port 5561 proto tcp comment "PULL datos - permite PC2"
ufw deny to any port 5561 proto tcp comment "PULL datos - bloquea resto"

# Puerto 5566 — Heartbeat REP: PC2 verifica el estado de PC3
echo "[UFW] Protegiendo Puerto 5566 (Heartbeat REP) → solo PC2 ($PC2_IP)..."
ufw allow from "$PC2_IP" to any port 5566 proto tcp comment "Heartbeat REP - permite PC2"
ufw deny to any port 5566 proto tcp comment "Heartbeat REP - bloquea resto"

# ── PASO 3: Habilitar UFW ───────────────────────────────────────────────────
echo ""
echo "[UFW] Habilitando firewall..."
ufw --force enable

# ── RESUMEN ─────────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo "  UFW activo en PC3 — Reglas aplicadas:"
echo "============================================================"
ufw status verbose
echo ""
echo "  Tráfico bloqueado por defecto:"
echo "    • Cualquier IP no autorizada intentando acceder a los puertos 5561 y 5566."
echo "  [NOTA] Los demás puertos (RDP, Zabbix, SSH) NO han sido modificados."
echo ""
echo "  [OK] Configuración de firewall completada en PC3."
echo ""
