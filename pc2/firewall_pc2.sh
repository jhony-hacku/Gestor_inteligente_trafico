#!/usr/bin/env bash
# =============================================================================
#  firewall_pc2.sh — Configuración UFW para PC2 (10.43.100.91)
#  Gestor Inteligente de Tráfico Urbano
# =============================================================================
#
#  PC2 es el nodo central: recibe datos de PC1 y órdenes de PC3.
#
#  Puertos abiertos en PC2 y quién puede conectarse:
#  ┌──────┬───────────────────────────────────────────────────────────┐
#  │ Puerto │ Descripción                           │ Fuente permitida │
#  ├──────┬───────────────────────────────────────────────────────────┤
#  │ 22    │ SSH (administración)                   │ Cualquier IP     │
#  │ 5562  │ PUSH receptor (datos de PC1)           │ 10.43.99.192     │
#  │ 5563  │ Control REP CurveZMQ (órdenes PC3)    │ 10.43.99.78      │
#  │ 5564  │ Consulta REP failover (réplica → PC3) │ 10.43.99.78      │
#  └──────┴───────────────────────────────────────────────────────────┘
#
#  NOTA: PC2 inicia conexiones SALIENTES hacia:
#    • PC1:5560 (se suscribe al XPUB del broker)
#    • PC1:5565 (envía pings de heartbeat)
#    • PC3:5561 (push de eventos procesados)
#    • PC3:5566 (envía pings de heartbeat)
#  Estas conexiones salientes están permitidas por la política default
#  "allow outgoing", por lo que no necesitan reglas adicionales.
#
#  Uso:
#      sudo bash firewall_pc2.sh
#
# =============================================================================

set -euo pipefail

PC1_IP="10.43.99.192"
PC2_IP="10.43.100.91"
PC3_IP="10.43.99.78"

echo ""
echo "============================================================"
echo "  Configurando UFW — PC2 ($PC2_IP)"
echo "============================================================"
echo ""

if [[ $EUID -ne 0 ]]; then
    echo "[ERROR] Este script requiere privilegios de root."
    echo "        Usa: sudo bash firewall_pc2.sh"
    exit 1
fi

if ! command -v ufw &> /dev/null; then
    echo "[INFO] Instalando UFW..."
    apt-get install -y ufw
fi

# ── PASO 1: No interferir con la política por defecto ────────────────────────
echo "[UFW] Manteniendo configuración global y servicios de terceros intactos..."
# Eliminamos el reset y las politicas por defecto para no romper RDP/Zabbix

# ── PASO 2: Reglas específicas del sistema de tráfico (PC2) ─────────────────
# Las reglas de UFW se leen en orden. Primero permitimos la IP correcta,
# luego denegamos el resto del tráfico a ese puerto específico.

# Puerto 5562 — Receptor de datos: PC1 envía eventos procesados a PC2
echo "[UFW] Protegiendo Puerto 5562 (PULL receptor datos) → solo PC1 ($PC1_IP)..."
ufw allow from "$PC1_IP" to any port 5562 proto tcp comment "PULL datos - permite PC1"
ufw deny to any port 5562 proto tcp comment "PULL datos - bloquea resto"

# Puerto 5563 — Control REP CurveZMQ: PC3 envía comandos de semáforo
echo "[UFW] Protegiendo Puerto 5563 (Control REP CurveZMQ) → solo PC3 ($PC3_IP)..."
ufw allow from "$PC3_IP" to any port 5563 proto tcp comment "Control REP - permite PC3"
ufw deny to any port 5563 proto tcp comment "Control REP - bloquea resto"

# Puerto 5564 — Consulta REP failover: PC3 consulta réplica en PC2
echo "[UFW] Protegiendo Puerto 5564 (Consulta REP failover) → solo PC3 ($PC3_IP)..."
ufw allow from "$PC3_IP" to any port 5564 proto tcp comment "Consulta failover - permite PC3"
ufw deny to any port 5564 proto tcp comment "Consulta failover - bloquea resto"

# ── PASO 3: Habilitar UFW ───────────────────────────────────────────────────
echo ""
echo "[UFW] Habilitando firewall..."
ufw --force enable

# ── RESUMEN ─────────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo "  UFW activo en PC2 — Reglas aplicadas:"
echo "============================================================"
ufw status verbose
echo ""
echo "  Tráfico bloqueado por defecto:"
echo "    • Cualquier IP no autorizada intentando acceder a los puertos 5562, 5563 y 5564."
echo "  [NOTA] Los demás puertos (RDP, Zabbix, SSH) NO han sido modificados."
echo ""
echo "  [OK] Configuración de firewall completada en PC2."
echo ""
