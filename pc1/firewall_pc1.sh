#!/usr/bin/env bash
# =============================================================================
#  firewall_pc1.sh — Configuración UFW para PC1 (10.43.99.192)
#  Gestor Inteligente de Tráfico Urbano
# =============================================================================
#
#  Puertos abiertos en PC1 y quién puede conectarse:
#  ┌──────┬──────────────────────────────────────────────────────────┐
#  │ Puerto │ Descripción                          │ Fuente permitida │
#  ├──────┬──────────────────────────────────────────────────────────┤
#  │ 22    │ SSH (administración)                  │ Cualquier IP     │
#  │ 5559  │ XSUB Broker (entrada sensores)        │ Solo 127.0.0.1   │
#  │ 5560  │ XPUB Broker (salida → PC2)            │ 10.43.100.91     │
#  │ 5565  │ Heartbeat REP (PC2 verifica PC1)      │ 10.43.100.91     │
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
echo "  Configurando UFW — PC1 ($PC1_IP)"
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
    apt-get install -y ufw
fi

# ── PASO 1: No interferir con la política por defecto ────────────────────────
echo "[UFW] Manteniendo configuración global y servicios de terceros intactos..."
# Eliminamos el reset y las politicas por defecto para no romper RDP/Zabbix

# ── PASO 2: Reglas específicas del sistema de tráfico (PC1) ─────────────────
# Las reglas de UFW se leen en orden. Primero permitimos la IP correcta,
# luego denegamos el resto del tráfico a ese puerto específico.

# Puerto 5559 — XSUB Broker: solo sensores locales (misma máquina)
echo "[UFW] Protegiendo Puerto 5559 (XSUB Broker) → solo localhost..."
ufw allow from 127.0.0.1 to any port 5559 proto tcp comment "XSUB Broker - permite localhost"
ufw deny to any port 5559 proto tcp comment "XSUB Broker - bloquea resto"

# Puerto 5560 — XPUB Broker: PC2 se suscribe a los eventos
echo "[UFW] Protegiendo Puerto 5560 (XPUB Broker) → solo PC2 ($PC2_IP)..."
ufw allow from "$PC2_IP" to any port 5560 proto tcp comment "XPUB Broker - permite PC2"
ufw deny to any port 5560 proto tcp comment "XPUB Broker - bloquea resto"

# Puerto 5565 — Heartbeat REP: PC2 verifica el estado de PC1
echo "[UFW] Protegiendo Puerto 5565 (Heartbeat REP) → solo PC2 ($PC2_IP)..."
ufw allow from "$PC2_IP" to any port 5565 proto tcp comment "Heartbeat REP - permite PC2"
ufw deny to any port 5565 proto tcp comment "Heartbeat REP - bloquea resto"

# ── PASO 3: Habilitar UFW ───────────────────────────────────────────────────
echo ""
echo "[UFW] Habilitando firewall..."
ufw --force enable

# ── RESUMEN ─────────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo "  UFW activo en PC1 — Reglas aplicadas:"
echo "============================================================"
ufw status verbose
echo ""
echo "  Tráfico bloqueado por defecto:"
echo "    • Cualquier IP no autorizada intentando acceder a los puertos 5559, 5560 y 5565."
echo "  [NOTA] Los demás puertos (RDP, Zabbix, SSH) NO han sido modificados."
echo ""
echo "  [OK] Configuración de firewall completada en PC1."
echo ""
