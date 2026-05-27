#!/usr/bin/env bash
# =============================================================================
#  firewall_pc3.sh — Configuración UFW para PC3 (10.43.99.78)
#  Gestor Inteligente de Tráfico Urbano
# =============================================================================
#
#  PC3 es el nodo de alta disponibilidad, persistencia y monitoreo. 
#  Recibe datos consolidados de PC2 y expone su servidor de heartbeat.
#
#  Puertos abiertos en PC3 y quién puede conectarse:
#  ┌──────┬───────────────────────────────────────────────────────────┐
#  │ Puerto │ Descripción                                           │ Fuente permitida │
#  ├──────┼───────────────────────────────────────────────────────────┤
#  │ 22    │ SSH (administración)                                  │ Cualquier IP     │
#  │ 5561  │ Receptor PULL (datos de PC2)                           │ 10.43.100.91     │
#  │ 5566  │ Heartbeat REP (PC2 verifica PC3)                       │ 10.43.100.91     │
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
echo " Configurando UFW — PC3 ($PC3_IP)"
echo "============================================================"
echo ""

# Verificar que se ejecuta como root
if [[ $EUID -ne 0 ]]; then
    echo "[ERROR] Este script requiere privilegios de root."
    echo "        Usa: sudo bash firewall_pc3.sh"
    exit 1
fi

# Asegurar que UFW está instalado
if ! command -v ufw &> /dev/null; then
    echo "[INFO] Instalando UFW..."
    apt-get update && apt-get install -y ufw
fi

# ── PASO 1: Asegurar acceso SSH ─────────────────────────────────────────────
# Prevenir bloqueos accidentales en la administración remota de PC3
echo "[UFW] Asegurando puerto SSH (22)..."
ufw allow 22/tcp comment "SSH - administracion"

# ── PASO 2: Limpieza preventiva de reglas viejas de ZeroMQ ──────────────────
# Eliminamos cualquier regla previa en estos puertos para evitar redundancias o bloqueos fantasma
echo "[UFW] Limpiando reglas previas de los puertos de PC3..."
ufw delete allow 5561/tcp || true
ufw delete deny 5561/tcp || true
ufw delete allow 5566/tcp || true
ufw delete deny 5566/tcp || true

# ── PASO 3: Inyección de reglas en orden estricto (Top-Down) ────────────────
# Usamos 'insert 1'. Inyectamos en orden inverso para que los ALLOW
# queden empujados encima de sus respectivos DENY.

# --- Puerto 5566: Heartbeat REP (PC2) ---
echo "[UFW] Configurando Puerto 5566 (Heartbeat REP) para PC2..."
ufw insert 1 deny to any port 5566 proto tcp comment "Heartbeat REP - bloquea resto"
ufw insert 1 allow from "$PC2_IP" to any port 5566 proto tcp comment "Heartbeat REP - permite PC2"

# --- Puerto 5561: Receptor PULL datos (PC2) ---
echo "[UFW] Configurando Puerto 5561 (PULL receptor datos) para PC2..."
ufw insert 1 deny to any port 5561 proto tcp comment "PULL datos - bloquea resto"
ufw insert 1 allow from "$PC2_IP" to any port 5561 proto tcp comment "PULL datos - permite PC2"


# ── PASO 4: Habilitar UFW ───────────────────────────────────────────────────
echo ""
echo "[UFW] Aplicando cambios y habilitando firewall..."
ufw --force enable

# ── RESUMEN ─────────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo "  UFW activo en PC3 — Reglas aplicadas ordenadas:"
echo "============================================================"
ufw status numbered
echo ""
echo "  [OK] Configuración del Gestor Inteligente de Tráfico completada en PC3."
echo ""