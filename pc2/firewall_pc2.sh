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
#  │ Puerto │ Descripción                                           │ Fuente permitida │
#  ├──────┼───────────────────────────────────────────────────────────┤
#  │ 22    │ SSH (administración)                                  │ Cualquier IP     │
#  │ 5562  │ PUSH receptor (datos de PC1)                           │ 10.43.99.192     │
#  │ 5563  │ Control REP CurveZMQ (órdenes PC3)                    │ 10.43.99.78      │
#  │ 5564  │ Consulta REP failover (réplica → PC3)                 │ 10.43.99.78      │
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
echo " Configurando UFW — PC2 ($PC2_IP)"
echo "============================================================"
echo ""

# Verificar que se ejecuta como root
if [[ $EUID -ne 0 ]]; then
    echo "[ERROR] Este script requiere privilegios de root."
    echo "        Usa: sudo bash firewall_pc2.sh"
    exit 1
fi

# Asegurar que UFW está instalado
if ! command -v ufw &> /dev/null; then
    echo "[INFO] Instalando UFW..."
    apt-get update && apt-get install -y ufw
fi

# ── PASO 1: Asegurar acceso SSH ─────────────────────────────────────────────
# Prevenir bloqueos accidentales en la administración remota de PC2
echo "[UFW] Asegurando puerto SSH (22)..."
ufw allow 22/tcp comment "SSH - administracion"

# ── PASO 2: Limpieza preventiva de reglas viejas de ZeroMQ ──────────────────
# Eliminamos cualquier regla previa en estos puertos para evitar redundancias
echo "[UFW] Limpiando reglas previas de los puertos de PC2..."
ufw delete allow 5562/tcp || true
ufw delete deny 5562/tcp || true
ufw delete allow 5563/tcp || true
ufw delete deny 5563/tcp || true
ufw delete allow 5564/tcp || true
ufw delete deny 5564/tcp || true

# ── PASO 3: Inyección de reglas en orden estricto (Top-Down) ────────────────
# Usamos 'insert 1'. Al igual que antes, las inyectamos en orden inverso
# para que los ALLOW queden empujados encima de sus respectivos DENY.

# --- Puerto 5564: Consulta REP failover (PC3) ---
echo "[UFW] Configurando Puerto 5564 (Consulta REP failover) para PC3..."
ufw insert 1 deny to any port 5564 proto tcp comment "Consulta failover - bloquea resto"
ufw insert 1 allow from "$PC3_IP" to any port 5564 proto tcp comment "Consulta failover - permite PC3"

# --- Puerto 5563: Control REP CurveZMQ (PC3) ---
echo "[UFW] Configurando Puerto 5563 (Control REP CurveZMQ) para PC3..."
ufw insert 1 deny to any port 5563 proto tcp comment "Control REP - bloquea resto"
ufw insert 1 allow from "$PC3_IP" to any port 5563 proto tcp comment "Control REP - permite PC3"

# --- Puerto 5562: PULL receptor datos (PC1) ---
echo "[UFW] Configurando Puerto 5562 (PULL receptor datos) para PC1..."
ufw insert 1 deny to any port 5562 proto tcp comment "PULL datos - bloquea resto"
ufw insert 1 allow from "$PC1_IP" to any port 5562 proto tcp comment "PULL datos - permite PC1"


# ── PASO 4: Habilitar UFW ───────────────────────────────────────────────────
echo ""
echo "[UFW] Aplicando cambios y habilitando firewall..."
ufw --force enable

# ── RESUMEN ─────────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo "  UFW activo en PC2 — Reglas aplicadas ordenadas:"
echo "============================================================"
ufw status numbered
echo ""
echo "  [OK] Configuración del Gestor Inteligente de Tráfico completada en PC2."
echo ""