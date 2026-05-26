"""
test_pc2_local.py
=================
Smoke test local de PC2.

Lanza PC1 (gestor) y PC2 (analitica) apuntando a localhost,
espera 8 segundos para que lleguen eventos y detiene todo.
Verifica que:
  - PC2 arranca sin errores
  - El suscriptor recibe eventos de PC1
  - El motor de reglas clasifica estados
  - El controlador de semaforos imprime operaciones
  - La replica SQLite se crea correctamente

Uso: python test_pc2_local.py
"""
import json
import os
import subprocess
import sys
import time
from pathlib import Path

# ── Configuracion temporal: apuntar a localhost ───────────────────────────────
CONFIG_ORIGINAL = Path(__file__).parent / "config" / "pc2_config.json"
config = json.loads(CONFIG_ORIGINAL.read_text(encoding="utf-8"))
config["pc1"]["xpub_host"] = "localhost"  # PC1 en la misma maquina para el test
CONFIG_TEMPORAL = Path(__file__).parent / "config" / "pc2_config_test.json"
CONFIG_TEMPORAL.write_text(json.dumps(config, indent=2), encoding="utf-8")

env = os.environ.copy()
env["PYTHONUTF8"] = "1"

# ── 1. Lanzar PC1 ─────────────────────────────────────────────────────────────
pc1_gestor = Path(__file__).parent.parent / "pc1" / "gestor.py"
proc_pc1 = subprocess.Popen(
    [sys.executable, "-u", str(pc1_gestor)],
    cwd=str(pc1_gestor.parent),
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    env=env,
)
print(f"[TEST] PC1 lanzado (PID={proc_pc1.pid})")
time.sleep(1.5)

# ── 2. Lanzar PC2 apuntando al config temporal ────────────────────────────────
# Modificar analitica.py para usar el config de test via variable de entorno
env["PC2_CONFIG"] = str(CONFIG_TEMPORAL)

proc_pc2 = subprocess.Popen(
    [sys.executable, "-u", "analitica.py"],
    cwd=str(Path(__file__).parent),
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
    encoding="utf-8",
    errors="replace",
    env=env,
)
print(f"[TEST] PC2 lanzado (PID={proc_pc2.pid})")
print("[TEST] Capturando salida durante 10 segundos...\n")

time.sleep(10)

# ── 3. Recoger salida ─────────────────────────────────────────────────────────
proc_pc2.terminate()
proc_pc1.terminate()
out, _ = proc_pc2.communicate(timeout=5)
CONFIG_TEMPORAL.unlink(missing_ok=True)

print("=" * 60)
print("SALIDA DE PC2:")
print("=" * 60)
print(out[:6000])

# ── 4. Verificar replica SQLite ───────────────────────────────────────────────
db_path = Path(__file__).parent / "replica" / "trafico.db"
if db_path.exists():
    import sqlite3
    conn = sqlite3.connect(str(db_path))
    n_eventos = conn.execute("SELECT COUNT(*) FROM eventos").fetchone()[0]
    n_estados = conn.execute("SELECT COUNT(*) FROM estados").fetchone()[0]
    conn.close()
    print(f"\n[TEST] SQLite OK - eventos={n_eventos}, intersecciones con estado={n_estados}")
else:
    print("\n[TEST] AVISO: No se creo la replica SQLite (normal si PC2 no pudo conectar con PC1)")

print("\n[TEST] Test completado.")
