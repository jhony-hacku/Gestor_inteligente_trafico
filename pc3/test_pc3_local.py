"""
test_pc3_local.py
=================
Smoke test de PC3: verifica que el servidor arranca, crea la BD
y recibe eventos via ZMQ PUSH simulado.
"""
import json
import os
import sys
import time
import sqlite3
import threading
import queue
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
os.environ["PYTHONUTF8"] = "1"

from servicios.receptor import Receptor
from servicios.almacenador import Almacenador

CONFIG = {
    "receptor": {"pull_port": 5561},
    "pc2":      {"host": "localhost", "rep_port": 5563},
    "db":       {"ruta": "db/trafico_test.db"},
}

cola_db    = queue.Queue()
stop_event = threading.Event()

# Lanzar receptor y almacenador
receptor    = Receptor(CONFIG, cola_db, stop_event)
almacenador = Almacenador(CONFIG, cola_db, stop_event)
receptor.start()
almacenador.start()
time.sleep(0.5)

# Simular envio de PC2 via PUSH
import zmq
ctx  = zmq.Context()
sock = ctx.socket(zmq.PUSH)
sock.connect("tcp://localhost:5561")
time.sleep(0.3)

EVENTOS = [
    {"sensor_id":"CAM_A1","tipo":"camara","posicion":"INT_A1",
     "timestamp":"2026-04-07T22:00:00","estado_trafico":"NORMAL",
     "motivo":"Flujo normal","timestamp_proceso":"2026-04-07T22:00:00.1"},
    {"sensor_id":"GPS_B2","tipo":"gps","posicion":"INT_B2",
     "timestamp":"2026-04-07T22:00:01","estado_trafico":"CONGESTION",
     "motivo":"Densidad alta","timestamp_proceso":"2026-04-07T22:00:01.1"},
    {"sensor_id":"CAM_C3","tipo":"camara","posicion":"INT_C3",
     "timestamp":"2026-04-07T22:00:02","estado_trafico":"OLA_VERDE",
     "motivo":"Ambulancia detectada","timestamp_proceso":"2026-04-07T22:00:02.1"},
]

for ev in EVENTOS:
    sock.send(json.dumps(ev).encode("utf-8"))
    time.sleep(0.1)

sock.close()
ctx.term()
time.sleep(3)  # esperar flush del almacenador

stop_event.set()
receptor.join(timeout=5)
almacenador.join(timeout=5)

# Verificar BD
db_path = Path(__file__).parent / "db" / "trafico_test.db"
conn = sqlite3.connect(str(db_path))
n_ev = conn.execute("SELECT COUNT(*) FROM eventos").fetchone()[0]
estados = conn.execute("SELECT posicion, estado_trafico FROM estados ORDER BY posicion").fetchall()
conn.close()

print(f"\n[TEST] Eventos en BD : {n_ev}")
print("[TEST] Estados por interseccion:")
for pos, est in estados:
    print(f"  {pos} -> {est}")

assert n_ev == 3, f"Esperaba 3 eventos, tenia {n_ev}"
assert len(estados) == 3
print("\n[TEST] OK - PC3 funciona correctamente.")

# Limpiar BD de test
db_path.unlink(missing_ok=True)
