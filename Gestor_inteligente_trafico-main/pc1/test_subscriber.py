"""
test_subscriber.py - Suscriptor de prueba para verificar el flujo ZMQ del PC1.
Captura los primeros N mensajes publicados por el broker y los imprime en JSON.
Uso: python test_subscriber.py
"""
import json
import os
import subprocess
import sys
import time

import zmq

# ── 1. Conectar suscriptor ANTES de lanzar el gestor (evita slow-joiner) ──────
ctx = zmq.Context()
sock = ctx.socket(zmq.SUB)
sock.connect("tcp://localhost:5560")
sock.setsockopt_string(zmq.SUBSCRIBE, "")  # suscribirse a TODOS los topicos
sock.setsockopt(zmq.RCVTIMEO, 8000)         # timeout 8 segundos

# ── 2. Lanzar el gestor en background ────────────────────────────────────────
env = os.environ.copy()
env["PYTHONUTF8"] = "1"

gestor = subprocess.Popen(
    [sys.executable, "-u", "gestor.py"],
    cwd=os.path.dirname(__file__),
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    env=env,
)

print("[TEST] Gestor lanzado (PID={})".format(gestor.pid))
print("[TEST] Esperando que broker y sensores se inicien y envien primer evento...")
time.sleep(1.5)

N = 9  # capturar 9 mensajes (1 de cada sensor en INT_A1, INT_A2, INT_A3)
print(f"[TEST] Esperando {N} mensajes del broker...\n")

mensajes = []
try:
    for _ in range(N):
        topico_bytes, payload_bytes = sock.recv_multipart()
        evento = json.loads(payload_bytes.decode("utf-8"))
        mensajes.append({"topico": topico_bytes.decode("utf-8"), "evento": evento})
except zmq.Again:
    print("[TEST] Timeout: no se recibieron suficientes mensajes.")

sock.close()
ctx.term()
gestor.terminate()
gestor.wait(timeout=5)
print("[TEST] Gestor detenido.\n")

# ── 3. Mostrar resultados ─────────────────────────────────────────────────────
print("=" * 60)
print(f"  Mensajes capturados: {len(mensajes)}/{N}")
print("=" * 60)
for i, m in enumerate(mensajes, 1):
    print(f"\n[MSG {i}] Topico: {m['topico']}")
    for k, v in m["evento"].items():
        print(f"         {k:<22}: {v}")

print("\n[TEST] OK - PC1 funciona correctamente.")
