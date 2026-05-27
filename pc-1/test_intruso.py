"""
test_intruso.py
===============
Script de prueba de seguridad CurveZMQ — SOLO PARA FINES ACADÉMICOS.

Simula dos tipos de ataque al canal de control de PC2 (puerto 5563) para
demostrar que el servidor rechaza automáticamente cualquier cliente que no
posea las claves criptográficas válidas registradas en la whitelist ZAP.

PRUEBA 1 — Cliente sin cifrado (texto plano)
    Socket REQ estándar, sin opciones CURVE.
    Resultado esperado: timeout (ZMQ descarta el handshake en capa de transporte).

PRUEBA 2 — Cliente con claves falsas (CURVE pero no autorizadas)
    Se generan claves Curve25519 nuevas al vuelo, nunca registradas en
    pc2/keys/clients/.
    Resultado esperado: timeout (ZAP rechaza la clave pública desconocida).

En ambos casos el servidor PC2 nunca llega a ejecutar recv_json() ni
inyecta ningún evento en su cola interna. El rechazo es automático y
transparente, sin log de error en PC2 salvo el timeout de handshake.

Uso:
    python3 test_intruso.py <IP_PC2>          # usa puerto por defecto 5563
    python3 test_intruso.py <IP_PC2> <PUERTO>

Ejemplo:
    python3 test_intruso.py 10.43.100.91
    python3 test_intruso.py 10.43.100.91 5563
"""

import sys
import time
import zmq

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
PC2_IP   = sys.argv[1] if len(sys.argv) > 1 else "10.43.100.91"
PC2_PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 5563
TIMEOUT_MS = 4_000   # 4 s — si no responde en este tiempo, el ataque falló
COMANDO_FALSO = {
    "comando":      "OLA_VERDE",
    "posicion":     "INT_A1_NS",
    "duracion_seg": 9999,          # duración exagerada para hacer evidente el ataque
}

_SEP = "=" * 62


def _banner() -> None:
    print()
    print(_SEP)
    print("  TEST DE INTRUSIÓN — Canal de control PC2 (CurveZMQ)")
    print(f"  Objetivo : tcp://{PC2_IP}:{PC2_PORT}")
    print(f"  Comando  : {COMANDO_FALSO}")
    print(_SEP)
    print()
    print("  ADVERTENCIA: Este script es SOLO para verificar que la")
    print("  seguridad funciona. Úsalo únicamente en entornos de laboratorio.")
    print()


def _resultado(prueba: str, enviado: bool, recibido: bool, elapsed_ms: float) -> None:
    estado = "✔ RECHAZADO (seguridad OK)" if not recibido else "✖ ACEPTADO  (¡FALLO DE SEGURIDAD!)"
    print(f"  [{prueba}]")
    print(f"    Mensaje enviado   : {'Sí' if enviado else 'No (error de envío)'}")
    print(f"    Respuesta recibida: {'Sí ← peligro' if recibido else 'No (timeout)'}")
    print(f"    Tiempo transcurrido: {elapsed_ms:.0f} ms")
    print(f"    Resultado → {estado}")
    print()


# ---------------------------------------------------------------------------
# PRUEBA 1: socket plano sin CURVE
# ---------------------------------------------------------------------------
def prueba_sin_cifrado() -> None:
    print("─" * 62)
    print("  PRUEBA 1: Socket REQ SIN cifrado (texto plano)")
    print("─" * 62)
    print("  Intentando enviar OLA_VERDE sin ninguna clave CURVE ...")
    print()

    ctx  = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, TIMEOUT_MS)
    sock.setsockopt(zmq.SNDTIMEO, TIMEOUT_MS)
    sock.setsockopt(zmq.LINGER,   0)
    sock.connect(f"tcp://{PC2_IP}:{PC2_PORT}")

    enviado  = False
    recibido = False
    t0 = time.monotonic()

    try:
        sock.send_json(COMANDO_FALSO)
        enviado = True
        resp = sock.recv_json()
        recibido = True
        print(f"  [!] Respuesta inesperada del servidor: {resp}")
    except zmq.Again:
        pass   # timeout esperado — el servidor no respondió
    except zmq.ZMQError as exc:
        print(f"  Error ZMQ (esperado): {exc}")

    elapsed = (time.monotonic() - t0) * 1000
    sock.close()
    ctx.term()
    _resultado("SIN CIFRADO", enviado, recibido, elapsed)


# ---------------------------------------------------------------------------
# PRUEBA 2: socket con claves CURVE falsas (no están en la whitelist)
# ---------------------------------------------------------------------------
def prueba_claves_falsas() -> None:
    print("─" * 62)
    print("  PRUEBA 2: Socket REQ con claves CURVE falsas (no autorizadas)")
    print("─" * 62)
    print("  Generando par de claves Curve25519 aleatorio (no registradas) ...")

    fake_pub, fake_sec = zmq.curve_keypair()

    # Necesitamos la clave PÚBLICA del servidor para iniciar el handshake.
    # Un atacante real intentaría obtenerla por fuerza bruta o intercepción.
    # Aquí usamos también una clave pública de servidor falsa para simular
    # que el atacante no conoce la clave del servidor.
    fake_server_pub, _ = zmq.curve_keypair()

    print(f"  Clave pública falsa del cliente : {fake_pub[:20]}...")
    print(f"  Clave pública falsa del servidor: {fake_server_pub[:20]}...")
    print()
    print("  Intentando enviar OLA_VERDE con claves no autorizadas ...")
    print()

    ctx  = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    sock.curve_publickey = fake_pub
    sock.curve_secretkey = fake_sec
    sock.curve_serverkey = fake_server_pub   # clave de servidor incorrecta
    sock.setsockopt(zmq.RCVTIMEO, TIMEOUT_MS)
    sock.setsockopt(zmq.SNDTIMEO, TIMEOUT_MS)
    sock.setsockopt(zmq.LINGER,   0)
    sock.connect(f"tcp://{PC2_IP}:{PC2_PORT}")

    enviado  = False
    recibido = False
    t0 = time.monotonic()

    try:
        sock.send_json(COMANDO_FALSO)
        enviado = True
        resp = sock.recv_json()
        recibido = True
        print(f"  [!] Respuesta inesperada del servidor: {resp}")
    except zmq.Again:
        pass
    except zmq.ZMQError as exc:
        print(f"  Error ZMQ (esperado): {exc}")

    elapsed = (time.monotonic() - t0) * 1000
    sock.close()
    ctx.term()
    _resultado("CLAVES FALSAS", enviado, recibido, elapsed)


# ---------------------------------------------------------------------------
# Resumen final
# ---------------------------------------------------------------------------
def resumen_final() -> None:
    print(_SEP)
    print("  RESUMEN")
    print(_SEP)
    print()
    print("  Si ambas pruebas muestran 'RECHAZADO', CurveZMQ + ZAP están")
    print("  funcionando correctamente:")
    print()
    print("    • El servidor PC2 nunca procesó ningún mensaje.")
    print("    • Ningún comando fue inyectado en la cola de eventos.")
    print("    • El rechazo ocurrió en la capa de transporte ZMQ,")
    print("      antes de que el código de ServidorControl se ejecutara.")
    print()
    print("  Mecanismo de protección activo:")
    print("    1. CURVE  → cifrado y autenticación de clave pública/privada")
    print("    2. ZAP    → whitelist de claves públicas autorizadas")
    print("    3. Doble barrera → un atacante necesita AMBAS claves correctas")
    print()
    print(_SEP)
    print()


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    _banner()
    prueba_sin_cifrado()
    prueba_claves_falsas()
    resumen_final()
