"""
pc2/analitica.py
================
Punto de entrada del PC2 -- Servicio de Analitica de Trafico.

Lanza y coordina cinco hilos independientes:
  1. Suscriptor       - SUB <- PC1 XPUB 10.43.99.192:5560
  2. Motor de Reglas  - aplica umbrales, detecta ambulancias, clasifica estado
  3. Ctrl. Semaforos  - imprime cada operacion de semaforo con timestamp
  4. Persistencia     - PUSH -> PC3 10.43.99.78:5561 + SQLite replica local
  5. Servidor Control - REP :5563 <- comandos manuales de PC3

La comunicacion entre hilos se realiza mediante colas de Python (queue.Queue)
para evitar condiciones de carrera y mantener el desacoplamiento temporal.

Uso:
  python3 analitica.py
"""

import json
import os
import queue

import signal
import sys
import threading
import time
from pathlib import Path

from servicios import (
    ControlSemaforos,
    MotorReglas,
    Persistencia,
    ServidorControl,
    Suscriptor,
)

# ---------------------------------------------------------------------------
BASE_DIR    = Path(__file__).parent
CONFIG_PATH = Path(os.environ.get("PC2_CONFIG", str(BASE_DIR / "config" / "pc2_config.json")))

# ---------------------------------------------------------------------------


def cargar_config() -> dict:
    """Lee y valida el archivo de configuracion pc2_config.json."""
    if not CONFIG_PATH.exists():
        print(f"[ERROR] No se encontro la configuracion: {CONFIG_PATH}")
        sys.exit(1)
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _banner(config: dict) -> None:
    u = config["umbrales"]
    s = config["semaforo"]
    print()
    print("=" * 60)
    print("  PC2 -- Servicio de Analitica de Trafico Urbano")
    print("=" * 60)
    print()
    print(f"  PC1 (fuente)  : tcp://{config['pc1']['xpub_host']}:{config['pc1']['xpub_port']}")
    print(f"  PC3 (destino) : tcp://{config['pc3']['host']}:{config['pc3']['push_port']}")
    print(f"  Control REP   : tcp://*:{config['servidor_comandos']['rep_port']}")
    print()
    print("  Umbrales de congestion:")
    print(f"    Cola max         : {u['cola_max']} vehiculos")
    print(f"    Velocidad min    : {u['velocidad_min_kmh']} km/h")
    print(f"    Densidad max     : {u['densidad_max']}")
    print(f"    Velocidad ambul. : {u['velocidad_ambulancia_kmh']} km/h")
    print()
    print("  Tiempos de semaforo:")
    print(f"    NORMAL           : {s['tiempo_verde_normal_seg']}s verde")
    print(f"    CONGESTION       : {s['tiempo_verde_congestion_seg']}s verde (extendido)")
    print(f"    OLA_VERDE        : {s['tiempo_ola_verde_seg']}s verde continuo")
    print()


def main() -> None:
    config = cargar_config()
    _banner(config)

    # Colas de comunicacion inter-hilo
    cola_eventos      = queue.Queue()
    cola_semaforos    = queue.Queue()
    cola_persistencia = queue.Queue()

    # Evento de parada compartido entre todos los hilos
    stop_event = threading.Event()

    # Instanciar los cinco servicios
    servicios: list[threading.Thread] = [
        Suscriptor(config, cola_eventos, stop_event),
        MotorReglas(config, cola_eventos, cola_semaforos, cola_persistencia, stop_event),
        ControlSemaforos(config, cola_semaforos, stop_event),
        Persistencia(config, cola_persistencia, stop_event),
        ServidorControl(config, cola_eventos, stop_event),
    ]

    # Lanzar todos los hilos
    for srv in servicios:
        srv.start()

    print("-" * 60)
    print("[PC2] Todos los servicios activos. Ctrl+C para detener.")
    print("-" * 60)
    print()

    # Manejador de apagado limpio
    def _apagar(sig, frame):  # noqa: ANN001
        print("\n[PC2] Apagando servicios...")
        stop_event.set()
        for srv in servicios:
            srv.join(timeout=8)
        activos = sum(1 for s in servicios if s.is_alive())
        if activos:
            print(f"[PC2] Advertencia: {activos} servicio(s) no terminaron a tiempo.")
        print("[PC2] PC2 detenido correctamente.")
        sys.exit(0)

    signal.signal(signal.SIGINT,  _apagar)
    signal.signal(signal.SIGTERM, _apagar)

    # Hilo de monitoreo: imprime estado cada 60s
    def _monitor():
        while not stop_event.is_set():
            stop_event.wait(timeout=60)
            if stop_event.is_set():
                break
            print(f"\n[MONITOR] {time.strftime('%H:%M:%S')} | "
                  f"Colas - eventos:{cola_eventos.qsize()} "
                  f"semaforos:{cola_semaforos.qsize()} "
                  f"persistencia:{cola_persistencia.qsize()}")

    threading.Thread(target=_monitor, name="Monitor", daemon=True).start()

    # Mantener el hilo principal vivo
    while not stop_event.is_set():
        time.sleep(1)


if __name__ == "__main__":
    main()
