"""
pc3/servidor_db.py
==================
Punto de entrada del PC3 -- Servidor de Base de Datos Principal.

Responsabilidades:
  1. Recibir eventos procesados de PC2 via ZMQ PULL (puerto 5561)
  2. Almacenar en la base de datos SQLite principal (db/trafico_principal.db)
     - Tabla eventos   : historico completo
     - Tabla estados   : ultimo estado por interseccion (upsert)
     - Tabla semaforos : registro de cada cambio de semaforo
  3. Monitorear el estado de la ciudad y reportar periodicamente
  4. Permitir enviar comandos manuales a PC2 (OLA_VERDE, NORMAL)
     via REQ -> PC2 REP (puerto 5563)

Hilo principal:
  Escucha comandos interactivos por consola (stdin).

Hilos secundarios:
  Receptor    - PULL :5561    <- eventos de PC2
  Almacenador - escribe en SQLite desde la cola interna
  Monitor     - reporte periodico + conexion REQ a PC2

Uso:
  python3 servidor_db.py
"""

import json
import os
import queue
import signal
import sys
import threading
import time
from pathlib import Path

from servicios import Almacenador, MonitorComandos, Receptor

# ---------------------------------------------------------------------------
BASE_DIR    = Path(__file__).parent
CONFIG_PATH = Path(os.environ.get(
    "PC3_CONFIG",
    str(BASE_DIR / "config" / "pc3_config.json")
))
# ---------------------------------------------------------------------------


def cargar_config() -> dict:
    if not CONFIG_PATH.exists():
        print(f"[ERROR] No se encontro la configuracion: {CONFIG_PATH}")
        sys.exit(1)
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _banner(config: dict) -> None:
    print()
    print("=" * 60)
    print("  PC3 -- Servidor de Base de Datos Principal")
    print("=" * 60)
    print()
    print(f"  Escuchando de PC2 : tcp://*:{config['receptor']['pull_port']}")
    print(f"  PC2 comandos      : tcp://{config['pc2']['host']}:{config['pc2']['rep_port']}")
    db_path = BASE_DIR / config["db"]["ruta"]
    print(f"  Base de datos     : {db_path}")
    print()


def main() -> None:
    config    = cargar_config()
    _banner(config)

    # Cola de comunicacion Receptor -> Almacenador
    cola_db    = queue.Queue()
    stop_event = threading.Event()

    # Instanciar servicios
    servicios: list[threading.Thread] = [
        Receptor(config, cola_db, stop_event),
        Almacenador(config, cola_db, stop_event),
        MonitorComandos(config, stop_event),
    ]

    for srv in servicios:
        srv.start()

    print("-" * 60)
    print("[PC3] Servicios activos. Escribe 'ayuda' para ver comandos.")
    print("-" * 60)
    print()

    # Manejador de apagado limpio (Ctrl+C)
    def _apagar(sig, frame):  # noqa: ANN001
        print("\n[PC3] Apagando servicios...")
        stop_event.set()
        for srv in servicios:
            srv.join(timeout=5)
        print("[PC3] PC3 detenido correctamente.")
        sys.exit(0)

    signal.signal(signal.SIGINT,  _apagar)
    signal.signal(signal.SIGTERM, _apagar)

    # Bucle interactivo en el hilo principal (lee stdin)
    monitor = next(s for s in servicios if isinstance(s, MonitorComandos))
    try:
        while not stop_event.is_set():
            try:
                linea = input("PC3> ").strip()
                if linea:
                    continuar = monitor._procesar_comando_consola(linea)
                    if not continuar:
                        break
            except EOFError:
                # Sin terminal interactiva (ej. redireccion), solo esperar
                time.sleep(1)
    except KeyboardInterrupt:
        pass

    _apagar(None, None)


if __name__ == "__main__":
    main()
