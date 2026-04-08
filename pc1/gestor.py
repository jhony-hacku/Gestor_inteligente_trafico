"""
pc1/gestor.py
=============
Gestor Principal del PC1 -- Simulador de Trafico Urbano

Responsabilidades:
  1. Cargar la configuracion de la ciudad desde config/ciudad.json
  2. Generar dinamicamente la lista de sensores a partir de la cuadricula NxM
     (1 Camara + 1 Espira + 1 GPS por interseccion)
  3. Lanzar cada sensor como un hilo independiente (threading.Thread)
  4. Actuar como Broker ZMQ (XSUB -> XPUB) para desacoplar sensores y analitica
  5. Mostrar estadisticas periodicas del estado del sistema
  6. Manejar el apagado limpio con Ctrl+C (SIGINT)

Arquitectura de red ZMQ:
  Sensores (PUB)  ->  tcp://localhost:5559  ->  Broker XSUB
  Broker XPUB     ->  tcp://*:5560          ->  PC2 SUB (analitica)

Uso:
  python gestor.py
"""

import io
import json
import signal
import sys
import threading
import time
from pathlib import Path

import zmq

# ─── Codificacion UTF-8 forzada para Windows (evita UnicodeEncodeError cp1252) ─
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from sensores.camara import SensorCamara
from sensores.espira import SensorEspira
from sensores.gps import SensorGPS

# ─── Rutas ────────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config" / "ciudad.json"

# ─── Mapeo tipo -> clase de sensor ─────────────────────────────────────────────
TIPO_A_CLASE: dict = {
    "camara": SensorCamara,
    "espira": SensorEspira,
    "gps":    SensorGPS,
}

# ─── Prefijos cortos para los IDs de sensor ───────────────────────────────────
PREFIJO: dict = {
    "camara": "CAM",
    "espira": "ESP",
    "gps":    "GPS",
}


# ==============================================================================
# Carga de configuracion
# ==============================================================================

def cargar_config() -> dict:
    """Lee y devuelve el archivo de configuracion ciudad.json."""
    if not CONFIG_PATH.exists():
        print(f"[ERROR] No se encontro el archivo de configuracion: {CONFIG_PATH}")
        sys.exit(1)
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


# ==============================================================================
# Generacion dinamica de sensores desde la cuadricula
# ==============================================================================

def generar_lista_sensores(config: dict) -> list[dict]:
    """
    Genera la configuracion de cada sensor a partir de las dimensiones
    de la cuadricula y los parametros por tipo definidos en ciudad.json.

    Para una cuadricula 5x5 produce 25 intersecciones x 3 tipos = 75 sensores.

    Nomenclatura:
      Interseccion -> INT_<LETRA_FILA><NUMERO_COLUMNA>   ej. INT_C3
      Sensor ID   -> <PREFIJO>_<LETRA><NUMERO>           ej. CAM_C3
    """
    filas    = config["ciudad"]["filas"]
    columnas = config["ciudad"]["columnas"]
    tipos    = config["tipos_sensor"]
    letras   = [chr(ord("A") + i) for i in range(filas)]  # A, B, C, D, E

    sensores: list[dict] = []
    for letra in letras:
        for col in range(1, columnas + 1):
            posicion = f"INT_{letra}{col}"
            for tipo, params in tipos.items():
                sensor_cfg = {
                    "sensor_id": f"{PREFIJO[tipo]}_{letra}{col}",
                    "tipo":      tipo,
                    "posicion":  posicion,
                    **params,           # frecuencia_seg + rangos especificos
                }
                sensores.append(sensor_cfg)
    return sensores


# ==============================================================================
# Broker ZMQ -- XSUB / XPUB
# ==============================================================================

def iniciar_broker(config: dict, ctx: zmq.Context) -> threading.Thread:
    """
    Lanza el broker ZMQ en un hilo daemon.

    El broker usa zmq.proxy() que bloquea indefinidamente; por eso corre
    en un hilo separado mientras el hilo principal gestiona los sensores.

    Puertos (configurables en ciudad.json):
      XSUB -> 5559 : los sensores (PUB) se conectan aqui
      XPUB -> 5560 : PC2 (SUB) se conecta aqui para recibir datos
    """
    xsub_port = config["broker"]["xsub_port"]
    xpub_port = config["broker"]["xpub_port"]

    xsub = ctx.socket(zmq.XSUB)
    xsub.bind(f"tcp://*:{xsub_port}")

    xpub = ctx.socket(zmq.XPUB)
    xpub.bind(f"tcp://*:{xpub_port}")

    print(f"  • Broker XSUB escuchando en  tcp://*:{xsub_port}  (entrada sensores)")
    print(f"  • Broker XPUB publicando en  tcp://*:{xpub_port}  (salida -> PC2)")

    def _proxy():
        try:
            zmq.proxy(xsub, xpub)
        except zmq.ZMQError as e:
            # Se dispara cuando el contexto se termina al apagar
            if e.errno != zmq.ETERM:
                print(f"[BROKER] Error inesperado: {e}")
        finally:
            xsub.close()
            xpub.close()

    hilo_broker = threading.Thread(target=_proxy, name="Broker-ZMQ", daemon=True)
    hilo_broker.start()
    return hilo_broker


# ==============================================================================
# Visualizacion periodica de estado
# ==============================================================================

def _banner_ciudad(config: dict) -> None:
    """Imprime la cuadricula ASCII de la ciudad con posiciones de sensores."""
    filas    = config["ciudad"]["filas"]
    columnas = config["ciudad"]["columnas"]
    letras   = [chr(ord("A") + i) for i in range(filas)]

    print()
    print("  +" + "-----+" * columnas)
    for i, letra in enumerate(letras):
        fila = "  |"
        for col in range(1, columnas + 1):
            fila += f" {letra}{col}  |"
        print(fila)
        print("  +" + "-----+" * columnas)
    print()


def _monitor(stop_event: threading.Event, hilos: list[threading.Thread],
             intervalo: int = 60) -> None:
    """
    Hilo de monitorizacion: imprime cada `intervalo` segundos cuantos
    hilos de sensor siguen activos.
    """
    while not stop_event.is_set():
        stop_event.wait(timeout=intervalo)
        if stop_event.is_set():
            break
        activos = sum(1 for h in hilos if h.is_alive())
        total   = len(hilos)
        print(f"\n[MONITOR] Sensores activos: {activos}/{total} -- "
              f"{time.strftime('%H:%M:%S')}")


# ==============================================================================
# Punto de entrada
# ==============================================================================

def main() -> None:
    # ── Cabecera ───────────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("  PC1 -- Gestor Inteligente de Trafico Urbano")
    print("=" * 60)
    print()

    # ── Configuracion ──────────────────────────────────────────────────────────
    config  = cargar_config()
    ciudad  = config["ciudad"]
    print(f"[CONFIG] Ciudad : {ciudad['nombre']}")
    print(f"[CONFIG] Cuadricula : {ciudad['filas']} filas x {ciudad['columnas']} columnas")

    lista_sensores = generar_lista_sensores(config)
    n_camara = sum(1 for s in lista_sensores if s["tipo"] == "camara")
    n_espira = sum(1 for s in lista_sensores if s["tipo"] == "espira")
    n_gps    = sum(1 for s in lista_sensores if s["tipo"] == "gps")
    print(f"[CONFIG] Sensores   : {len(lista_sensores)} total  "
          f"(CAM={n_camara} / ESP={n_espira} / GPS={n_gps})")

    _banner_ciudad(config)

    # ── Contexto ZMQ compartido ────────────────────────────────────────────────
    ctx = zmq.Context()

    # ── Broker ZMQ ────────────────────────────────────────────────────────────
    print("[BROKER] Iniciando...")
    iniciar_broker(config, ctx)
    # Dar tiempo al broker para que haga bind antes de que los sensores conecten
    time.sleep(0.5)

    # ── Stop event compartido ─────────────────────────────────────────────────
    stop_event = threading.Event()

    # ── Lanzar hilos de sensores ───────────────────────────────────────────────
    print(f"\n[GESTOR] Lanzando {len(lista_sensores)} sensores...\n")
    hilos: list[threading.Thread] = []
    for sensor_cfg in lista_sensores:
        clase = TIPO_A_CLASE[sensor_cfg["tipo"]]
        hilo  = clase(sensor_cfg, config["broker"]["xsub_port"], stop_event)
        hilo.start()
        hilos.append(hilo)

    # ── Hilo de monitorizacion ─────────────────────────────────────────────────
    hilo_monitor = threading.Thread(
        target=_monitor,
        args=(stop_event, hilos, 60),
        name="Monitor",
        daemon=True,
    )
    hilo_monitor.start()

    print()
    print("-" * 60)
    print("[GESTOR] Sistema activo. Presiona Ctrl+C para detener.")
    print("-" * 60)
    print()

    # ── Manejador de senales (apagado limpio) ──────────────────────────────────
    def _apagar(sig, frame):  # noqa: ANN001
        print("\n\n[GESTOR] Senal de parada recibida. Deteniendo sensores...")
        stop_event.set()

        # Esperar a que todos los hilos de sensor terminen (max. 10 s)
        for hilo in hilos:
            hilo.join(timeout=10)

        activos = sum(1 for h in hilos if h.is_alive())
        if activos:
            print(f"[GESTOR] Advertencia: {activos} hilo(s) no terminaron a tiempo.")

        # Terminar el contexto ZMQ (esto fuerza el cierre del proxy)
        ctx.term()
        print("[GESTOR] Broker ZMQ cerrado.")
        print("[GESTOR] PC1 detenido correctamente. ¡Adios!")
        sys.exit(0)

    signal.signal(signal.SIGINT,  _apagar)
    signal.signal(signal.SIGTERM, _apagar)

    # ── Mantener el hilo principal vivo ───────────────────────────────────────
    while not stop_event.is_set():
        time.sleep(1)


if __name__ == "__main__":
    main()
