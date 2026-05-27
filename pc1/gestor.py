"""
pc1/gestor.py
=============
Gestor Principal del PC1 -- Simulador de Trafico Urbano

Responsabilidades:
  1. Cargar la configuracion de la ciudad desde config/ciudad.json
  2. Generar dinamicamente la lista de sensores a partir de la cuadricula NxM
     (1 Camara + 1 Espira + 1 GPS por interseccion)
  3. Lanzar cada sensor como un hilo independiente (threading.Thread)
  4. Actuar como Broker ZMQ multihilo (XSUB -> XPUB) mediante BrokerMultihilo:
       • Hilo Proxy       — zmq.proxy_steerable (libera GIL en C, maximo throughput)
       • Hilo Coordinador — escucha stop_event y envia TERMINATE al proxy via PAIR inproc
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
from heartbeat_servidor import HeartbeatServidor

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

    Para una cuadricula 5x5 produce 25 intersecciones x 2 direcciones x 3 tipos = 150 sensores.

    Nomenclatura:
      Interseccion -> INT_<LETRA_FILA><NUMERO_COLUMNA>_<DIR>   ej. INT_C3_NS
      Sensor ID   -> <PREFIJO>_<LETRA><NUMERO>_<DIR>           ej. CAM_C3_NS
    """
    filas    = config["ciudad"]["filas"]
    columnas = config["ciudad"]["columnas"]
    tipos    = config["tipos_sensor"]
    letras   = [chr(ord("A") + i) for i in range(filas)]  # A, B, C, D, E
    direcciones = ["NS", "EO"]  # Norte-Sur, Este-Oeste

    sensores: list[dict] = []
    for letra in letras:
        for col in range(1, columnas + 1):
            for direccion in direcciones:
                posicion = f"INT_{letra}{col}_{direccion}"
                for tipo, params in tipos.items():
                    sensor_cfg = {
                        "sensor_id": f"{PREFIJO[tipo]}_{letra}{col}_{direccion}",
                        "tipo":      tipo,
                        "posicion":  posicion,
                        **params,           # frecuencia_seg + rangos especificos
                    }
                    sensores.append(sensor_cfg)
    return sensores


# ==============================================================================
# Broker ZMQ Multihilo -- XSUB / XPUB con proxy_steerable
# ==============================================================================

# High-Water Mark de los sockets del broker (mensajes en buffer antes de descartar).
# Un valor alto amortigua rafagas masivas de los 150 sensores sin perder mensajes.
_BROKER_HWM    = 100_000  # mensajes
_BROKER_LINGER = 0        # ms — cierre inmediato sin esperar drenado pendiente


class BrokerMultihilo:
    """
    Broker ZMQ de alta eficiencia basado en zmq.proxy_steerable.

    Arquitectura interna:
      Hilo Proxy       — ejecuta proxy_steerable(xsub, xpub, control=ctrl)
                         Libera el GIL en C durante el transporte, maximizando
                         el throughput con rafagas concurrentes de 150+ sensores.
      Hilo Coordinador — espera stop_event y envia b"TERMINATE" al socket PAIR
                         de control para detener el proxy de forma determinista,
                         sin necesidad de ctx.term() abrupto.

    Puertos (configurables en ciudad.json):
      XSUB -> xsub_port (5559): los sensores (PUB) se conectan aqui
      XPUB -> xpub_port (5560): PC2 (SUB) se conecta aqui para recibir datos

    Parametros
    ----------
    config : dict
        Configuracion de ciudad.json. Lee broker.xsub_port y broker.xpub_port.
    ctx : zmq.Context
        Contexto ZMQ compartido del proceso.
    stop_event : threading.Event
        Evento de parada coordinada compartido con todos los hilos del gestor.
    """

    # Endpoint inproc del canal de control entre Coordinador y Proxy.
    # Se usa un UUID simple basado en id(self) para evitar colisiones si se
    # instanciara mas de un broker en el mismo proceso.
    _CTRL_EP_TMPL = "inproc://broker-ctrl-{}"

    def __init__(self, config: dict, ctx: zmq.Context,
                 stop_event: threading.Event) -> None:
        self._config     = config
        self._ctx        = ctx
        self._stop_event = stop_event
        self._ctrl_ep    = self._CTRL_EP_TMPL.format(id(self))

        xsub_port = config["broker"]["xsub_port"]
        xpub_port = config["broker"]["xpub_port"]

        # ── Sockets de datos ──────────────────────────────────────────────────
        self._xsub = ctx.socket(zmq.XSUB)
        self._xsub.set_hwm(_BROKER_HWM)
        self._xsub.setsockopt(zmq.LINGER, _BROKER_LINGER)
        self._xsub.bind(f"tcp://*:{xsub_port}")

        self._xpub = ctx.socket(zmq.XPUB)
        self._xpub.set_hwm(_BROKER_HWM)
        self._xpub.setsockopt(zmq.LINGER, _BROKER_LINGER)
        self._xpub.bind(f"tcp://*:{xpub_port}")

        # ── Socket de control (lado coordinador — PAIR dealer) ────────────────
        # Se crea en __init__ para que el bind ocurra antes de que el hilo
        # Proxy intente conectar su extremo.
        self._ctrl_dealer = ctx.socket(zmq.PAIR)
        self._ctrl_dealer.setsockopt(zmq.LINGER, _BROKER_LINGER)
        self._ctrl_dealer.bind(self._ctrl_ep)

        # ── Hilos ─────────────────────────────────────────────────────────────
        self.hilo_proxy = threading.Thread(
            target=self._ejecutar_proxy,
            name="Broker-Proxy",
            daemon=True,
        )
        self.hilo_coordinador = threading.Thread(
            target=self._ejecutar_coordinador,
            name="Broker-Coord",
            daemon=True,
        )

        print(f"  • XSUB escuchando en  tcp://*:{xsub_port}  (entrada sensores)")
        print(f"  • XPUB publicando en  tcp://*:{xpub_port}  (salida -> PC2)")

    # ------------------------------------------------------------------
    # API publica
    # ------------------------------------------------------------------

    def arrancar(self) -> "BrokerMultihilo":
        """Lanza ambos hilos y devuelve self para encadenamiento fluido."""
        self.hilo_proxy.start()
        self.hilo_coordinador.start()
        print("  • Hilo Proxy       arrancado  (Broker-Proxy)")
        print("  • Hilo Coordinador arrancado  (Broker-Coord)")
        return self

    def esta_vivo(self) -> bool:
        """True si el hilo Proxy sigue activo (consultable por HeartbeatServidor)."""
        return self.hilo_proxy.is_alive()

    def detener(self, timeout: float = 5.0) -> None:
        """
        Detiene el broker de forma ordenada.

        El hilo Coordinador ya observa stop_event; este metodo puede llamarse
        explicitamente si se desea forzar la parada sin esperar al event.
        """
        # Si el coordinador aun no envio TERMINATE, forzarlo aqui.
        if self.hilo_proxy.is_alive():
            try:
                self._ctrl_dealer.send(b"TERMINATE", zmq.NOBLOCK)
            except zmq.ZMQError:
                pass  # ya enviado o socket cerrado
        self.hilo_proxy.join(timeout=timeout)
        self.hilo_coordinador.join(timeout=timeout)

    # ------------------------------------------------------------------
    # Implementacion de los hilos
    # ------------------------------------------------------------------

    def _ejecutar_proxy(self) -> None:
        """
        Hilo Proxy: conecta el socket de control (lado proxy — PAIR connect)
        y ejecuta proxy_steerable, que bloquea en C liberando el GIL Python.

        Al recibir b"TERMINATE" en el socket de control, proxy_steerable
        retorna con ZMQError(ETERM) o simplemente con errno==0 segun la
        version de libzmq; ambos casos se capturan y el hilo termina limpio.
        """
        # El extremo connect del canal de control vive en el hilo Proxy.
        ctrl_router = self._ctx.socket(zmq.PAIR)
        ctrl_router.setsockopt(zmq.LINGER, _BROKER_LINGER)
        ctrl_router.connect(self._ctrl_ep)

        try:
            zmq.proxy_steerable(self._xsub, self._xpub, None, ctrl_router)
        except zmq.ZMQError as exc:
            # ETERM: contexto terminado (apagado normal)
            # Cualquier otro error es inesperado.
            if exc.errno not in (zmq.ETERM, 0):
                print(f"[BROKER] Error inesperado en hilo Proxy: {exc}")
        finally:
            ctrl_router.close()
            self._xsub.close()
            self._xpub.close()
            self._ctrl_dealer.close()
            print("[BROKER] Hilo Proxy detenido limpiamente.")

    def _ejecutar_coordinador(self) -> None:
        """
        Hilo Coordinador: bloquea en stop_event y, al activarse, envia
        b"TERMINATE" al socket PAIR de control para detener proxy_steerable
        sin necesidad de ctx.term() abrupto.
        """
        self._stop_event.wait()  # bloqueo eficiente sin busy-wait
        try:
            self._ctrl_dealer.send(b"TERMINATE", zmq.NOBLOCK)
        except zmq.ZMQError:
            pass  # proxy ya detenido o socket cerrado
        print("[BROKER] Hilo Coordinador: TERMINATE enviado al Proxy.")


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

    # ── Broker ZMQ Multihilo ──────────────────────────────────────────────────
    # stop_event se crea antes del broker para que el Coordinador pueda recibirlo.
    stop_event = threading.Event()

    print("[BROKER] BrokerMultihilo iniciando...")
    broker = BrokerMultihilo(config, ctx, stop_event).arrancar()
    # Dar tiempo al broker para que haga bind antes de que los sensores conecten
    time.sleep(0.5)

    # (stop_event ya creado antes del broker para pasarlo al Coordinador)

    # ── Lanzar hilos de sensores ───────────────────────────────────────────────
    print(f"\n[GESTOR] Lanzando {len(lista_sensores)} sensores...\n")
    hilos: list[threading.Thread] = []
    for sensor_cfg in lista_sensores:
        clase = TIPO_A_CLASE[sensor_cfg["tipo"]]
        hilo  = clase(sensor_cfg, config["broker"]["xsub_port"], stop_event, ctx)
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

    # ── Servidor Heartbeat (sondeable por PC2) ────────────────────────────────
    hb_servidor = HeartbeatServidor(config, hilos, broker, stop_event)
    hb_servidor.start()

    print()
    print("-" * 60)
    print("[GESTOR] Sistema activo. Presiona Ctrl+C para detener.")
    print("-" * 60)
    print()

    # ── Manejador de senales (apagado limpio) ──────────────────────────────────
    def _apagar(sig, frame):  # noqa: ANN001
        print("\n\n[GESTOR] Senal de parada recibida. Deteniendo sensores...")
        # Activar stop_event: detiene sensores Y dispara el hilo Coordinador
        # del broker (que enviara TERMINATE al hilo Proxy via PAIR inproc).
        stop_event.set()

        # Esperar a que todos los hilos de sensor terminen (max. 10 s)
        for hilo in hilos:
            hilo.join(timeout=10)

        activos = sum(1 for h in hilos if h.is_alive())
        if activos:
            print(f"[GESTOR] Advertencia: {activos} hilo(s) no terminaron a tiempo.")

        # Esperar cierre ordenado del broker (max. 5 s) — no necesita ctx.term()
        broker.detener(timeout=5.0)
        print("[GESTOR] Broker ZMQ multihilo cerrado.")

        # Terminar el contexto una vez que todos los sockets esten cerrados
        ctx.term()
        print("[GESTOR] PC1 detenido correctamente. ¡Adios!")
        sys.exit(0)

    signal.signal(signal.SIGINT,  _apagar)
    signal.signal(signal.SIGTERM, _apagar)

    # ── Mantener el hilo principal vivo ───────────────────────────────────────
    while not stop_event.is_set():
        time.sleep(1)


if __name__ == "__main__":
    main()
