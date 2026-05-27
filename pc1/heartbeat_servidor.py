"""
pc1/heartbeat_servidor.py
=========================
Hilo servidor de Heartbeat del PC1.

Escucha en tcp://*:<heartbeat.rep_port> (REP, puerto 5565) y responde
a los pings enviados por el HeartbeatCliente de PC2, permitiendo que PC2
sepa con anticipación si la fuente de datos está operativa.

Cada PONG informa:
  - broker_ok        : si el Broker ZMQ sigue vivo
  - sensores_activos : cuántos hilos de sensor están corriendo en este momento
  - sensores_total   : total de sensores al arranque (constante)
  - uptime_s         : segundos transcurridos desde el inicio del servidor
  - timestamp        : marca de tiempo ISO-8601 con zona horaria local

Compatibilidad con el broker:
  - Acepta un objeto BrokerMultihilo (expone .esta_vivo())
  - Acepta un threading.Thread clásico (expone .is_alive())
  - Acepta None (broker_ok = False)

Esto permite a PC2 distinguir entre tres estados:
  1. PC1 completamente caído      → sin respuesta (timeout de socket)
  2. PC1 vivo, broker caído       → broker_ok=False
  3. PC1 vivo, broker OK          → broker_ok=True, con conteo de sensores
"""

import threading
import time
from datetime import datetime, timezone

import zmq


class HeartbeatServidor(threading.Thread):
    """
    Hilo REP que responde pings de PC2 con el estado de PC1.

    Parámetros
    ----------
    config : dict
        Configuración de ciudad.json.  Lee ``config["heartbeat"]["rep_port"]``.
    hilos_sensores : list[threading.Thread]
        Lista de hilos de sensor lanzados por gestor.py.
    hilo_broker : threading.Thread | None
        Referencia al hilo del broker ZMQ para verificar si está vivo.
    stop_event : threading.Event
        Evento de parada compartido con todos los hilos del gestor.
    """

    def __init__(
        self,
        config: dict,
        hilos_sensores: list,
        hilo_broker,  # BrokerMultihilo | threading.Thread | None
        stop_event: threading.Event,
    ) -> None:
        super().__init__(name="HB-Servidor-PC1", daemon=True)
        self.config         = config
        self.hilos_sensores = hilos_sensores
        self.hilo_broker    = hilo_broker
        self.stop_event     = stop_event
        self._inicio        = time.time()

    # ------------------------------------------------------------------

    @staticmethod
    def _ts() -> str:
        return datetime.now(timezone.utc).astimezone().isoformat(timespec="microseconds")

    def _construir_pong(self) -> dict:
        # Compatibilidad con BrokerMultihilo (.esta_vivo()) y
        # threading.Thread (.is_alive()), con fallback a False si es None.
        broker = self.hilo_broker
        if broker is None:
            broker_ok = False
        elif hasattr(broker, "esta_vivo"):
            broker_ok = broker.esta_vivo()          # BrokerMultihilo
        else:
            broker_ok = broker.is_alive()           # threading.Thread clasico
        sensores_activos = sum(1 for h in self.hilos_sensores if h.is_alive())
        return {
            "tipo":             "pong",
            "broker_ok":        broker_ok,
            "sensores_activos": sensores_activos,
            "sensores_total":   len(self.hilos_sensores),
            "uptime_s":         int(time.time() - self._inicio),
            "timestamp":        self._ts(),
        }

    # ------------------------------------------------------------------

    def run(self) -> None:
        ctx  = zmq.Context()
        sock = ctx.socket(zmq.REP)
        port = self.config["heartbeat"]["rep_port"]
        sock.bind(f"tcp://*:{port}")
        sock.setsockopt(zmq.RCVTIMEO, 1000)  # 1 s para verificar stop_event

        print(f"[HB-PC1] Servidor heartbeat REP escuchando en tcp://*:{port}")

        while not self.stop_event.is_set():
            try:
                msg = sock.recv_json()
                if msg.get("tipo") == "ping":
                    sock.send_json(self._construir_pong())
                else:
                    sock.send_json({
                        "tipo":    "error",
                        "mensaje": f"tipo desconocido: '{msg.get('tipo')}'",
                    })
            except zmq.Again:
                continue
            except zmq.ZMQError as exc:
                if not self.stop_event.is_set():
                    print(f"[HB-PC1] Error ZMQ: {exc}")
            except Exception as exc:
                if not self.stop_event.is_set():
                    print(f"[HB-PC1] Error inesperado: {exc}")
                    try:
                        sock.send_json({"tipo": "error", "mensaje": str(exc)})
                    except Exception:
                        pass

        sock.close()
        ctx.term()
        print("[HB-PC1] Servidor heartbeat detenido.")
