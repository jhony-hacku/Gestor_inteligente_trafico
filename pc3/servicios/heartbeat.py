"""
servicios/heartbeat.py
======================
Hilo servidor de Heartbeat del PC3.

Escucha en tcp://*:<heartbeat.rep_port> (REP, puerto 5566) y responde
a los pings enviados por el HeartbeatCliente de PC2.

En cada PONG informa:
  - db_ok    : True si trafico_principal.db responde (SELECT COUNT(*))
  - db_ruta  : ruta relativa de la base de datos
  - eventos  : total de filas en la tabla eventos (-1 si la DB falla)
  - uptime_s : segundos desde que se inició el servidor
  - timestamp: marca ISO-8601 con zona horaria local

Esto permite a PC2 distinguir entre:
  1. PC3 completamente caído          → sin respuesta (timeout de socket)
  2. PC3 vivo, DB no accesible        → db_ok=False
  3. PC3 vivo, DB OK                  → db_ok=True con conteo de eventos
"""

import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import zmq


class HeartbeatServidor(threading.Thread):
    """
    Hilo REP que responde pings de PC2 con el estado de PC3.

    Parámetros
    ----------
    config : dict
        Configuración de PC3.  Lee ``heartbeat.rep_port`` y ``db.ruta``.
    stop_event : threading.Event
        Evento de parada compartido con los demás servicios de PC3.
    """

    def __init__(self, config: dict, stop_event: threading.Event) -> None:
        super().__init__(name="HB-Servidor-PC3", daemon=True)
        self.config     = config
        self.stop_event = stop_event
        self._inicio    = time.time()

    # ------------------------------------------------------------------

    @staticmethod
    def _ts() -> str:
        return datetime.now(timezone.utc).astimezone().isoformat(timespec="microseconds")

    def _ruta_db(self) -> Path:
        base = Path(__file__).parent.parent
        return base / self.config["db"]["ruta"]

    def _chequear_db(self) -> tuple[bool, int]:
        """
        Verifica la disponibilidad de la DB principal.
        Retorna (db_ok, total_eventos).
        """
        ruta = self._ruta_db()
        if not ruta.exists():
            return False, -1
        try:
            conn   = sqlite3.connect(str(ruta), timeout=3)
            total  = conn.execute("SELECT COUNT(*) FROM eventos").fetchone()[0]
            conn.close()
            return True, total
        except Exception:
            return False, -1

    def _construir_pong(self) -> dict:
        db_ok, eventos = self._chequear_db()
        return {
            "tipo":     "pong",
            "db_ok":    db_ok,
            "db_ruta":  self.config["db"]["ruta"],
            "eventos":  eventos,
            "uptime_s": int(time.time() - self._inicio),
            "timestamp": self._ts(),
        }

    # ------------------------------------------------------------------

    def run(self) -> None:
        ctx  = zmq.Context()
        sock = ctx.socket(zmq.REP)
        port = self.config["heartbeat"]["rep_port"]
        sock.bind(f"tcp://*:{port}")
        sock.setsockopt(zmq.RCVTIMEO, 1000)  # 1 s para verificar stop_event

        print(f"[HB-PC3] Servidor heartbeat REP escuchando en tcp://*:{port}")

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
                    print(f"[HB-PC3] Error ZMQ: {exc}")
            except Exception as exc:
                if not self.stop_event.is_set():
                    print(f"[HB-PC3] Error inesperado: {exc}")
                    try:
                        sock.send_json({"tipo": "error", "mensaje": str(exc)})
                    except Exception:
                        pass

        sock.close()
        ctx.term()
        print("[HB-PC3] Servidor heartbeat detenido.")
