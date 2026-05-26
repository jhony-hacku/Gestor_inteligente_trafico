"""
servicios/servidor_control.py
=============================
Hilo servidor de control (patron REQ/REP).

Escucha conexiones entrantes desde PC3 (u otros clientes) en el
puerto 5563. Acepta comandos JSON para inyectar ordenes manuales
al motor de reglas, como una OLA_VERDE forzada en una interseccion.

Protocolo de comando (JSON):
  Peticion:  { "comando": "OLA_VERDE", "posicion": "INT_C3", "duracion_seg": 60 }
  Respuesta: { "status": "ok", "timestamp": "...", "mensaje": "..." }
             { "status": "error", "mensaje": "descripcion del error" }

El servidor inyecta el comando en cola_eventos con _fuente="PC3_MANUAL"
para que el motor de reglas lo procese exactamente igual que si
viniera de un sensor, garantizando trazabilidad y timestamp consistente.
"""

import queue
import threading
from datetime import datetime, timezone

import zmq


class ServidorControl(threading.Thread):
    """
    Servidor REQ/REP que recibe ordenes de PC3.

    Parametros
    ----------
    config : dict
        Configuracion de PC2 (servidor_comandos.rep_port).
    cola_eventos : queue.Queue
        Cola compartida con el motor de reglas (inyeccion de comandos).
    stop_event : threading.Event
        Evento de parada compartido.
    """

    COMANDOS_VALIDOS = {"OLA_VERDE", "CONGESTION", "NORMAL"}

    def __init__(self, config: dict, cola_eventos: queue.Queue,
                 stop_event: threading.Event) -> None:
        super().__init__(name="Servidor-Control", daemon=True)
        self.config      = config
        self.cola_eventos = cola_eventos
        self.stop_event  = stop_event

    @staticmethod
    def _ts() -> str:
        return datetime.now(timezone.utc).astimezone().isoformat(timespec="microseconds")

    def _validar(self, msg: dict) -> str | None:
        """Retorna None si el mensaje es valido, o el mensaje de error."""
        if "comando" not in msg:
            return "Falta campo 'comando'"
        if msg["comando"] not in self.COMANDOS_VALIDOS:
            return f"Comando '{msg['comando']}' desconocido. Validos: {self.COMANDOS_VALIDOS}"
        if "posicion" not in msg:
            return "Falta campo 'posicion' (ej. 'INT_C3')"
        return None

    def run(self) -> None:
        ctx  = zmq.Context()
        sock = ctx.socket(zmq.REP)
        port = self.config["servidor_comandos"]["rep_port"]
        sock.bind(f"tcp://*:{port}")
        sock.setsockopt(zmq.RCVTIMEO, 1000)  # 1s timeout para verificar stop_event

        print(f"[CONTROL] Servidor REP escuchando en tcp://*:{port}")
        print(f"[CONTROL] Comandos aceptados: {self.COMANDOS_VALIDOS}")

        while not self.stop_event.is_set():
            try:
                msg = sock.recv_json()
                ts  = self._ts()

                # Validar
                error = self._validar(msg)
                if error:
                    sock.send_json({"status": "error", "mensaje": error})
                    print(f"[CONTROL] Comando invalido de PC3: {error} | {msg}")
                    continue

                print(f"[CONTROL] {ts[:19]} | Comando de PC3: {msg}")

                # Inyectar como evento interno
                evento_interno = {
                    **msg,
                    "_fuente":  "PC3_MANUAL",
                    "tipo":     "comando",
                    "posicion": msg["posicion"],
                    "timestamp": ts,
                }
                self.cola_eventos.put_nowait(evento_interno)

                respuesta = {
                    "status":    "ok",
                    "timestamp": ts,
                    "mensaje":   (
                        f"Comando {msg['comando']} recibido para "
                        f"{msg['posicion']}. Se aplicara en la proxima evaluacion."
                    ),
                }
                sock.send_json(respuesta)

            except zmq.Again:
                # Timeout normal, revisar stop_event
                continue
            except zmq.ZMQError as e:
                if not self.stop_event.is_set():
                    print(f"[CONTROL] Error ZMQ: {e}")
            except Exception as e:
                if not self.stop_event.is_set():
                    print(f"[CONTROL] Error inesperado: {e}")
                    try:
                        sock.send_json({"status": "error", "mensaje": str(e)})
                    except Exception:
                        pass

        sock.close()
        ctx.term()
        print("[CONTROL] Servidor de control detenido.")
