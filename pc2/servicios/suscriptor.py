"""
servicios/suscriptor.py
=======================
Hilo suscriptor ZMQ (patron SUB).

Se conecta al Broker XPUB del PC1 (10.43.99.192:5560) y reenvía
cada evento recibido a la cola interna para que el motor de reglas
lo procese. Usa RCVTIMEO para poder verificar el stop_event
periódicamente sin bloquearse indefinidamente.
"""

import json
import queue
import threading

import zmq


class Suscriptor(threading.Thread):
    """
    Hilo suscriptor ZMQ que ingesta eventos del broker de PC1.

    Parametros
    ----------
    config : dict
        Configuracion general de PC2 (contiene pc1.xpub_host y pc1.xpub_port).
    cola_eventos : queue.Queue
        Cola compartida donde se depositan los eventos deserializados.
    stop_event : threading.Event
        Evento de parada compartido con todos los servicios.
    """

    def __init__(self, config: dict, cola_eventos: queue.Queue,
                 stop_event: threading.Event) -> None:
        super().__init__(name="Suscriptor-ZMQ", daemon=True)
        self.config      = config
        self.cola        = cola_eventos
        self.stop_event  = stop_event

    def run(self) -> None:
        ctx  = zmq.Context()
        sock = ctx.socket(zmq.SUB)

        host = self.config["pc1"]["xpub_host"]
        port = self.config["pc1"]["xpub_port"]
        sock.connect(f"tcp://{host}:{port}")

        # Suscribirse a TODOS los topicos (camara.*, espira.*, gps.*)
        sock.setsockopt_string(zmq.SUBSCRIBE, "")

        # Timeout para que el hilo no quede bloqueado al parar el sistema
        sock.setsockopt(zmq.RCVTIMEO, 1000)

        print(f"[SUSCRIPTOR] Conectado a PC1 tcp://{host}:{port}")

        while not self.stop_event.is_set():
            try:
                topico_bytes, payload_bytes = sock.recv_multipart()
                evento = json.loads(payload_bytes.decode("utf-8"))
                # Aniadir metadato del topico para el motor de reglas
                evento["_topico"] = topico_bytes.decode("utf-8")
                evento["_fuente"] = "sensor"
                self.cola.put_nowait(evento)
            except zmq.Again:
                # Timeout normal, verificar stop_event y continuar
                continue
            except json.JSONDecodeError as e:
                print(f"[SUSCRIPTOR] JSON invalido ignorado: {e}")
            except Exception as e:
                if not self.stop_event.is_set():
                    print(f"[SUSCRIPTOR] Error inesperado: {e}")

        sock.close()
        ctx.term()
        print("[SUSCRIPTOR] Detenido.")
