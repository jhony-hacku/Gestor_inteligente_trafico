"""
servicios/receptor.py
=====================
Hilo receptor ZMQ (patron PULL).

Escucha en tcp://*:5561 y recibe los eventos procesados que
PC2 envia via PUSH. Cada mensaje recibido (JSON) es depositado
en la cola interna para que el almacenador los escriba en la BD.

El patron PUSH/PULL garantiza que si PC3 esta temporalmente
ocupado, los mensajes se encolan en el buffer ZMQ de PC2 sin
perderse. Cuando PC3 esta disponible retoma el procesamiento.
"""

import json
import queue
import threading

import zmq


class Receptor(threading.Thread):
    """
    Hilo PULL que recibe eventos procesados desde PC2.

    Parametros
    ----------
    config : dict
        Configuracion de PC3 (receptor.pull_port).
    cola_db : queue.Queue
        Cola compartida donde deposita los eventos para el almacenador.
    stop_event : threading.Event
        Evento de parada compartido.
    """

    def __init__(self, config: dict, cola_db: queue.Queue,
                 stop_event: threading.Event) -> None:
        super().__init__(name="Receptor-PULL", daemon=True)
        self.config     = config
        self.cola_db    = cola_db
        self.stop_event = stop_event

    def run(self) -> None:
        ctx  = zmq.Context()
        sock = ctx.socket(zmq.PULL)
        port = self.config["receptor"]["pull_port"]
        sock.bind(f"tcp://*:{port}")
        sock.setsockopt(zmq.RCVTIMEO, 1000)  # 1s timeout para revisar stop_event

        print(f"[RECEPTOR] Escuchando conexiones de PC2 en tcp://*:{port}")

        recibidos = 0
        while not self.stop_event.is_set():
            try:
                payload = sock.recv()
                evento  = json.loads(payload.decode("utf-8"))
                self.cola_db.put_nowait(evento)
                recibidos += 1
                if recibidos % 100 == 0:
                    print(f"[RECEPTOR] {recibidos} eventos recibidos de PC2")
            except zmq.Again:
                continue
            except json.JSONDecodeError as e:
                print(f"[RECEPTOR] Mensaje invalido ignorado: {e}")
            except Exception as e:
                if not self.stop_event.is_set():
                    print(f"[RECEPTOR] Error: {e}")

        sock.close()
        ctx.term()
        print(f"[RECEPTOR] Detenido. Total recibidos: {recibidos}")
