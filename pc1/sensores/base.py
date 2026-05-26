"""
sensores/base.py
================
Clase base abstracta para todos los sensores del PC1.

Cada sensor es un hilo independiente (threading.Thread) que:
  - Se conecta al Broker ZMQ (socket PUB -> XSUB del broker)
  - Genera un evento de trafico aleatorio cada `frecuencia_seg` segundos
  - Serializa el evento como JSON y lo publica en el topico correspondiente
  - Respeta el stop_event para apagarse limpiamente
"""

import json
import threading
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone

import zmq


class SensorBase(threading.Thread, ABC):
    """
    Clase base para los sensores de trafico.

    Parametros
    ----------
    config : dict
        Configuracion del sensor (sensor_id, tipo, posicion, frecuencia_seg, rangos...)
    broker_port : int
        Puerto XSUB del broker ZMQ al que se conecta el socket PUB del sensor.
    stop_event : threading.Event
        Evento compartido para senalar el apagado coordinado de todos los hilos.
    """

    def __init__(self, config: dict, broker_port: int, stop_event: threading.Event):
        super().__init__(name=config["sensor_id"], daemon=True)
        self.config = config
        self.sensor_id: str = config["sensor_id"]
        self.posicion: str = config["posicion"]
        self.tipo: str = config["tipo"]
        self.frecuencia: int = config["frecuencia_seg"]
        self.broker_port: int = broker_port
        self.stop_event: threading.Event = stop_event

        self._context: zmq.Context | None = None
        self._socket: zmq.Socket | None = None

    # ------------------------------------------------------------------
    # Conexion ZMQ
    # ------------------------------------------------------------------

    def _conectar(self) -> None:
        """Crea el socket PUB y se conecta al XSUB del Broker."""
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.PUB)
        self._socket.connect(f"tcp://localhost:{self.broker_port}")

    def _desconectar(self) -> None:
        """Cierra el socket y termina el contexto ZMQ del hilo."""
        if self._socket:
            self._socket.close()
        if self._context:
            self._context.term()

    # ------------------------------------------------------------------
    # Publicacion de mensajes
    # ------------------------------------------------------------------

    def _topico(self) -> bytes:
        """Devuelve el topico ZMQ del sensor: '<tipo>.<posicion>'."""
        return f"{self.tipo}.{self.posicion}".encode("utf-8")

    @staticmethod
    def _timestamp() -> str:
        """Timestamp ISO-8601 con zona horaria local, de alta precision."""
        return datetime.now(timezone.utc).astimezone().isoformat(timespec="microseconds")

    @abstractmethod
    def _generar_evento(self) -> dict:
        """
        Genera y devuelve el paquete de datos del sensor (sin serializar).
        Debe ser implementado por cada clase hija.
        """

    def _publicar(self, evento: dict) -> None:
        """Serializa el evento a JSON y lo envia al broker en dos partes: [topico, payload]."""
        payload = json.dumps(evento, ensure_ascii=False).encode("utf-8")
        self._socket.send_multipart([self._topico(), payload])

    # ------------------------------------------------------------------
    # Ciclo de vida del hilo
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Bucle principal del sensor: genera y publica eventos periodicamente."""
        self._conectar()
        # Pequena pausa para que el socket PUB se establezca con el broker
        time.sleep(0.3)

        print(f"  [ON]  [{self.sensor_id:<10}] activo en {self.posicion} - cada {self.frecuencia}s")

        while not self.stop_event.is_set():
            evento = self._generar_evento()
            self._publicar(evento)
            # Espera `frecuencia` segundos o hasta que stop_event sea activado
            self.stop_event.wait(timeout=self.frecuencia)

        self._desconectar()
        print(f"  [OFF] [{self.sensor_id:<10}] detenido.")
