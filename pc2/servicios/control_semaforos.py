"""
servicios/control_semaforos.py
==============================
Hilo controlador de semaforos.

Consume comandos de la cola interna (cola_semaforos) producida por
el motor de reglas y simula el actuador imprimiendo cada operacion
con timestamp, posicion, color resultante y duracion.

No usa ZMQ (comunicacion interna via queue.Queue). Si en fases
posteriores se requiere actuacion real, este modulo es el punto
de extension para enviar comandos a los controladores fisicos.
"""

import queue
import threading
from datetime import datetime, timezone


# Duraciones por defecto si no vienen en el config (segundos)
_DURACION_DEFECTO = {
    "NORMAL":     15,
    "CONGESTION": 30,
    "OLA_VERDE":  60,
}


class ControlSemaforos(threading.Thread):
    """
    Simula el actuador de semaforos de una interseccion.

    Por cada comando recibido imprime una linea con el formato:
      [SEMAFORO] HH:MM:SS | INT_XX | Verde=Xs / Rojo=Ys | Estado: XXX | Motivo: ...
    """

    def __init__(self, config: dict, cola_semaforos: queue.Queue,
                 stop_event: threading.Event) -> None:
        super().__init__(name="Control-Semaforos", daemon=True)
        self.config         = config
        self.cola           = cola_semaforos
        self.stop_event     = stop_event
        self.tiempos        = config.get("semaforo", {})
        self._historial: list[dict] = []   # registro en memoria

    @staticmethod
    def _timestamp() -> str:
        return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    def _duracion_verde(self, estado: str) -> int:
        mapa = {
            "NORMAL":     self.tiempos.get("tiempo_verde_normal_seg",     15),
            "CONGESTION": self.tiempos.get("tiempo_verde_congestion_seg", 30),
            "OLA_VERDE":  self.tiempos.get("tiempo_ola_verde_seg",        60),
        }
        return mapa.get(estado, _DURACION_DEFECTO.get(estado, 15))

    def _aplicar(self, cmd: dict) -> None:
        posicion = cmd.get("posicion", "???")
        estado   = cmd.get("estado",   "NORMAL")
        motivo   = cmd.get("motivo",   "")
        sensor   = cmd.get("sensor_id", "N/A")
        ts       = cmd.get("timestamp", self._timestamp())[:19]

        t_verde = self._duracion_verde(estado)
        t_rojo  = max(5, 30 - t_verde) if estado != "OLA_VERDE" else 0

        # Descripcion de la accion
        if estado == "OLA_VERDE":
            accion = f"VERDE={t_verde}s (ola verde) / ROJO=0s"
        elif estado == "CONGESTION":
            accion = f"VERDE={t_verde}s (extendido) / ROJO={t_rojo}s"
        else:
            accion = f"VERDE={t_verde}s / ROJO={t_rojo}s"

        linea = (
            f"[SEMAFORO] {ts} | {posicion:<8} | {accion:<35} "
            f"| Estado: {estado:<10} | Sensor: {sensor}"
        )
        print(linea)
        if motivo:
            print(f"           Razon: {motivo}")

        self._historial.append({
            "timestamp": ts,
            "posicion":  posicion,
            "estado":    estado,
            "t_verde":   t_verde,
            "t_rojo":    t_rojo,
            "motivo":    motivo,
        })

    def run(self) -> None:
        print("[SEMAFORO] Controlador activo. Esperando comandos...")
        while not self.stop_event.is_set():
            try:
                cmd = self.cola.get(timeout=1.0)
                self._aplicar(cmd)
            except queue.Empty:
                continue
        print(f"[SEMAFORO] Controlador detenido. "
              f"{len(self._historial)} operaciones registradas.")
