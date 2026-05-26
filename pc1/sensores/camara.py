"""
sensores/camara.py
==================
Sensor Camara de trafico.

Metricas generadas:
  - longitud_cola  : numero de vehiculos en cola en la interseccion (entero)
  - velocidad_promedio : velocidad media de los vehiculos detectados (km/h, float)

Topico ZMQ: camara.<posicion>   ej. camara.INT_C5
"""

import random

from sensores.base import SensorBase


class SensorCamara(SensorBase):
    """
    Simula una camara de videovigilancia instalada en una interseccion.

    La camara mide la longitud de la cola de vehiculos detenidos y la
    velocidad media de los vehiculos que circulan. Estos datos son
    clave para detectar retenciones prolongadas.
    """

    def _generar_evento(self) -> dict:
        """
        Genera un evento de camara con valores aleatorios dentro de los
        rangos configurados en ciudad.json.

        Retorna
        -------
        dict con las claves:
            sensor_id, tipo, posicion, timestamp,
            longitud_cola, velocidad_promedio
        """
        cola = random.randint(*self.config["rango_cola"])
        velocidad = round(random.uniform(*self.config["rango_velocidad"]), 1)

        return {
            "sensor_id": self.sensor_id,
            "tipo": self.tipo,
            "posicion": self.posicion,
            "timestamp": self._timestamp(),
            "longitud_cola": cola,           # numero de vehiculos en cola
            "velocidad_promedio": velocidad, # km/h
        }
