"""
sensores/espira.py
==================
Sensor Espira Inductiva de trafico.

Metricas generadas:
  - conteo_vehicular  : numero de vehiculos detectados en el intervalo (entero)
  - intervalo_seg     : duracion del intervalo de medicion en segundos (entero)

Topico ZMQ: espira.<posicion>   ej. espira.INT_A2
"""

import random

from sensores.base import SensorBase


class SensorEspira(SensorBase):
    """
    Simula una espira inductiva embebida en el asfalto de una interseccion.

    La espira detecta el paso de vehiculos mediante cambios en el campo
    magnetico y reporta el conteo acumulado en un intervalo de tiempo
    configurable. Es el sensor mas preciso para medir volumen de trafico.
    """

    def _generar_evento(self) -> dict:
        """
        Genera un evento de espira con el conteo vehicular aleatorio dentro
        de los rangos configurados en ciudad.json.

        Retorna
        -------
        dict con las claves:
            sensor_id, tipo, posicion, timestamp,
            conteo_vehicular, intervalo_seg
        """
        conteo = random.randint(*self.config["rango_conteo"])
        intervalo = self.config["intervalo_medicion_seg"]

        return {
            "sensor_id": self.sensor_id,
            "tipo": self.tipo,
            "posicion": self.posicion,
            "timestamp": self._timestamp(),
            "conteo_vehicular": conteo,  # vehiculos detectados en el intervalo
            "intervalo_seg": intervalo,  # duracion de la ventana de medicion
        }
