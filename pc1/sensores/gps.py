"""
sensores/gps.py
===============
Sensor GPS de trafico (agrupado / flotilla).

Metricas generadas:
  - densidad          : fraccion de la capacidad maxima ocupada [0.0 - 1.0]
  - velocidad_promedio: velocidad media de los vehiculos con GPS (km/h, float)
  - nivel_congestion  : clasificacion cualitativa del flujo

Logica de clasificacion:
  densidad > 0.70  -> ALTA
  densidad ≥ 0.40  -> NORMAL
  densidad < 0.40  -> BAJA

Topico ZMQ: gps.<posicion>   ej. gps.INT_E1
"""

import random

from sensores.base import SensorBase

# Umbrales de congestion (fraccion de densidad)
UMBRAL_ALTA   = 0.70
UMBRAL_NORMAL = 0.40


def _clasificar_congestion(densidad: float) -> str:
    """Clasifica el nivel de congestion a partir de la densidad vehicular."""
    if densidad > UMBRAL_ALTA:
        return "ALTA"
    elif densidad >= UMBRAL_NORMAL:
        return "NORMAL"
    else:
        return "BAJA"


class SensorGPS(SensorBase):
    """
    Simula datos agregados de una flotilla de vehiculos con GPS en una zona.

    Combina la densidad de la zona con la velocidad media reportada
    por los vehiculos, permitiendo a la analitica (PC2) clasificar
    el nivel de congestion sin necesidad de infraestructura fisica.
    """

    def _generar_evento(self) -> dict:
        """
        Genera un evento GPS con densidad y velocidad aleatorias dentro de
        los rangos configurados en ciudad.json.

        Retorna
        -------
        dict con las claves:
            sensor_id, tipo, posicion, timestamp,
            densidad, velocidad_promedio, nivel_congestion
        """
        densidad  = round(random.uniform(*self.config["rango_densidad"]), 3)
        velocidad = round(random.uniform(*self.config["rango_velocidad"]), 1)
        nivel     = _clasificar_congestion(densidad)

        return {
            "sensor_id": self.sensor_id,
            "tipo": self.tipo,
            "posicion": self.posicion,
            "timestamp": self._timestamp(),
            "densidad": densidad,            # [0.0 - 1.0]
            "velocidad_promedio": velocidad, # km/h
            "nivel_congestion": nivel,       # ALTA | NORMAL | BAJA
        }
