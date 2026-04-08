"""
Paquete de sensores del PC1.
Expone las tres clases de sensor disponibles.
"""

from .camara import SensorCamara
from .espira import SensorEspira
from .gps import SensorGPS

__all__ = ["SensorCamara", "SensorEspira", "SensorGPS"]
