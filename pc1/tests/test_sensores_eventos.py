"""
pc1/tests/test_sensores_eventos.py
====================================
Pruebas unitarias — PT-U-05: Generación de eventos por tipo de sensor.

Verifica que cada clase de sensor (_generar_evento) produce un dict con
los campos requeridos y valores dentro de los rangos configurados,
sin necesidad de levantar ZMQ ni hilos.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

PC1_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PC1_DIR))

from sensores.camara import SensorCamara
from sensores.espira import SensorEspira
from sensores.gps import SensorGPS, _clasificar_congestion


# ─── Configuraciones de sensor individuales ───────────────────────────────────

CFG_CAMARA = {
    "sensor_id": "CAM_A1_NS",
    "tipo": "camara",
    "posicion": "INT_A1_NS",
    "frecuencia_seg": 30,
    "rango_cola": [0, 20],
    "rango_velocidad": [10.0, 90.0],
}

CFG_ESPIRA = {
    "sensor_id": "ESP_B2_EO",
    "tipo": "espira",
    "posicion": "INT_B2_EO",
    "frecuencia_seg": 30,
    "rango_conteo": [0, 200],
    "intervalo_medicion_seg": 30,
}

CFG_GPS = {
    "sensor_id": "GPS_C3_NS",
    "tipo": "gps",
    "posicion": "INT_C3_NS",
    "frecuencia_seg": 30,
    "rango_densidad": [0.0, 1.0],
    "rango_velocidad": [5.0, 100.0],
}


def _crear_sensor(clase, cfg):
    """Crea instancia de sensor con ZMQ mockeado (sin conexión real)."""
    ctx_mock = MagicMock()
    stop_ev  = MagicMock()
    stop_ev.is_set.return_value = True   # evita que el hilo entre en bucle
    sensor   = clase.__new__(clase)
    sensor.config     = cfg
    sensor.sensor_id  = cfg["sensor_id"]
    sensor.posicion   = cfg["posicion"]
    sensor.tipo       = cfg["tipo"]
    sensor.frecuencia = cfg["frecuencia_seg"]
    return sensor


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-05a: Sensor Cámara
# ──────────────────────────────────────────────────────────────────────────────

class TestSensorCamara:
    @pytest.fixture(autouse=True)
    def sensor(self):
        self.sensor = _crear_sensor(SensorCamara, CFG_CAMARA)

    def test_contiene_campos_requeridos(self):
        ev = self.sensor._generar_evento()
        for campo in ("sensor_id", "tipo", "posicion", "timestamp",
                      "longitud_cola", "velocidad_promedio"):
            assert campo in ev, f"Campo faltante: {campo}"

    def test_sensor_id_correcto(self):
        ev = self.sensor._generar_evento()
        assert ev["sensor_id"] == "CAM_A1_NS"

    def test_tipo_es_camara(self):
        ev = self.sensor._generar_evento()
        assert ev["tipo"] == "camara"

    def test_longitud_cola_en_rango(self):
        for _ in range(20):   # muestreo estadístico
            ev = self.sensor._generar_evento()
            assert 0 <= ev["longitud_cola"] <= 20, (
                f"longitud_cola fuera de rango: {ev['longitud_cola']}"
            )

    def test_velocidad_promedio_en_rango(self):
        for _ in range(20):
            ev = self.sensor._generar_evento()
            assert 10.0 <= ev["velocidad_promedio"] <= 90.0, (
                f"velocidad_promedio fuera de rango: {ev['velocidad_promedio']}"
            )

    def test_longitud_cola_es_entero(self):
        ev = self.sensor._generar_evento()
        assert isinstance(ev["longitud_cola"], int)

    def test_velocidad_es_float(self):
        ev = self.sensor._generar_evento()
        assert isinstance(ev["velocidad_promedio"], float)


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-05b: Sensor Espira
# ──────────────────────────────────────────────────────────────────────────────

class TestSensorEspira:
    @pytest.fixture(autouse=True)
    def sensor(self):
        self.sensor = _crear_sensor(SensorEspira, CFG_ESPIRA)

    def test_contiene_campos_requeridos(self):
        ev = self.sensor._generar_evento()
        for campo in ("sensor_id", "tipo", "posicion", "timestamp",
                      "conteo_vehicular", "intervalo_seg"):
            assert campo in ev, f"Campo faltante: {campo}"

    def test_tipo_es_espira(self):
        ev = self.sensor._generar_evento()
        assert ev["tipo"] == "espira"

    def test_conteo_vehicular_en_rango(self):
        for _ in range(20):
            ev = self.sensor._generar_evento()
            assert 0 <= ev["conteo_vehicular"] <= 200, (
                f"conteo_vehicular fuera de rango: {ev['conteo_vehicular']}"
            )

    def test_intervalo_seg_es_constante(self):
        """El intervalo de medición debe ser fijo (30s según config)."""
        for _ in range(5):
            ev = self.sensor._generar_evento()
            assert ev["intervalo_seg"] == 30

    def test_conteo_es_entero(self):
        ev = self.sensor._generar_evento()
        assert isinstance(ev["conteo_vehicular"], int)


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-05c: Sensor GPS
# ──────────────────────────────────────────────────────────────────────────────

class TestSensorGPS:
    @pytest.fixture(autouse=True)
    def sensor(self):
        self.sensor = _crear_sensor(SensorGPS, CFG_GPS)

    def test_contiene_campos_requeridos(self):
        ev = self.sensor._generar_evento()
        for campo in ("sensor_id", "tipo", "posicion", "timestamp",
                      "densidad", "velocidad_promedio", "nivel_congestion"):
            assert campo in ev, f"Campo faltante: {campo}"

    def test_tipo_es_gps(self):
        ev = self.sensor._generar_evento()
        assert ev["tipo"] == "gps"

    def test_densidad_en_rango_0_1(self):
        for _ in range(20):
            ev = self.sensor._generar_evento()
            assert 0.0 <= ev["densidad"] <= 1.0, (
                f"densidad fuera de [0,1]: {ev['densidad']}"
            )

    def test_nivel_congestion_valores_validos(self):
        valores_validos = {"ALTA", "NORMAL", "BAJA"}
        for _ in range(30):
            ev = self.sensor._generar_evento()
            assert ev["nivel_congestion"] in valores_validos, (
                f"nivel_congestion inválido: {ev['nivel_congestion']}"
            )

    def test_velocidad_en_rango(self):
        for _ in range(20):
            ev = self.sensor._generar_evento()
            assert 5.0 <= ev["velocidad_promedio"] <= 100.0


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-05d: Función auxiliar _clasificar_congestion (GPS)
# ──────────────────────────────────────────────────────────────────────────────

class TestClasificarCongestion:
    """Prueba la función pura _clasificar_congestion directamente."""

    @pytest.mark.parametrize("densidad,esperado", [
        (0.00, "BAJA"),
        (0.39, "BAJA"),
        (0.40, "NORMAL"),
        (0.55, "NORMAL"),
        (0.70, "NORMAL"),   # 0.70 NO supera el umbral ALTA (es estricto >)
        (0.71, "ALTA"),
        (1.00, "ALTA"),
    ])
    def test_clasificacion_correcta(self, densidad, esperado):
        resultado = _clasificar_congestion(densidad)
        assert resultado == esperado, (
            f"densidad={densidad}: esperado={esperado}, obtenido={resultado}"
        )
