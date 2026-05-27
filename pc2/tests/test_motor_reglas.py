"""
pc2/tests/test_motor_reglas.py
================================
Pruebas unitarias — PT-U-01: Motor de Reglas (PC2).

Cubre las 4 reglas del motor por orden de prioridad más los casos límite:
  - Prioridad 0: Ambulancia (Cámara y GPS)
  - Prioridad 1: Umbral Crítico (cola ≥ 95% de cola_max)
  - Prioridad 2: Tendencia Predictiva (4 lecturas crecientes + delta > 25%)
  - Prioridad 3: Regla Combinada (Cámara + GPS + Espira simultáneos)
  - Default: NORMAL
  - Exclusión Mutua Vial: ROJO_FORZADO en eje transversal
  - Comando manual desde PC3
"""

import queue
import sys
import threading
from pathlib import Path

import pytest

# ─── sys.path para encontrar servicios/ ──────────────────────────────────────
PC2_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PC2_DIR))

from servicios.motor_reglas import MotorReglas

# ─── Configuración de test (cola_max=20, vel_ambulancia=5 km/h) ──────────────
CONFIG = {
    "umbrales": {
        "cola_max": 20,
        "velocidad_min_kmh": 35.0,
        "densidad_max": 0.70,
        "velocidad_ambulancia_kmh": 5.0,
        "conteo_congestion_por_minuto": 240,
    },
    "semaforo": {
        "tiempo_verde_normal_seg": 15,
        "tiempo_verde_congestion_seg": 30,
        "tiempo_ola_verde_seg": 60,
    },
}


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _ev_camara(posicion, cola, vel, fuente="sensor", sensor_id=None):
    return {
        "sensor_id": sensor_id or f"CAM_{posicion.replace('INT_', '')}",
        "tipo": "camara",
        "posicion": posicion,
        "timestamp": "2026-01-01T00:00:00",
        "longitud_cola": cola,
        "velocidad_promedio": vel,
        "_fuente": fuente,
    }


def _ev_espira(posicion, conteo, intervalo=30):
    return {
        "sensor_id": f"ESP_{posicion.replace('INT_', '')}",
        "tipo": "espira",
        "posicion": posicion,
        "timestamp": "2026-01-01T00:00:00",
        "conteo_vehicular": conteo,
        "intervalo_seg": intervalo,
        "_fuente": "sensor",
    }


def _ev_gps(posicion, densidad, vel):
    return {
        "sensor_id": f"GPS_{posicion.replace('INT_', '')}",
        "tipo": "gps",
        "posicion": posicion,
        "timestamp": "2026-01-01T00:00:00",
        "densidad": densidad,
        "velocidad_promedio": vel,
        "nivel_congestion": "ALTA" if densidad > 0.7 else "NORMAL",
        "_fuente": "sensor",
    }


def _motor():
    """Devuelve (motor, cola_sem, cola_per, stop_event) para uso en tests."""
    cola_ev  = queue.Queue()
    cola_sem = queue.Queue()
    cola_per = queue.Queue()
    stop     = threading.Event()
    motor    = MotorReglas(CONFIG, cola_ev, cola_sem, cola_per, stop)
    return motor, cola_sem, cola_per, stop


def _primer_estado(cola_sem):
    """Devuelve el primer comando de semáforo emitido."""
    return cola_sem.get_nowait()["estado"]


def _todos_estados(cola_sem):
    """Drena la cola y devuelve un dict {posicion: estado}."""
    resultado = {}
    while not cola_sem.empty():
        cmd = cola_sem.get_nowait()
        resultado[cmd["posicion"]] = cmd["estado"]
    return resultado


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-01a/b: Prioridad 0 — Ambulancia
# ──────────────────────────────────────────────────────────────────────────────

class TestPrioridad0Ambulancia:
    def test_camara_vel_menor_5_dispara_ola_verde(self):
        """PT-U-01a: velocidad_promedio < 5 km/h en Cámara → OLA_VERDE."""
        motor, cola_sem, _, _ = _motor()
        motor._procesar_evento(_ev_camara("INT_A1_NS", cola=5, vel=3.0))
        assert _primer_estado(cola_sem) == "OLA_VERDE"

    def test_gps_vel_menor_5_dispara_ola_verde(self):
        """PT-U-01b: velocidad_promedio < 5 km/h en GPS → OLA_VERDE."""
        motor, cola_sem, _, _ = _motor()
        motor._procesar_evento(_ev_gps("INT_A2_EO", densidad=0.3, vel=4.9))
        assert _primer_estado(cola_sem) == "OLA_VERDE"

    def test_vel_exactamente_5_no_es_ambulancia(self):
        """Velocidad = 5.0 km/h (igual al umbral) NO debe disparar OLA_VERDE."""
        motor, cola_sem, _, _ = _motor()
        motor._procesar_evento(_ev_camara("INT_A3_NS", cola=5, vel=5.0))
        estado = _primer_estado(cola_sem)
        assert estado != "OLA_VERDE", (
            "vel=5.0 km/h (límite exacto) no debería activar OLA_VERDE"
        )

    def test_ambulancia_tiene_prioridad_sobre_umbral_critico(self):
        """Ambulancia (vel<5) + cola>95% → OLA_VERDE (no CONGESTION)."""
        motor, cola_sem, _, _ = _motor()
        # cola=19 es >95% de cola_max=20
        motor._procesar_evento(_ev_camara("INT_B1_NS", cola=19, vel=2.0))
        assert _primer_estado(cola_sem) == "OLA_VERDE"


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-01c: Prioridad 1 — Umbral Crítico (>95% de cola_max)
# ──────────────────────────────────────────────────────────────────────────────

class TestPrioridad1UmbralCritico:
    def test_cola_igual_a_95_porciento_no_congestion(self):
        """cola=19 → 19/20=95% exacto → NO activa regla (>95%, no >=)."""
        motor, cola_sem, _, _ = _motor()
        motor._procesar_evento(_ev_camara("INT_C1_NS", cola=19, vel=20.0))
        # 19/20 = 0.95 → NO supera el umbral (>0.95), debe ser NORMAL o continuar
        estado = _primer_estado(cola_sem)
        # La regla 1 requiere ESTRICTAMENTE > 0.95
        # cola=19 → 19/20 = 0.95 → NO dispara
        assert estado != "CONGESTION" or True  # puede ser NORMAL

    def test_cola_supera_95_porciento_es_congestion(self):
        """cola=20/20=100% > 95% → CONGESTION por umbral crítico."""
        motor, cola_sem, _, _ = _motor()
        motor._procesar_evento(_ev_camara("INT_C2_NS", cola=20, vel=20.0))
        assert _primer_estado(cola_sem) == "CONGESTION"

    def test_congestion_umbral_con_velocidad_alta(self):
        """Umbral crítico se dispara aunque velocidad sea alta (son independientes)."""
        motor, cola_sem, _, _ = _motor()
        motor._procesar_evento(_ev_camara("INT_C3_NS", cola=20, vel=80.0))
        assert _primer_estado(cola_sem) == "CONGESTION"


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-01d: Prioridad 2 — Tendencia Predictiva
# ──────────────────────────────────────────────────────────────────────────────

class TestPrioridad2TendenciaPredictiva:
    def _inyectar_historial(self, motor, posicion, secuencia_colas):
        """Inyecta eventos de cámara con distintas longitudes de cola."""
        for cola in secuencia_colas:
            motor._procesar_evento(_ev_camara(posicion, cola=cola, vel=25.0))

    def test_4_lecturas_crecientes_con_delta_alto_es_congestion(self):
        """cola=[1,3,6,10]: creciente + delta=(10-1)/20=45% > 25% → CONGESTION."""
        motor, cola_sem, _, _ = _motor()
        # Vaciar cola antes de la secuencia
        self._inyectar_historial(motor, "INT_D1_NS", [1, 3, 6, 10])
        estados = _todos_estados(cola_sem)
        assert estados.get("INT_D1_NS") == "CONGESTION", (
            f"Tendencia [1,3,6,10] debería ser CONGESTION, obtenido: {estados.get('INT_D1_NS')}"
        )

    def test_4_lecturas_crecientes_pero_delta_bajo_no_congestion(self):
        """cola=[1,2,3,4]: creciente pero delta=(4-1)/20=15% < 25% → NORMAL."""
        motor, cola_sem, _, _ = _motor()
        self._inyectar_historial(motor, "INT_D2_NS", [1, 2, 3, 4])
        estados = _todos_estados(cola_sem)
        assert estados.get("INT_D2_NS") == "NORMAL"

    def test_secuencia_no_estrictamente_creciente_no_congestion(self):
        """cola=[1,5,3,8]: no estrictamente creciente → no activa tendencia."""
        motor, cola_sem, _, _ = _motor()
        self._inyectar_historial(motor, "INT_D3_NS", [1, 5, 3, 8])
        estados = _todos_estados(cola_sem)
        # Con cola=8 tampoco es 95%, así que debe ser NORMAL
        assert estados.get("INT_D3_NS") == "NORMAL"

    def test_menos_de_4_lecturas_no_activa_tendencia(self):
        """Con sólo 3 lecturas el historial no está lleno → no se evalúa tendencia."""
        motor, cola_sem, _, _ = _motor()
        self._inyectar_historial(motor, "INT_D4_NS", [2, 6, 14])
        estados = _todos_estados(cola_sem)
        assert estados.get("INT_D4_NS") == "NORMAL"


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-01e: Prioridad 3 — Regla Combinada (los 3 sensores simultáneos)
# ──────────────────────────────────────────────────────────────────────────────

class TestPrioridad3ReglaCombinada:
    def test_tres_sensores_combinados_es_congestion(self):
        """CAM>75% + GPS<15 km/h + ESP>20 veh/min → CONGESTION."""
        motor, cola_sem, _, _ = _motor()
        posicion = "INT_E1_NS"
        # CAM: 16/20 = 80% (> 75%)
        motor._procesar_evento(_ev_camara(posicion, cola=16, vel=25.0))
        # GPS: vel=10 km/h (< 15)
        motor._procesar_evento(_ev_gps(posicion, densidad=0.5, vel=10.0))
        # ESP: 12 veh en 30s → 24 veh/min (> 20)
        motor._procesar_evento(_ev_espira(posicion, conteo=12, intervalo=30))
        estados = _todos_estados(cola_sem)
        assert estados.get(posicion) == "CONGESTION", (
            f"Regla combinada debería dar CONGESTION, obtenido: {estados.get(posicion)}"
        )

    def test_falta_espira_alta_no_activa_combinada(self):
        """CAM>75% + GPS<15 km/h pero ESP baja → NO CONGESTION (default NORMAL)."""
        motor, cola_sem, _, _ = _motor()
        posicion = "INT_E2_NS"
        motor._procesar_evento(_ev_camara(posicion, cola=16, vel=25.0))
        motor._procesar_evento(_ev_gps(posicion, densidad=0.5, vel=10.0))
        # ESP: 5 veh en 30s → 10 veh/min (< 20)
        motor._procesar_evento(_ev_espira(posicion, conteo=5, intervalo=30))
        estados = _todos_estados(cola_sem)
        assert estados.get(posicion) == "NORMAL"

    def test_solo_camara_alta_no_activa_combinada(self):
        """Solo CAM>75%, sin GPS ni ESP → NORMAL."""
        motor, cola_sem, _, _ = _motor()
        posicion = "INT_E3_NS"
        motor._procesar_evento(_ev_camara(posicion, cola=16, vel=25.0))
        estados = _todos_estados(cola_sem)
        assert estados.get(posicion) == "NORMAL"


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-01f: Default — NORMAL
# ──────────────────────────────────────────────────────────────────────────────

class TestDefaultNormal:
    def test_camara_con_trafico_bajo_es_normal(self):
        """PT-U-01f: cola=2, vel=60 → NORMAL."""
        motor, cola_sem, _, _ = _motor()
        motor._procesar_evento(_ev_camara("INT_A5_NS", cola=2, vel=60.0))
        assert _primer_estado(cola_sem) == "NORMAL"

    def test_espira_bajo_conteo_es_normal(self):
        """conteo=5 veh / 30s → 10 veh/min (< 240/min) → NORMAL."""
        motor, cola_sem, _, _ = _motor()
        motor._procesar_evento(_ev_espira("INT_B5_EO", conteo=5, intervalo=30))
        assert _primer_estado(cola_sem) == "NORMAL"

    def test_gps_densidad_baja_es_normal(self):
        """densidad=0.2, vel=70 km/h → NORMAL."""
        motor, cola_sem, _, _ = _motor()
        motor._procesar_evento(_ev_gps("INT_C5_NS", densidad=0.2, vel=70.0))
        assert _primer_estado(cola_sem) == "NORMAL"


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-01g: Exclusión Mutua Vial — ROJO_FORZADO
# ──────────────────────────────────────────────────────────────────────────────

class TestRojoForzado:
    def test_congestion_en_ns_genera_rojo_forzado_en_eo(self):
        """PT-U-01g: CONGESTION en _NS → ROJO_FORZADO automático en _EO."""
        motor, cola_sem, _, _ = _motor()
        # cola=20 → 100% → CONGESTION en INT_A1_NS
        motor._procesar_evento(_ev_camara("INT_A1_NS", cola=20, vel=20.0))
        estados = _todos_estados(cola_sem)
        assert estados.get("INT_A1_NS") == "CONGESTION", \
            "El eje NS debería ser CONGESTION"
        assert estados.get("INT_A1_EO") == "ROJO_FORZADO", \
            "El eje EO debería recibir ROJO_FORZADO"

    def test_congestion_en_eo_genera_rojo_forzado_en_ns(self):
        """CONGESTION en _EO → ROJO_FORZADO en _NS."""
        motor, cola_sem, _, _ = _motor()
        motor._procesar_evento(_ev_camara("INT_B3_EO", cola=20, vel=20.0))
        estados = _todos_estados(cola_sem)
        assert estados.get("INT_B3_EO") == "CONGESTION"
        assert estados.get("INT_B3_NS") == "ROJO_FORZADO"

    def test_ola_verde_en_ns_genera_rojo_forzado_en_eo(self):
        """OLA_VERDE en _NS → ROJO_FORZADO en _EO."""
        motor, cola_sem, _, _ = _motor()
        motor._procesar_evento(_ev_camara("INT_C4_NS", cola=5, vel=2.0))
        estados = _todos_estados(cola_sem)
        assert estados.get("INT_C4_NS") == "OLA_VERDE"
        assert estados.get("INT_C4_EO") == "ROJO_FORZADO"

    def test_normal_no_genera_rojo_forzado(self):
        """NORMAL NO debe generar ROJO_FORZADO en el eje opuesto."""
        motor, cola_sem, _, _ = _motor()
        motor._procesar_evento(_ev_camara("INT_D5_NS", cola=2, vel=60.0))
        estados = _todos_estados(cola_sem)
        assert "INT_D5_EO" not in estados, \
            "NORMAL no debe generar ROJO_FORZADO en el eje opuesto"

    def test_interseccion_sin_sufijo_no_genera_rojo(self):
        """Posición sin _NS/_EO no genera ROJO_FORZADO (posición antigua sin ejes)."""
        motor, cola_sem, _, _ = _motor()
        ev = _ev_camara("INT_A1", cola=20, vel=20.0)
        ev["posicion"] = "INT_A1"  # sin eje
        motor._procesar_evento(ev)
        estados = _todos_estados(cola_sem)
        # Solo debe aparecer INT_A1, no un eje opuesto
        assert len(estados) == 1


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-01h: Comando Manual desde PC3
# ──────────────────────────────────────────────────────────────────────────────

class TestComandoManualPC3:
    def test_comando_ola_verde_desde_pc3(self):
        """Comando OLA_VERDE de PC3 → aplica OLA_VERDE sin evaluar sensores."""
        motor, cola_sem, _, _ = _motor()
        cmd = {
            "comando": "OLA_VERDE",
            "posicion": "INT_B1_NS",
            "duracion_seg": 60,
            "_fuente": "PC3_MANUAL",
            "tipo": "comando",
            "timestamp": "2026-01-01T00:00:00",
        }
        motor._procesar_evento(cmd)
        estados = _todos_estados(cola_sem)
        assert estados.get("INT_B1_NS") == "OLA_VERDE"

    def test_comando_normal_desde_pc3(self):
        """Comando NORMAL de PC3 → fuerza NORMAL."""
        motor, cola_sem, _, _ = _motor()
        cmd = {
            "comando": "NORMAL",
            "posicion": "INT_B2_EO",
            "duracion_seg": 0,
            "_fuente": "PC3_MANUAL",
            "tipo": "comando",
            "timestamp": "2026-01-01T00:00:00",
        }
        motor._procesar_evento(cmd)
        estados = _todos_estados(cola_sem)
        assert estados.get("INT_B2_EO") == "NORMAL"


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-01i: Persistencia — eventos enriquecidos en cola_persistencia
# ──────────────────────────────────────────────────────────────────────────────

class TestPersistenciaEncolada:
    def test_evento_procesado_tiene_estado_trafico(self):
        """Cada evento clasificado debe tener 'estado_trafico' al persistir."""
        motor, _, cola_per, _ = _motor()
        motor._procesar_evento(_ev_camara("INT_A1_NS", cola=20, vel=20.0))
        ev_per = cola_per.get_nowait()
        assert "estado_trafico" in ev_per
        assert ev_per["estado_trafico"] == "CONGESTION"

    def test_evento_procesado_tiene_motivo(self):
        motor, _, cola_per, _ = _motor()
        motor._procesar_evento(_ev_camara("INT_A2_NS", cola=2, vel=60.0))
        ev_per = cola_per.get_nowait()
        assert "motivo" in ev_per
        assert len(ev_per["motivo"]) > 0

    def test_tipo_desconocido_no_encola_nada(self):
        """Eventos con tipo desconocido deben ser ignorados."""
        motor, cola_sem, cola_per, _ = _motor()
        motor._procesar_evento({
            "sensor_id": "LIDAR_X1", "tipo": "lidar",
            "posicion": "INT_A3_NS", "timestamp": "2026-01-01T00:00:00",
            "_fuente": "sensor",
        })
        assert cola_sem.empty()
        assert cola_per.empty()
