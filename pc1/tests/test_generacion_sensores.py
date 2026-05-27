"""
pc1/tests/test_generacion_sensores.py
======================================
Pruebas unitarias — PT-U-02: Generación dinámica de sensores (PC1).

Verifica que `generar_lista_sensores()` produce exactamente la lista correcta
para una cuadrícula 5×5: 150 sensores, 50 de cada tipo, 50 posiciones únicas
y que todos los IDs respetan el formato `CAM|ESP|GPS_<Letra><Num>_<NS|EO>`.
"""

import re
import sys
from pathlib import Path

# ─── Aseguramos que 'pc1/' esté en sys.path para importar gestor.py ──────────
PC1_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PC1_DIR))

import pytest
from gestor import generar_lista_sensores

# ─── Config de referencia para una cuadrícula 5×5 ────────────────────────────
CONFIG_5x5 = {
    "ciudad": {"nombre": "Test", "filas": 5, "columnas": 5},
    "tipos_sensor": {
        "camara": {
            "frecuencia_seg": 30,
            "rango_cola": [0, 20],
            "rango_velocidad": [10.0, 90.0],
        },
        "espira": {
            "frecuencia_seg": 30,
            "rango_conteo": [0, 200],
            "intervalo_medicion_seg": 30,
        },
        "gps": {
            "frecuencia_seg": 30,
            "rango_densidad": [0.0, 1.0],
            "rango_velocidad": [5.0, 100.0],
        },
    },
}

# Config reducida para verificar escala diferente (2×2)
CONFIG_2x2 = {
    "ciudad": {"nombre": "Mini", "filas": 2, "columnas": 2},
    "tipos_sensor": CONFIG_5x5["tipos_sensor"],
}


@pytest.fixture(scope="module")
def sensores_5x5():
    """Genera la lista una sola vez para todos los tests del módulo."""
    return generar_lista_sensores(CONFIG_5x5)


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-02a: Cantidad total
# ──────────────────────────────────────────────────────────────────────────────

class TestCantidadTotal:
    def test_150_sensores_para_5x5(self, sensores_5x5):
        """5 filas × 5 cols × 2 ejes × 3 tipos = 150 sensores."""
        assert len(sensores_5x5) == 150, (
            f"Se esperaban 150 sensores, se obtuvieron {len(sensores_5x5)}"
        )

    def test_escala_2x2_produce_24_sensores(self):
        """2×2×2×3 = 24 sensores."""
        sensores = generar_lista_sensores(CONFIG_2x2)
        assert len(sensores) == 24

    def test_lista_no_vacia(self, sensores_5x5):
        assert len(sensores_5x5) > 0


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-02b: Formato de sensor_id
# ──────────────────────────────────────────────────────────────────────────────

class TestFormatoSensorId:
    PATRON_ID = re.compile(r"^(CAM|ESP|GPS)_[A-E][1-5]_(NS|EO)$")

    def test_todos_los_ids_respetan_patron(self, sensores_5x5):
        """Todos los IDs deben seguir el formato PREFIJO_LetraNumero_Eje."""
        invalidos = [
            s["sensor_id"]
            for s in sensores_5x5
            if not self.PATRON_ID.match(s["sensor_id"])
        ]
        assert invalidos == [], f"IDs con formato inválido: {invalidos}"

    def test_ids_son_unicos(self, sensores_5x5):
        """No debe haber dos sensores con el mismo sensor_id."""
        ids = [s["sensor_id"] for s in sensores_5x5]
        assert len(ids) == len(set(ids)), "Existen sensor_id duplicados"

    def test_prefijo_camara_es_CAM(self, sensores_5x5):
        camaras = [s for s in sensores_5x5 if s["tipo"] == "camara"]
        for s in camaras:
            assert s["sensor_id"].startswith("CAM_"), (
                f"Cámara con prefijo incorrecto: {s['sensor_id']}"
            )

    def test_prefijo_espira_es_ESP(self, sensores_5x5):
        espiras = [s for s in sensores_5x5 if s["tipo"] == "espira"]
        for s in espiras:
            assert s["sensor_id"].startswith("ESP_"), (
                f"Espira con prefijo incorrecto: {s['sensor_id']}"
            )

    def test_prefijo_gps_es_GPS(self, sensores_5x5):
        gps_list = [s for s in sensores_5x5 if s["tipo"] == "gps"]
        for s in gps_list:
            assert s["sensor_id"].startswith("GPS_"), (
                f"GPS con prefijo incorrecto: {s['sensor_id']}"
            )


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-02c: Cantidad por tipo
# ──────────────────────────────────────────────────────────────────────────────

class TestCantidadPorTipo:
    def test_50_camaras(self, sensores_5x5):
        n = sum(1 for s in sensores_5x5 if s["tipo"] == "camara")
        assert n == 50, f"Se esperaban 50 cámaras, hay {n}"

    def test_50_espiras(self, sensores_5x5):
        n = sum(1 for s in sensores_5x5 if s["tipo"] == "espira")
        assert n == 50, f"Se esperaban 50 espiras, hay {n}"

    def test_50_gps(self, sensores_5x5):
        n = sum(1 for s in sensores_5x5 if s["tipo"] == "gps")
        assert n == 50, f"Se esperaban 50 GPS, hay {n}"

    def test_no_hay_tipos_desconocidos(self, sensores_5x5):
        tipos_validos = {"camara", "espira", "gps"}
        tipos_encontrados = {s["tipo"] for s in sensores_5x5}
        assert tipos_encontrados.issubset(tipos_validos), (
            f"Tipos desconocidos: {tipos_encontrados - tipos_validos}"
        )


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-02d: Posiciones únicas
# ──────────────────────────────────────────────────────────────────────────────

class TestPosiciones:
    def test_50_posiciones_unicas(self, sensores_5x5):
        """25 intersecciones × 2 ejes (NS/EO) = 50 posiciones únicas."""
        posiciones = {s["posicion"] for s in sensores_5x5}
        assert len(posiciones) == 50, (
            f"Se esperaban 50 posiciones únicas, hay {len(posiciones)}"
        )

    def test_formato_posicion(self, sensores_5x5):
        """Las posiciones deben tener formato INT_<Letra><Num>_<NS|EO>."""
        patron = re.compile(r"^INT_[A-E][1-5]_(NS|EO)$")
        invalidas = [
            s["posicion"]
            for s in sensores_5x5
            if not patron.match(s["posicion"])
        ]
        assert invalidas == [], f"Posiciones con formato inválido: {invalidas}"

    def test_cada_posicion_tiene_3_sensores(self, sensores_5x5):
        """Cada posición debe tener exactamente 3 sensores (CAM + ESP + GPS)."""
        from collections import Counter
        conteo = Counter(s["posicion"] for s in sensores_5x5)
        incorrectas = {pos: n for pos, n in conteo.items() if n != 3}
        assert incorrectas == {}, (
            f"Posiciones con ≠ 3 sensores: {incorrectas}"
        )

    def test_ambos_ejes_presentes(self, sensores_5x5):
        """Para cada intersección A1-E5 deben existir tanto _NS como _EO."""
        posiciones = {s["posicion"] for s in sensores_5x5}
        letras = "ABCDE"
        for letra in letras:
            for col in range(1, 6):
                for eje in ("NS", "EO"):
                    pos = f"INT_{letra}{col}_{eje}"
                    assert pos in posiciones, f"Falta la posición: {pos}"


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-02e: Campos requeridos en cada sensor
# ──────────────────────────────────────────────────────────────────────────────

class TestCamposRequeridos:
    CAMPOS_BASE = {"sensor_id", "tipo", "posicion", "frecuencia_seg"}

    def test_todos_tienen_campos_base(self, sensores_5x5):
        for s in sensores_5x5:
            faltantes = self.CAMPOS_BASE - s.keys()
            assert faltantes == set(), (
                f"Sensor {s.get('sensor_id')} sin campos: {faltantes}"
            )

    def test_frecuencia_es_entero_positivo(self, sensores_5x5):
        for s in sensores_5x5:
            assert isinstance(s["frecuencia_seg"], int) and s["frecuencia_seg"] > 0, (
                f"Frecuencia inválida en {s['sensor_id']}: {s['frecuencia_seg']}"
            )
