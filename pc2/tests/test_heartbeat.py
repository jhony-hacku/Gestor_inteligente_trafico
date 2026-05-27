"""
pc2/tests/test_heartbeat.py
=============================
Pruebas unitarias — PT-U-03: EstadoNodos (Heartbeat).

Verifica el comportamiento thread-safe de EstadoNodos:
  - Valores iniciales optimistas
  - Actualización correcta de PC1 y PC3
  - Lógica combinada pc3_disponible (vivo AND db_ok)
  - Concurrencia: múltiples hilos leyendo y escribiendo
"""

import sys
import threading
from pathlib import Path

import pytest

PC2_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PC2_DIR))

from servicios.heartbeat import EstadoNodos


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-03a: Valores iniciales
# ──────────────────────────────────────────────────────────────────────────────

class TestEstadoNodosInicial:
    def test_pc1_disponible_por_defecto(self):
        """Estado inicial de PC1 debe ser True (optimista)."""
        en = EstadoNodos()
        assert en.pc1_disponible is True

    def test_pc3_disponible_por_defecto(self):
        """Estado inicial de PC3 debe ser True (optimista)."""
        en = EstadoNodos()
        assert en.pc3_disponible is True

    def test_resumen_inicial_indica_ok(self):
        """El resumen inicial debe indicar ambos nodos OK."""
        en = EstadoNodos()
        resumen = en.resumen()
        assert "PC1=OK" in resumen
        assert "PC3=OK" in resumen


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-03b: Actualización PC1
# ──────────────────────────────────────────────────────────────────────────────

class TestActualizarPC1:
    def test_marcar_pc1_caido(self):
        en = EstadoNodos()
        en.actualizar_pc1(False)
        assert en.pc1_disponible is False

    def test_marcar_pc1_disponible(self):
        en = EstadoNodos()
        en.actualizar_pc1(False)
        en.actualizar_pc1(True)
        assert en.pc1_disponible is True

    def test_pc1_no_afecta_pc3(self):
        """Cambiar PC1 no debe alterar el estado de PC3."""
        en = EstadoNodos()
        en.actualizar_pc1(False)
        assert en.pc3_disponible is True

    def test_resumen_refleja_caida_pc1(self):
        en = EstadoNodos()
        en.actualizar_pc1(False)
        assert "PC1=CAÍDO" in en.resumen()


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-03c: Actualización PC3 — lógica combinada
# ──────────────────────────────────────────────────────────────────────────────

class TestActualizarPC3:
    @pytest.mark.parametrize("vivo,db_ok,esperado", [
        (True,  True,  True),   # nodo vivo + BD OK → disponible
        (True,  False, False),  # nodo vivo pero BD caída → no disponible
        (False, True,  False),  # nodo caído aunque BD OK → no disponible
        (False, False, False),  # nodo caído + BD caída → no disponible
    ])
    def test_pc3_disponible_logica_combinada(self, vivo, db_ok, esperado):
        """pc3_disponible = vivo AND db_ok."""
        en = EstadoNodos()
        en.actualizar_pc3(vivo, db_ok)
        assert en.pc3_disponible == esperado, (
            f"vivo={vivo}, db_ok={db_ok}: esperado={esperado}, "
            f"obtenido={en.pc3_disponible}"
        )

    def test_transicion_caida_a_recuperacion(self):
        """Verificar ciclo: OK → CAÍDO → OK."""
        en = EstadoNodos()
        assert en.pc3_disponible is True
        en.actualizar_pc3(False, False)
        assert en.pc3_disponible is False
        en.actualizar_pc3(True, True)
        assert en.pc3_disponible is True

    def test_pc3_no_afecta_pc1(self):
        """Cambiar PC3 no debe alterar el estado de PC1."""
        en = EstadoNodos()
        en.actualizar_pc3(False, False)
        assert en.pc1_disponible is True

    def test_resumen_refleja_caida_pc3(self):
        en = EstadoNodos()
        en.actualizar_pc3(False, False)
        resumen = en.resumen()
        assert "PC3=CAÍDO" in resumen

    def test_resumen_refleja_bd_caida(self):
        """PC3 vivo pero BD caída debe aparecer en el resumen."""
        en = EstadoNodos()
        en.actualizar_pc3(True, False)
        resumen = en.resumen()
        assert "DB=FALLO" in resumen


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-03d: Thread-safety — concurrencia
# ──────────────────────────────────────────────────────────────────────────────

class TestConcurrenciaEstadoNodos:
    def test_actualizaciones_concurrentes_no_producen_excepcion(self):
        """100 hilos actualizando simultáneamente no deben lanzar excepciones."""
        en = EstadoNodos()
        errores = []

        def escribir(idx):
            try:
                for _ in range(50):
                    en.actualizar_pc1(idx % 2 == 0)
                    en.actualizar_pc3(idx % 3 != 0, idx % 5 != 0)
            except Exception as e:
                errores.append(str(e))

        hilos = [threading.Thread(target=escribir, args=(i,)) for i in range(10)]
        for h in hilos:
            h.start()
        for h in hilos:
            h.join(timeout=5)

        assert errores == [], f"Errores de concurrencia: {errores}"

    def test_lecturas_concurrentes_no_producen_excepcion(self):
        """Lecturas y escrituras simultáneas no deben generar race conditions."""
        en = EstadoNodos()
        errores = []

        def leer():
            try:
                for _ in range(100):
                    _ = en.pc1_disponible
                    _ = en.pc3_disponible
                    _ = en.resumen()
            except Exception as e:
                errores.append(str(e))

        def escribir():
            for i in range(100):
                en.actualizar_pc1(i % 2 == 0)
                en.actualizar_pc3(i % 3 != 0, True)

        t_lector  = threading.Thread(target=leer)
        t_escritor = threading.Thread(target=escribir)
        t_lector.start()
        t_escritor.start()
        t_lector.join(timeout=5)
        t_escritor.join(timeout=5)

        assert errores == [], f"Errores de concurrencia en lectura: {errores}"

    def test_valor_final_es_consistente_tras_concurrencia(self):
        """Después de que todos los hilos terminen el estado debe ser legible."""
        en = EstadoNodos()

        def forzar_caida():
            for _ in range(50):
                en.actualizar_pc1(False)
                en.actualizar_pc3(False, False)

        def forzar_ok():
            for _ in range(50):
                en.actualizar_pc1(True)
                en.actualizar_pc3(True, True)

        t1 = threading.Thread(target=forzar_caida)
        t2 = threading.Thread(target=forzar_ok)
        t1.start(); t2.start()
        t1.join(); t2.join()

        # Después de concurrencia el valor debe ser bool (sin corrupción)
        assert isinstance(en.pc1_disponible, bool)
        assert isinstance(en.pc3_disponible, bool)
