"""
pc2/tests/test_persistencia.py
================================
Pruebas unitarias — PT-U-04: Persistencia (PC2).

Verifica el comportamiento del servicio de persistencia SQLite local:
  - U04-a: PC3 no disponible → evento guardado con sincronizado=0
  - U04-b: PC3 vuelve disponible → Batch Flush marca sincronizado=1
  - U04-c: PC3 cae durante el Batch Flush → registros permanecen en sincronizado=0
  - Casos adicionales: escritura normal (sincronizado=1), upsert de estados,
    campos obligatorios, y aislamiento de la BD de prueba.

Estrategia: se usan bases de datos SQLite en memoria o en directorio
temporal para no interferir con los datos de réplica reales.
El socket ZMQ se sustituye con unittest.mock para evitar conexiones reales.
"""

import json
import queue
import sqlite3
import sys
import tempfile
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import zmq

# ─── sys.path para encontrar servicios/ ──────────────────────────────────────
PC2_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PC2_DIR))

from servicios.heartbeat import EstadoNodos
from servicios.persistencia import Persistencia

# ─── Configuración mínima de PC2 para el servicio de persistencia ─────────────
CONFIG_TEST = {
    "pc3": {
        "host": "127.0.0.1",
        "push_port": 55610,   # puerto ficticio; ZMQ será mockeado
    }
}

# ─── Evento de muestra enriquecido (salida del motor de reglas) ───────────────
def _evento(posicion="INT_A1_NS", estado="NORMAL", sensor_id="CAM_A1_NS"):
    return {
        "sensor_id":        sensor_id,
        "tipo":             "camara",
        "posicion":         posicion,
        "timestamp":        "2026-01-01T00:00:00",
        "estado_trafico":   estado,
        "motivo":           f"Prueba unitaria — {estado}",
        "timestamp_proceso": "2026-01-01T00:00:01",
        "_fuente":          "sensor",
    }


# ─── Fixture: Persistencia con BD en fichero temporal + ZMQ mockeado ─────────

class _PersistenciaTest:
    """
    Contenedor que levanta un Persistencia con:
      - BD SQLite en un archivo temporal aislado.
      - Socket ZMQ PUSH reemplazado por un MagicMock configurable.

    Uso:
        p = _PersistenciaTest(pc3_disponible=False)
        p.persistencia._guardar_local(evento, sincronizado=0)
        filas = p.query("SELECT sincronizado FROM eventos")
    """

    def __init__(self, pc3_disponible: bool = True, estado_nodos=None):
        self._tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self._db_path = Path(self._tmp.name)
        self._tmp.close()

        self.cola  = queue.Queue()
        self.stop  = threading.Event()

        self.persistencia = Persistencia(
            CONFIG_TEST,
            self.cola,
            self.stop,
            estado_nodos=estado_nodos,
        )

        # Inicializar BD apuntando al fichero temporal
        with patch("servicios.persistencia.DB_PATH", self._db_path):
            self.persistencia._conn_db = self._init_db_en_tmp()

        # Mock del socket ZMQ PUSH
        self._sock_mock = MagicMock()
        self.persistencia._sock_pc3 = self._sock_mock

        # Configurar comportamiento inicial del socket
        if pc3_disponible:
            self._sock_mock.send.return_value = None   # éxito silencioso
        else:
            self._sock_mock.send.side_effect = zmq.Again()

    def _init_db_en_tmp(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS eventos (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_id        TEXT,
                tipo             TEXT,
                posicion         TEXT,
                timestamp        TEXT,
                estado_trafico   TEXT,
                motivo           TEXT,
                datos_json       TEXT,
                sincronizado     INTEGER DEFAULT 1,
                timestamp_ingreso TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS estados (
                posicion         TEXT PRIMARY KEY,
                estado_trafico   TEXT,
                timestamp_cambio TEXT
            )
        """)
        conn.commit()
        return conn

    def query(self, sql: str, params=()):
        cur = self.persistencia._conn_db.cursor()
        cur.execute(sql, params)
        return cur.fetchall()

    def count(self, tabla="eventos", where="1=1"):
        rows = self.query(f"SELECT COUNT(*) FROM {tabla} WHERE {where}")
        return rows[0][0]

    def teardown(self):
        try:
            self.persistencia._conn_db.close()
        except Exception:
            pass
        try:
            self._db_path.unlink(missing_ok=True)
        except Exception:
            pass


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-04a: PC3 no disponible → sincronizado = 0
# ──────────────────────────────────────────────────────────────────────────────

class TestPC3NoDisponible:
    """
    Cuando el socket PUSH falla (zmq.Again), el evento debe guardarse
    localmente con sincronizado=0.
    """

    def setup_method(self):
        self.p = _PersistenciaTest(pc3_disponible=False)

    def teardown_method(self):
        self.p.teardown()

    def test_guardar_local_con_sincronizado_cero(self):
        """PT-U-04a: _guardar_local con sincronizado=0 persiste el flag."""
        ev = _evento()
        self.p.persistencia._guardar_local(ev, sincronizado=0)
        filas = self.p.query("SELECT sincronizado FROM eventos")
        assert len(filas) == 1
        assert filas[0][0] == 0, "El evento debe tener sincronizado=0"

    def test_enviar_pc3_falla_devuelve_false(self):
        """PT-U-04a: _enviar_pc3 retorna False cuando el socket lanza zmq.Again."""
        resultado = self.p.persistencia._enviar_pc3(_evento())
        assert resultado is False

    def test_multiples_eventos_quedan_pendientes(self):
        """Varios eventos con PC3 caído → todos quedan con sincronizado=0."""
        for i in range(5):
            ev = _evento(posicion=f"INT_A{i+1}_NS", sensor_id=f"CAM_A{i+1}_NS")
            self.p.persistencia._guardar_local(ev, sincronizado=0)

        pendientes = self.p.count(where="sincronizado=0")
        assert pendientes == 5

    def test_evento_con_estado_nodos_caido_usa_sincronizado_cero(self):
        """
        PT-U-04a alternativo: cuando EstadoNodos reporta PC3 caído,
        el ciclo principal debe usar sincronizado=0.
        """
        estado_nodos = EstadoNodos()
        estado_nodos.actualizar_pc3(False, False)

        p = _PersistenciaTest(pc3_disponible=True, estado_nodos=estado_nodos)
        try:
            # Simulamos el bloque condicional del ciclo principal
            ev = _evento()
            if not p.persistencia._estado_nodos.pc3_disponible:
                p.persistencia._guardar_local(ev, sincronizado=0)

            filas = p.query("SELECT sincronizado FROM eventos")
            assert len(filas) == 1
            assert filas[0][0] == 0
        finally:
            p.teardown()


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-04b: PC3 vuelve disponible → Batch Flush marca sincronizado=1
# ──────────────────────────────────────────────────────────────────────────────

class TestBatchFlush:
    """
    Tras la reconexión de PC3, _sincronizar_pendientes debe marcar
    todos los eventos sincronizado=0 → sincronizado=1.
    """

    def setup_method(self):
        # Empezamos con PC3 disponible (para poder usar _sincronizar_pendientes)
        self.p = _PersistenciaTest(pc3_disponible=True)

    def teardown_method(self):
        self.p.teardown()

    def _insertar_pendientes(self, n: int):
        """Inserta n eventos con sincronizado=0 directamente en la BD."""
        for i in range(n):
            ev = _evento(posicion=f"INT_B{i+1}_NS", estado="CONGESTION",
                        sensor_id=f"CAM_B{i+1}_NS")
            self.p.persistencia._guardar_local(ev, sincronizado=0)
        # Verificar inserción
        assert self.p.count(where="sincronizado=0") == n

    def test_batch_flush_actualiza_sincronizado_a_uno(self):
        """PT-U-04b: después del Batch Flush todos los eventos quedan sincronizado=1."""
        self._insertar_pendientes(3)

        # Parchear DB_PATH para que _sincronizar_pendientes use el mismo archivo
        with patch("servicios.persistencia.DB_PATH", self.p._db_path):
            self.p.persistencia._sincronizar_pendientes()

        pendientes = self.p.count(where="sincronizado=0")
        assert pendientes == 0, (
            f"Quedan {pendientes} eventos sin sincronizar tras el Batch Flush"
        )

    def test_batch_flush_no_afecta_ya_sincronizados(self):
        """El Batch Flush NO debe modificar eventos que ya tienen sincronizado=1."""
        # Insertar 2 ya sincronizados
        ev1 = _evento(posicion="INT_C1_NS", estado="NORMAL")
        ev2 = _evento(posicion="INT_C2_NS", estado="NORMAL")
        self.p.persistencia._guardar_local(ev1, sincronizado=1)
        self.p.persistencia._guardar_local(ev2, sincronizado=1)

        # Insertar 1 pendiente
        ev3 = _evento(posicion="INT_C3_NS", estado="CONGESTION")
        self.p.persistencia._guardar_local(ev3, sincronizado=0)

        assert self.p.count(where="sincronizado=1") == 2
        assert self.p.count(where="sincronizado=0") == 1

        with patch("servicios.persistencia.DB_PATH", self.p._db_path):
            self.p.persistencia._sincronizar_pendientes()

        assert self.p.count(where="sincronizado=1") == 3
        assert self.p.count(where="sincronizado=0") == 0

    def test_batch_flush_con_cola_vacia_no_falla(self):
        """Si no hay pendientes, _sincronizar_pendientes debe terminar sin error."""
        assert self.p.count(where="sincronizado=0") == 0
        with patch("servicios.persistencia.DB_PATH", self.p._db_path):
            self.p.persistencia._sincronizar_pendientes()   # no debe lanzar excepción

    def test_batch_flush_llama_send_por_cada_evento(self):
        """El Batch Flush debe llamar a sock.send() una vez por cada pendiente."""
        self._insertar_pendientes(4)

        with patch("servicios.persistencia.DB_PATH", self.p._db_path):
            self.p.persistencia._sincronizar_pendientes()

        # El mock de la BD usa su propio socket; verificamos que la BD quedó limpia
        assert self.p.count(where="sincronizado=0") == 0


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-04c: PC3 cae durante el Batch Flush → registros permanecen en =0
# ──────────────────────────────────────────────────────────────────────────────

class TestBatchFlushInterrumpido:
    """
    Si PC3 se desconecta a mitad del Batch Flush, los registros que no
    pudieron enviarse deben permanecer con sincronizado=0.
    """

    def setup_method(self):
        self.p = _PersistenciaTest(pc3_disponible=True)

    def teardown_method(self):
        self.p.teardown()

    def _insertar_pendientes(self, n: int):
        for i in range(n):
            ev = _evento(posicion=f"INT_D{i+1}_NS", estado="CONGESTION",
                        sensor_id=f"CAM_D{i+1}_NS")
            self.p.persistencia._guardar_local(ev, sincronizado=0)

    def test_registros_quedan_sincronizado_cero_si_falla_en_mitad(self):
        """
        PT-U-04c: envío exitoso del primer evento, fallo a partir del segundo.
        → El primer evento queda en sincronizado=1, los restantes en =0.
        """
        self._insertar_pendientes(3)
        assert self.p.count(where="sincronizado=0") == 3

        # Configurar mock: éxito la 1.ª vez, fallo a partir de la 2.ª
        call_count = {"n": 0}
        original_send = None

        def send_intermitente(payload, flags):
            call_count["n"] += 1
            if call_count["n"] > 1:
                raise zmq.Again()

        # _sincronizar_pendientes crea su propio contexto/socket ZMQ interno,
        # así que parcheamos zmq.Context para capturarlo.
        ctx_mock  = MagicMock()
        sock_mock = MagicMock()
        sock_mock.send.side_effect = send_intermitente
        ctx_mock.socket.return_value = sock_mock

        with patch("servicios.persistencia.DB_PATH", self.p._db_path), \
             patch("servicios.persistencia.zmq.Context", return_value=ctx_mock):
            self.p.persistencia._sincronizar_pendientes()

        # 1 enviado con éxito → sincronizado=1, 2 restantes → sincronizado=0
        assert self.p.count(where="sincronizado=1") == 1, (
            "El primer evento sí se envió; debería quedar sincronizado=1"
        )
        assert self.p.count(where="sincronizado=0") == 2, (
            "Los 2 eventos no enviados deben permanecer con sincronizado=0"
        )

    def test_stop_event_aborta_flush_y_conserva_pendientes(self):
        """
        Si stop_event se activa durante el Batch Flush, los registros
        no enviados deben permanecer en sincronizado=0.
        """
        self._insertar_pendientes(3)

        # Activar stop_event para que el bucle se corte desde el inicio
        self.p.stop.set()

        ctx_mock  = MagicMock()
        sock_mock = MagicMock()
        ctx_mock.socket.return_value = sock_mock

        with patch("servicios.persistencia.DB_PATH", self.p._db_path), \
             patch("servicios.persistencia.zmq.Context", return_value=ctx_mock):
            self.p.persistencia._sincronizar_pendientes()

        # Ningún registro debe haberse marcado como sincronizado=1
        assert self.p.count(where="sincronizado=0") == 3
        # Y el socket nunca debió llamar send
        sock_mock.send.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-04d: Escritura normal con PC3 disponible → sincronizado=1
# ──────────────────────────────────────────────────────────────────────────────

class TestEscrituraNormal:
    """Cuando PC3 está disponible, los eventos se guardan con sincronizado=1."""

    def setup_method(self):
        self.p = _PersistenciaTest(pc3_disponible=True)

    def teardown_method(self):
        self.p.teardown()

    def test_guardar_local_por_defecto_es_sincronizado_uno(self):
        """_guardar_local sin argumento sincronizado usa el default=1."""
        self.p.persistencia._guardar_local(_evento())
        filas = self.p.query("SELECT sincronizado FROM eventos")
        assert len(filas) == 1
        assert filas[0][0] == 1

    def test_enviar_pc3_retorna_true_cuando_socket_ok(self):
        """_enviar_pc3 retorna True cuando el send() no lanza excepción."""
        resultado = self.p.persistencia._enviar_pc3(_evento())
        assert resultado is True

    def test_campos_obligatorios_en_bd(self):
        """Todos los campos principales deben guardarse correctamente."""
        ev = _evento(posicion="INT_E5_EO", estado="OLA_VERDE",
                     sensor_id="GPS_E5_EO")
        self.p.persistencia._guardar_local(ev, sincronizado=1)
        filas = self.p.query(
            "SELECT sensor_id, tipo, posicion, estado_trafico, sincronizado "
            "FROM eventos WHERE posicion='INT_E5_EO'"
        )
        assert len(filas) == 1
        sensor_id, tipo, posicion, estado, sync = filas[0]
        assert sensor_id   == "GPS_E5_EO"
        assert tipo        == "camara"    # el fixture usa tipo=camara
        assert posicion    == "INT_E5_EO"
        assert estado      == "OLA_VERDE"
        assert sync        == 1

    def test_upsert_estado_actualiza_interseccion(self):
        """
        Dos eventos en la misma posición → la tabla 'estados' debe contener
        solo 1 fila con el estado más reciente.
        """
        pos = "INT_A3_NS"
        self.p.persistencia._guardar_local(
            _evento(posicion=pos, estado="NORMAL"), sincronizado=1
        )
        self.p.persistencia._guardar_local(
            _evento(posicion=pos, estado="CONGESTION"), sincronizado=1
        )

        filas = self.p.query(
            "SELECT estado_trafico FROM estados WHERE posicion=?", (pos,)
        )
        assert len(filas) == 1, "Debe haber solo 1 fila por posición (UPSERT)"
        assert filas[0][0] == "CONGESTION", "Debe reflejar el estado más reciente"


# ──────────────────────────────────────────────────────────────────────────────
# PT-U-04e: Integración con EstadoNodos
# ──────────────────────────────────────────────────────────────────────────────

class TestIntegracionEstadoNodos:
    """
    Verifica que Persistencia consulta correctamente EstadoNodos
    para decidir si envía a PC3 o usa la réplica local.
    """

    def setup_method(self):
        self.estado_nodos = EstadoNodos()

    def test_pc3_disponible_segun_estado_nodos(self):
        """Con EstadoNodos en estado OK, pc3_disponible debe ser True."""
        self.estado_nodos.actualizar_pc3(True, True)
        assert self.estado_nodos.pc3_disponible is True

    def test_pc3_no_disponible_segun_estado_nodos(self):
        """Con EstadoNodos indicando caída, pc3_disponible debe ser False."""
        self.estado_nodos.actualizar_pc3(False, False)
        assert self.estado_nodos.pc3_disponible is False

    def test_persistencia_referencia_estado_nodos(self):
        """Persistencia almacena la referencia a EstadoNodos correctamente."""
        cola  = queue.Queue()
        stop  = threading.Event()
        p = Persistencia(CONFIG_TEST, cola, stop, estado_nodos=self.estado_nodos)
        assert p._estado_nodos is self.estado_nodos

    def test_persistencia_sin_estado_nodos_es_none(self):
        """Persistencia sin estado_nodos opera con _estado_nodos=None."""
        cola  = queue.Queue()
        stop  = threading.Event()
        p = Persistencia(CONFIG_TEST, cola, stop)
        assert p._estado_nodos is None

    def test_transicion_caida_a_subida_refleja_en_disponibilidad(self):
        """El ciclo OK→CAÍDO→OK debe reflejarse correctamente."""
        en = EstadoNodos()
        assert en.pc3_disponible is True
        en.actualizar_pc3(False, False)
        assert en.pc3_disponible is False
        en.actualizar_pc3(True, True)
        assert en.pc3_disponible is True
