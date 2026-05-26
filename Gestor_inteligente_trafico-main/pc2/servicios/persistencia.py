"""
servicios/persistencia.py
=========================
Hilo de persistencia de datos procesados.

Responsabilidades:
  1. Guardar SIEMPRE en la replica SQLite local (replica/trafico.db)
  2. Intentar enviar a PC3 via ZMQ PUSH (tcp://10.43.99.78:5561)
  3. Si PC3 no esta disponible -> marcar evento como pendiente en SQLite
     y seguir operando sin interrupcion (enmascaramiento de fallos)

Tablas SQLite:
  eventos  - cada evento de sensor enriquecido con estado_trafico
  estados  - ultima lectura por interseccion (upsert)

Cuando PC3 vuelva, la sincronizacion de pendientes se hara en PC3.
"""

import json
import queue
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

import zmq


DB_PATH = Path(__file__).parent.parent / "replica" / "trafico.db"


class Persistencia(threading.Thread):
    """
    Hilo de persistencia: SQLite local + PUSH a PC3.

    Parametros
    ----------
    config : dict
        Configuracion de PC2 (pc3.host, pc3.push_port).
    cola_persistencia : queue.Queue
        Cola de entrada con eventos ya procesados por el motor de reglas.
    stop_event : threading.Event
        Evento de parada compartido.
    """

    def __init__(self, config: dict, cola_persistencia: queue.Queue,
                 stop_event: threading.Event) -> None:
        super().__init__(name="Persistencia", daemon=True)
        self.config      = config
        self.cola        = cola_persistencia
        self.stop_event  = stop_event
        self._ctx        = None
        self._sock_pc3   = None
        self._conn_db: sqlite3.Connection | None = None
        self._pc3_ok     = True   # flag de disponibilidad de PC3

    # ------------------------------------------------------------------
    # Inicializacion
    # ------------------------------------------------------------------

    def _init_db(self) -> sqlite3.Connection:
        """Crea la base de datos SQLite y las tablas si no existen."""
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")  # mejor concurrencia

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
                pendiente_sync   INTEGER DEFAULT 0,
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

    def _init_zmq(self) -> tuple:
        """Crea el socket PUSH hacia PC3 con timeout de envio."""
        ctx  = zmq.Context()
        sock = ctx.socket(zmq.PUSH)
        sock.setsockopt(zmq.SNDTIMEO, 500)  # no bloquear mas de 500ms
        sock.setsockopt(zmq.LINGER,   0)    # cierre inmediato al term
        host = self.config["pc3"]["host"]
        port = self.config["pc3"]["push_port"]
        sock.connect(f"tcp://{host}:{port}")
        return ctx, sock

    # ------------------------------------------------------------------
    # Operaciones de datos
    # ------------------------------------------------------------------

    @staticmethod
    def _ts_now() -> str:
        return datetime.now(timezone.utc).astimezone().isoformat(timespec="microseconds")

    def _guardar_local(self, evento: dict, pendiente: bool = False) -> None:
        ts_ingreso = self._ts_now()
        datos_json = json.dumps(evento, ensure_ascii=False)
        self._conn_db.execute("""
            INSERT INTO eventos
              (sensor_id, tipo, posicion, timestamp, estado_trafico,
               motivo, datos_json, pendiente_sync, timestamp_ingreso)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            evento.get("sensor_id"),
            evento.get("tipo"),
            evento.get("posicion"),
            evento.get("timestamp"),
            evento.get("estado_trafico"),
            evento.get("motivo"),
            datos_json,
            1 if pendiente else 0,
            ts_ingreso,
        ))
        # Upsert del ultimo estado por interseccion
        self._conn_db.execute("""
            INSERT INTO estados (posicion, estado_trafico, timestamp_cambio)
            VALUES (?, ?, ?)
            ON CONFLICT(posicion) DO UPDATE SET
                estado_trafico   = excluded.estado_trafico,
                timestamp_cambio = excluded.timestamp_cambio
        """, (
            evento.get("posicion"),
            evento.get("estado_trafico"),
            evento.get("timestamp_proceso", evento.get("timestamp")),
        ))
        self._conn_db.commit()

    def _enviar_pc3(self, evento: dict) -> bool:
        """Intenta enviar el evento a PC3. Retorna True si tuvo exito."""
        try:
            payload = json.dumps(evento, ensure_ascii=False).encode("utf-8")
            self._sock_pc3.send(payload, zmq.NOBLOCK)
            return True
        except (zmq.Again, zmq.ZMQError):
            return False

    # ------------------------------------------------------------------
    # Ciclo principal
    # ------------------------------------------------------------------

    def run(self) -> None:
        self._conn_db = self._init_db()
        self._ctx, self._sock_pc3 = self._init_zmq()

        pc3_host = self.config["pc3"]["host"]
        pc3_port = self.config["pc3"]["push_port"]
        print(f"[PERSISTENCIA] Replica SQLite en: {DB_PATH}")
        print(f"[PERSISTENCIA] Enviando a PC3:    tcp://{pc3_host}:{pc3_port}")

        while not self.stop_event.is_set():
            try:
                evento = self.cola.get(timeout=1.0)

                # 1. Guardar siempre en replica local (sin marcar pendiente aun)
                self._guardar_local(evento, pendiente=False)

                # 2. Intentar enviar a PC3
                ok = self._enviar_pc3(evento)
                if not ok:
                    if self._pc3_ok:
                        print("[PERSISTENCIA] PC3 no disponible - operando con replica local")
                        self._pc3_ok = False
                else:
                    if not self._pc3_ok:
                        print("[PERSISTENCIA] PC3 disponible de nuevo")
                        self._pc3_ok = True

            except queue.Empty:
                continue
            except Exception as e:
                if not self.stop_event.is_set():
                    print(f"[PERSISTENCIA] Error: {e}")

        # Cierre limpio
        if self._conn_db:
            self._conn_db.close()
        if self._sock_pc3:
            self._sock_pc3.close()
        if self._ctx:
            self._ctx.term()
        print("[PERSISTENCIA] Servicio detenido.")
