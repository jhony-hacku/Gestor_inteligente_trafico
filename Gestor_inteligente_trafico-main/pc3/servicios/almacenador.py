"""
servicios/almacenador.py
========================
Hilo almacenador de base de datos principal (SQLite).

Consume eventos de la cola_db y los escribe en la base de datos
principal de PC3 (db/trafico_principal.db). Es la fuente de
verdad del sistema.

Esquema de tablas:
  eventos    - historico completo de todos los eventos de sensores
  estados    - ultimo estado conocido por interseccion (upsert)
  semaforos  - historico de comandos de semaforo inferidos

Patron de escritura:
  - Acumula hasta 50 eventos o hasta 2 segundos y los escribe
    en un solo commit (batch insert) para mayor rendimiento.
"""

import json
import queue
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path


class Almacenador(threading.Thread):
    """
    Hilo de escritura en la base de datos principal de PC3.

    Parametros
    ----------
    config : dict
        Configuracion de PC3 (db.ruta).
    cola_db : queue.Queue
        Cola compartida con el receptor de eventos.
    stop_event : threading.Event
        Evento de parada compartido.
    """

    BATCH_SIZE    = 50    # escribir cada N eventos
    BATCH_TIMEOUT = 2.0   # o cada N segundos (lo que ocurra primero)

    def __init__(self, config: dict, cola_db: queue.Queue,
                 stop_event: threading.Event) -> None:
        super().__init__(name="Almacenador-DB", daemon=True)
        self.config     = config
        self.cola_db    = cola_db
        self.stop_event = stop_event
        self._conn: sqlite3.Connection | None = None
        self._total_escritos = 0

    # ------------------------------------------------------------------
    # Inicializacion de la base de datos
    # ------------------------------------------------------------------

    def _init_db(self) -> sqlite3.Connection:
        """Crea el archivo DB y las tablas si no existen."""
        base = Path(__file__).parent.parent
        ruta = base / self.config["db"]["ruta"]
        ruta.parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(str(ruta), check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")

        # Tabla de eventos completos (historico)
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
                timestamp_ingreso TEXT
            )
        """)

        # Indices para consultas rapidas
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_eventos_posicion
            ON eventos (posicion)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_eventos_timestamp
            ON eventos (timestamp)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_eventos_estado
            ON eventos (estado_trafico)
        """)

        # Tabla de estado actual por interseccion
        conn.execute("""
            CREATE TABLE IF NOT EXISTS estados (
                posicion         TEXT PRIMARY KEY,
                estado_trafico   TEXT,
                motivo           TEXT,
                ultimo_sensor    TEXT,
                timestamp_cambio TEXT
            )
        """)

        # Tabla de comandos de semaforo inferidos
        conn.execute("""
            CREATE TABLE IF NOT EXISTS semaforos (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                posicion         TEXT,
                estado           TEXT,
                motivo           TEXT,
                sensor_origen    TEXT,
                timestamp        TEXT
            )
        """)

        conn.commit()
        print(f"[ALMACENADOR] Base de datos lista en: {ruta}")
        return conn

    # ------------------------------------------------------------------
    # Escritura de datos
    # ------------------------------------------------------------------

    @staticmethod
    def _ts_now() -> str:
        return datetime.now(timezone.utc).astimezone().isoformat(timespec="microseconds")

    def _escribir_batch(self, batch: list[dict]) -> None:
        """Escribe un lote de eventos en la base de datos en una sola transaccion."""
        ts_ingreso = self._ts_now()
        datos_eventos  = []
        datos_estados  = []
        datos_semaforos = []

        for ev in batch:
            datos_json = json.dumps(ev, ensure_ascii=False)

            # Insertar en historico
            datos_eventos.append((
                ev.get("sensor_id"),
                ev.get("tipo"),
                ev.get("posicion"),
                ev.get("timestamp"),
                ev.get("estado_trafico"),
                ev.get("motivo"),
                datos_json,
                ts_ingreso,
            ))

            # Upsert estado por interseccion
            datos_estados.append((
                ev.get("posicion"),
                ev.get("estado_trafico"),
                ev.get("motivo"),
                ev.get("sensor_id"),
                ev.get("timestamp_proceso", ev.get("timestamp")),
            ))

            # Registrar cambio de semaforo si hay estado_trafico
            if ev.get("estado_trafico"):
                datos_semaforos.append((
                    ev.get("posicion"),
                    ev.get("estado_trafico"),
                    ev.get("motivo"),
                    ev.get("sensor_id"),
                    ev.get("timestamp_proceso", ev.get("timestamp")),
                ))

        self._conn.executemany("""
            INSERT INTO eventos
              (sensor_id, tipo, posicion, timestamp, estado_trafico,
               motivo, datos_json, timestamp_ingreso)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, datos_eventos)

        self._conn.executemany("""
            INSERT INTO estados
              (posicion, estado_trafico, motivo, ultimo_sensor, timestamp_cambio)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(posicion) DO UPDATE SET
                estado_trafico   = excluded.estado_trafico,
                motivo           = excluded.motivo,
                ultimo_sensor    = excluded.ultimo_sensor,
                timestamp_cambio = excluded.timestamp_cambio
        """, datos_estados)

        if datos_semaforos:
            self._conn.executemany("""
                INSERT INTO semaforos
                  (posicion, estado, motivo, sensor_origen, timestamp)
                VALUES (?, ?, ?, ?, ?)
            """, datos_semaforos)

        self._conn.commit()
        self._total_escritos += len(batch)

    # ------------------------------------------------------------------
    # Ciclo principal
    # ------------------------------------------------------------------

    def run(self) -> None:
        self._conn = self._init_db()
        batch: list[dict] = []
        ultimo_flush = __import__("time").time()

        print("[ALMACENADOR] Listo para recibir eventos de PC2...")

        while not self.stop_event.is_set():
            ahora = __import__("time").time()
            espera = max(0.0, self.BATCH_TIMEOUT - (ahora - ultimo_flush))

            try:
                evento = self.cola_db.get(timeout=espera)
                batch.append(evento)
            except queue.Empty:
                pass

            # Escribir si el batch esta lleno o paso el timeout
            if len(batch) >= self.BATCH_SIZE or (
                batch and (__import__("time").time() - ultimo_flush) >= self.BATCH_TIMEOUT
            ):
                try:
                    self._escribir_batch(batch)
                    print(f"[ALMACENADOR] +{len(batch)} eventos "
                          f"| Total: {self._total_escritos}")
                    batch.clear()
                    ultimo_flush = __import__("time").time()
                except Exception as e:
                    print(f"[ALMACENADOR] Error al escribir: {e}")

        # Escribir lo que quede al cerrar
        if batch:
            try:
                self._escribir_batch(batch)
                print(f"[ALMACENADOR] Flush final: {len(batch)} eventos")
            except Exception as e:
                print(f"[ALMACENADOR] Error en flush final: {e}")

        if self._conn:
            self._conn.close()

        print(f"[ALMACENADOR] Detenido. Total escritos: {self._total_escritos}")
