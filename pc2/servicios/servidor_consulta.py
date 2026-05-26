"""
servicios/servidor_consulta.py
==============================
Hilo servidor de consultas de réplica (patrón REP).

Propósito:
  Exponer un canal secundario de lectura sobre la réplica SQLite local
  (replica/trafico.db) para que el monitor de PC3 pueda consultarla
  cuando su base de datos principal (trafico_principal.db) no está
  disponible — failover transparente de persistencia.

Puerto por defecto: 5564 (configurable en pc2_config.json).

Protocolo de petición (JSON):
  { "tipo": "estados" }   ->  lista de estados por intersección
  { "tipo": "resumen" }   ->  conteo por estado + total de eventos

Protocolo de respuesta (JSON):
  # estados
  { "status": "ok",
    "filas": [[posicion, estado_trafico, motivo, ultimo_sensor, ts], ...] }

  # resumen
  { "status": "ok",
    "resumen": {"NORMAL": 5, "CONGESTION": 2, "_total_eventos": 80} }

  # error
  { "status": "error", "mensaje": "descripción del error" }

Notas de compatibilidad:
  La réplica de PC2 no almacena las columnas 'motivo' ni 'ultimo_sensor'
  en la tabla 'estados' (schema de Persistencia). Se retornan como None
  para que la visualización del monitor de PC3 muestre "N/A" en esos
  campos sin romper el formato de la tabla.
"""

import json
import sqlite3
import threading
from pathlib import Path

import zmq


# Ruta a la réplica SQLite generada por Persistencia
DB_PATH = Path(__file__).parent.parent / "replica" / "trafico.db"


class ServidorConsulta(threading.Thread):
    """
    Hilo REP que sirve consultas de lectura sobre la réplica SQLite de PC2.

    Parámetros
    ----------
    config : dict
        Configuración de PC2.  Se lee ``config["servidor_consulta"]["rep_port"]``.
    stop_event : threading.Event
        Evento de parada compartido con los demás servicios de PC2.
    """

    def __init__(self, config: dict, stop_event: threading.Event) -> None:
        super().__init__(name="Servidor-Consulta", daemon=True)
        self.config     = config
        self.stop_event = stop_event

    # ------------------------------------------------------------------
    # Consultas a la réplica local
    # ------------------------------------------------------------------

    def _consultar_estados(self) -> list[list]:
        """
        Retorna los estados actuales de la réplica como lista de listas.

        Columnas: [posicion, estado_trafico, motivo, ultimo_sensor, timestamp_cambio]
        'motivo' y 'ultimo_sensor' son None (ausentes en la réplica) para mantener
        compatibilidad con el formato esperado por el monitor de PC3.
        """
        if not DB_PATH.exists():
            return []
        try:
            conn = sqlite3.connect(str(DB_PATH), timeout=5)
            rows = conn.execute("""
                SELECT posicion, estado_trafico, NULL, NULL, timestamp_cambio
                FROM estados
                ORDER BY posicion
            """).fetchall()
            conn.close()
            return [list(r) for r in rows]
        except Exception as exc:
            print(f"[CONSULTA] Error leyendo estados de réplica: {exc}")
            return []

    def _consultar_resumen(self) -> dict:
        """
        Retorna el conteo de intersecciones por estado y el total de eventos.
        """
        if not DB_PATH.exists():
            return {}
        try:
            conn  = sqlite3.connect(str(DB_PATH), timeout=5)
            filas = conn.execute("""
                SELECT estado_trafico, COUNT(*) AS n
                FROM estados
                GROUP BY estado_trafico
            """).fetchall()
            total = conn.execute("SELECT COUNT(*) FROM eventos").fetchone()[0]
            conn.close()
            resumen = {f[0]: f[1] for f in filas}
            resumen["_total_eventos"] = total
            return resumen
        except Exception as exc:
            print(f"[CONSULTA] Error leyendo resumen de réplica: {exc}")
            return {}

    # ------------------------------------------------------------------
    # Ciclo principal
    # ------------------------------------------------------------------

    def run(self) -> None:
        ctx  = zmq.Context()
        sock = ctx.socket(zmq.REP)
        port = self.config["servidor_consulta"]["rep_port"]
        sock.bind(f"tcp://*:{port}")
        # Timeout corto para poder verificar stop_event sin bloquearse
        sock.setsockopt(zmq.RCVTIMEO, 1000)

        print(f"[CONSULTA] Servidor de réplica REP escuchando en tcp://*:{port}")

        while not self.stop_event.is_set():
            try:
                msg  = sock.recv_json()
                tipo = msg.get("tipo", "")

                if tipo == "estados":
                    sock.send_json({
                        "status": "ok",
                        "filas":  self._consultar_estados(),
                    })

                elif tipo == "resumen":
                    sock.send_json({
                        "status":  "ok",
                        "resumen": self._consultar_resumen(),
                    })

                else:
                    sock.send_json({
                        "status":  "error",
                        "mensaje": (
                            f"Tipo de consulta desconocido: '{tipo}'. "
                            "Use 'estados' o 'resumen'."
                        ),
                    })

            except zmq.Again:
                # Timeout normal — volver a verificar stop_event
                continue

            except zmq.ZMQError as exc:
                if not self.stop_event.is_set():
                    print(f"[CONSULTA] Error ZMQ: {exc}")

            except Exception as exc:
                if not self.stop_event.is_set():
                    print(f"[CONSULTA] Error inesperado: {exc}")
                    try:
                        sock.send_json({"status": "error", "mensaje": str(exc)})
                    except Exception:
                        pass

        sock.close()
        ctx.term()
        print("[CONSULTA] Servidor de consulta detenido.")
