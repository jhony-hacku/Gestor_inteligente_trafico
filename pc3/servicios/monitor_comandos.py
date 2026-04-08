"""
servicios/monitor_comandos.py
==============================
Interfaz de monitoreo y comandos de PC3.

Dos funciones en un hilo:
  1. Mostrar periodicamente el resumen del estado de la ciudad
     consultando la base de datos local.
  2. Enviar comandos manuales a PC2 via REQ/REP (puerto 5563)
     cuando el operador los ingresa por consola.

Comandos disponibles (interactivos):
  ola <interseccion>       -> OLA_VERDE en una interseccion
                              ej: ola INT_C3
  normal <interseccion>    -> Forzar NORMAL
  estado                   -> Mostrar tabla de estados actuales
  ayuda                    -> Mostrar esta ayuda
  salir                    -> Terminar PC3
"""

import sqlite3
import threading
import time
from pathlib import Path

import zmq


class MonitorComandos(threading.Thread):
    """
    Hilo de monitoreo periodico y envio de comandos a PC2.

    Parametros
    ----------
    config : dict
        Configuracion de PC3 (pc2.host, pc2.rep_port, db.ruta).
    stop_event : threading.Event
        Evento de parada compartido.
    """

    INTERVALO_MONITOR = 30   # segundos entre reportes automaticos

    def __init__(self, config: dict, stop_event: threading.Event) -> None:
        super().__init__(name="Monitor-Comandos", daemon=True)
        self.config     = config
        self.stop_event = stop_event
        self._ctx       = None
        self._sock_pc2  = None

    # ------------------------------------------------------------------
    # ZMQ: envio de comandos a PC2
    # ------------------------------------------------------------------

    def _conectar_pc2(self) -> bool:
        """Crea el socket REQ hacia PC2. Retorna True si OK."""
        try:
            self._ctx      = zmq.Context()
            self._sock_pc2 = self._ctx.socket(zmq.REQ)
            self._sock_pc2.setsockopt(zmq.RCVTIMEO, 3000)
            self._sock_pc2.setsockopt(zmq.SNDTIMEO, 3000)
            self._sock_pc2.setsockopt(zmq.LINGER,   0)
            host = self.config["pc2"]["host"]
            port = self.config["pc2"]["rep_port"]
            self._sock_pc2.connect(f"tcp://{host}:{port}")
            print(f"[MONITOR] Conectado a PC2 tcp://{host}:{port}")
            return True
        except Exception as e:
            print(f"[MONITOR] No se pudo conectar a PC2: {e}")
            return False

    def _enviar_comando_pc2(self, comando: str, posicion: str,
                             duracion: int = 60) -> str:
        """Envia un comando a PC2 y retorna la respuesta."""
        if not self._sock_pc2:
            return "Error: sin conexion con PC2"
        try:
            self._sock_pc2.send_json({
                "comando":     comando.upper(),
                "posicion":    posicion.upper(),
                "duracion_seg": duracion,
            })
            resp = self._sock_pc2.recv_json()
            return resp.get("mensaje", str(resp))
        except zmq.Again:
            # Reiniciar el socket REQ (queda en estado invalido tras timeout)
            self._sock_pc2.close()
            self._conectar_pc2()
            return "Timeout: PC2 no respondio"
        except Exception as e:
            return f"Error: {e}"

    # ------------------------------------------------------------------
    # SQLite: consultas de estado
    # ------------------------------------------------------------------

    def _ruta_db(self) -> Path:
        base = Path(__file__).parent.parent
        return base / self.config["db"]["ruta"]

    def _leer_estados(self) -> list[tuple]:
        """Retorna los estados actuales de todas las intersecciones."""
        ruta = self._ruta_db()
        if not ruta.exists():
            return []
        try:
            conn = sqlite3.connect(str(ruta))
            rows = conn.execute("""
                SELECT posicion, estado_trafico, motivo, ultimo_sensor, timestamp_cambio
                FROM estados
                ORDER BY posicion
            """).fetchall()
            conn.close()
            return rows
        except Exception:
            return []

    def _leer_resumen(self) -> dict:
        """Retorna conteo de intersecciones por estado."""
        ruta = self._ruta_db()
        if not ruta.exists():
            return {}
        try:
            conn = sqlite3.connect(str(ruta))
            rows = conn.execute("""
                SELECT estado_trafico, COUNT(*) as n
                FROM estados
                GROUP BY estado_trafico
            """).fetchall()
            total = conn.execute("SELECT COUNT(*) FROM eventos").fetchone()[0]
            conn.close()
            resumen = {r[0]: r[1] for r in rows}
            resumen["_total_eventos"] = total
            return resumen
        except Exception:
            return {}

    # ------------------------------------------------------------------
    # Visualizacion
    # ------------------------------------------------------------------

    def _imprimir_estados(self) -> None:
        rows = self._leer_estados()
        if not rows:
            print("[MONITOR] Base de datos vacia aun.")
            return
        print()
        print(f"  {'POSICION':<10} {'ESTADO':<12} {'SENSOR':<12} {'TIMESTAMP':<22} MOTIVO")
        print("  " + "-" * 90)
        for r in rows:
            posicion, estado, motivo, sensor, ts = r
            ts_corto = (ts or "")[:19]
            motivo_corto = (motivo or "")[:40]
            print(f"  {posicion:<10} {estado:<12} {(sensor or 'N/A'):<12} {ts_corto:<22} {motivo_corto}")
        print()

    def _imprimir_resumen(self) -> None:
        r = self._leer_resumen()
        if not r:
            return
        ts = time.strftime("%H:%M:%S")
        normal     = r.get("NORMAL",     0)
        congestion = r.get("CONGESTION", 0)
        ola_verde  = r.get("OLA_VERDE",  0)
        total_ev   = r.get("_total_eventos", 0)
        print(f"\n[MONITOR] {ts} | Intersecciones: "
              f"NORMAL={normal} | CONGESTION={congestion} | OLA_VERDE={ola_verde} "
              f"| Eventos en BD={total_ev}")

    # ------------------------------------------------------------------
    # Consola interactiva
    # ------------------------------------------------------------------

    def _procesar_comando_consola(self, linea: str) -> bool:
        """Procesa una linea de la consola. Retorna False si debe salir."""
        partes = linea.strip().split()
        if not partes:
            return True

        cmd = partes[0].lower()

        if cmd == "salir":
            self.stop_event.set()
            return False

        elif cmd == "estado":
            self._imprimir_estados()

        elif cmd == "ayuda":
            print()
            print("  Comandos disponibles:")
            print("    ola <INT_XX>     -> Activar OLA_VERDE en una interseccion")
            print("    normal <INT_XX>  -> Forzar NORMAL en una interseccion")
            print("    estado           -> Ver tabla de estados actuales")
            print("    ayuda            -> Esta ayuda")
            print("    salir            -> Detener PC3")
            print()

        elif cmd in ("ola", "normal") and len(partes) >= 2:
            posicion = partes[1].upper()
            comando  = "OLA_VERDE" if cmd == "ola" else "NORMAL"
            print(f"[MONITOR] Enviando {comando} a {posicion}...")
            resp = self._enviar_comando_pc2(comando, posicion)
            print(f"[MONITOR] Respuesta PC2: {resp}")

        else:
            print(f"[MONITOR] Comando desconocido: '{linea}'. Escribe 'ayuda'.")

        return True

    # ------------------------------------------------------------------
    # Ciclo principal del hilo
    # ------------------------------------------------------------------

    def run(self) -> None:
        self._conectar_pc2()

        print()
        print("[MONITOR] Monitor activo. Comandos: ola <INT_XX> | normal <INT_XX> | estado | ayuda | salir")
        print()

        ultimo_reporte = 0.0

        while not self.stop_event.is_set():
            # Reporte periodico automatico
            ahora = time.time()
            if ahora - ultimo_reporte >= self.INTERVALO_MONITOR:
                self._imprimir_resumen()
                ultimo_reporte = ahora

            time.sleep(1)

        if self._sock_pc2:
            self._sock_pc2.close()
        if self._ctx:
            self._ctx.term()
        print("[MONITOR] Monitor detenido.")
