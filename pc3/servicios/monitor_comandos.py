"""
servicios/monitor_comandos.py
==============================
Interfaz de monitoreo y comandos de PC3 con failover automático de consultas.

Dos funciones en un hilo:
  1. Mostrar periódicamente el resumen del estado de la ciudad consultando
     la base de datos local, con conmutación dinámica a la réplica de PC2
     si la DB principal no está disponible (failover transparente).
  2. Enviar comandos manuales a PC2 vía REQ/REP (puerto 5563) cuando el
     operador los ingresa por consola.

Conmutación Dinámica de Consultas (Failover):
  - Cada DB_RECHEQUEO_INTERVALO segundos (15 s) se verifica si la DB
    principal (trafico_principal.db) responde correctamente.
  - Si no está disponible, las consultas se redirigen de forma transparente
    al ServidorConsulta de PC2 (puerto 5564) usando un socket REQ dedicado.
  - El timeout de ese canal remoto es de CONSULTA_TIMEOUT_MS (30 000 ms).
    Tras un timeout el socket REQ se recrea automáticamente.
  - Cuando la DB local vuelve a estar disponible, la conmutación se revierte
    sola sin intervención manual.
  - El cambio de fuente de datos se indica en consola con un aviso explícito.

Comandos disponibles (interactivos):
  ola <interseccion>       -> OLA_VERDE en una interseccion
  normal <interseccion>    -> Forzar NORMAL
  estado                   -> Mostrar tabla de estados actuales
  ayuda                    -> Mostrar esta ayuda
  salir                    -> Terminar PC3
"""

import sqlite3
import threading
import time
from pathlib import Path
from typing import Callable, TypeVar

import zmq


# ---------------------------------------------------------------------------
T = TypeVar("T")
# ---------------------------------------------------------------------------


class MonitorComandos(threading.Thread):
    """
    Hilo de monitoreo periódico, envío de comandos y failover de consultas.

    Parámetros
    ----------
    config : dict
        Configuración de PC3.  Claves relevantes:
          config["pc2"]["host"]                      IP de PC2
          config["pc2"]["rep_port"]                  Puerto de comandos (5563)
          config["servidor_consulta_pc2"]["port"]    Puerto de consultas (5564)
          config["db"]["ruta"]                       Ruta relativa a la DB local
    stop_event : threading.Event
        Evento de parada compartido.
    """

    INTERVALO_MONITOR       = 30       # segundos entre reportes automáticos
    DB_RECHEQUEO_INTERVALO  = 15       # segundos de caché del estado de la DB
    CONSULTA_TIMEOUT_MS     = 30_000   # 30 s de timeout para el canal remoto

    def __init__(self, config: dict, stop_event: threading.Event) -> None:
        super().__init__(name="Monitor-Comandos", daemon=True)
        self.config     = config
        self.stop_event = stop_event

        # Sockets ZMQ (creados en run())
        self._ctx           = None
        self._sock_pc2      = None   # REQ → PC2:5563  (comandos de semáforo)
        self._sock_consulta = None   # REQ → PC2:5564  (failover de lectura)

        # Estado de salud de la DB local con caché de 15 s
        self._db_ok              = True
        self._ultimo_chequeo_db  = 0.0   # epoch del último chequeo real

    # ------------------------------------------------------------------
    # ZMQ: canal de comandos a PC2 (puerto 5563)
    # ------------------------------------------------------------------

    def _conectar_pc2(self) -> bool:
        """Crea el socket REQ hacia PC2:5563 (comandos). Retorna True si OK."""
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
        except Exception as exc:
            print(f"[MONITOR] No se pudo conectar a PC2: {exc}")
            return False

    def _enviar_comando_pc2(self, comando: str, posicion: str,
                             duracion: int = 60) -> str:
        """Envía un comando de semáforo a PC2 y retorna la respuesta."""
        if not self._sock_pc2:
            return "Error: sin conexión con PC2"
        try:
            self._sock_pc2.send_json({
                "comando":      comando.upper(),
                "posicion":     posicion.upper(),
                "duracion_seg": duracion,
            })
            resp = self._sock_pc2.recv_json()
            return resp.get("mensaje", str(resp))
        except zmq.Again:
            # REQ queda inválido tras timeout → recrear socket
            self._sock_pc2.close()
            self._conectar_pc2()
            return "Timeout: PC2 no respondió"
        except Exception as exc:
            return f"Error: {exc}"

    # ------------------------------------------------------------------
    # ZMQ: canal de consulta a PC2 (puerto 5564) — failover
    # ------------------------------------------------------------------

    def _conectar_consulta_pc2(self) -> None:
        """
        Crea o recrea el socket REQ hacia PC2:5564 (consultas de réplica).

        Se llama una vez en la inicialización del hilo y de nuevo tras cada
        timeout de 30 s, ya que el socket REQ queda en estado inválido tras
        un timeout y debe recrearse para poder enviar nuevas peticiones.
        """
        if self._sock_consulta:
            try:
                self._sock_consulta.close()
            except Exception:
                pass

        if not self._ctx:
            return

        sock = self._ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.RCVTIMEO, self.CONSULTA_TIMEOUT_MS)
        sock.setsockopt(zmq.SNDTIMEO, self.CONSULTA_TIMEOUT_MS)
        sock.setsockopt(zmq.LINGER,   0)
        cfg  = self.config.get("servidor_consulta_pc2", {})
        host = self.config["pc2"]["host"]
        port = cfg.get("port", 5564)
        sock.connect(f"tcp://{host}:{port}")
        self._sock_consulta = sock

    def _enviar_consulta_remota(self, tipo: str) -> dict | None:
        """
        Envía una petición al ServidorConsulta de PC2 y retorna el JSON.

        Retorna None si hay fallo o timeout.  En ese caso recrea el socket
        REQ para que el próximo intento parta de un estado limpio.
        """
        if not self._sock_consulta:
            return None
        try:
            self._sock_consulta.send_json({"tipo": tipo})
            return self._sock_consulta.recv_json()
        except zmq.Again:
            print(
                f"[MONITOR] ⚠ Timeout {self.CONSULTA_TIMEOUT_MS // 1000}s "
                "consultando réplica en PC2 — reconstruyendo canal"
            )
            self._conectar_consulta_pc2()   # recrear socket inválido
            return None
        except Exception as exc:
            if not self.stop_event.is_set():
                print(f"[MONITOR] Error en consulta remota: {exc}")
            self._conectar_consulta_pc2()
            return None

    # ------------------------------------------------------------------
    # Failover: verificación de la DB local con caché de 15 s
    # ------------------------------------------------------------------

    def _db_local_ok(self) -> bool:
        """
        Verifica si la DB principal de PC3 está operativa.

        El resultado se cachea durante DB_RECHEQUEO_INTERVALO segundos para
        evitar abrir la conexión SQLite en cada ciclo del monitor.  Cuando el
        estado cambia (disponible ↔ no disponible) se imprime un aviso claro.

        Implementación reactiva: la función es pura respecto al tiempo —
        evalúa el estado, actualiza la caché y retorna sin efectos secundarios
        más allá del aviso en consola.
        """
        ahora = time.time()
        if ahora - self._ultimo_chequeo_db < self.DB_RECHEQUEO_INTERVALO:
            return self._db_ok   # respuesta cacheada, sin acceso a disco

        # ── Re-chequeo real ──────────────────────────────────────────────
        ruta     = self._ruta_db()
        nuevo_ok = False
        if ruta.exists():
            try:
                conn = sqlite3.connect(str(ruta), timeout=3)
                conn.execute("SELECT 1 FROM estados LIMIT 1")
                conn.close()
                nuevo_ok = True
            except Exception:
                nuevo_ok = False

        # ── Notificar cambio de fuente ───────────────────────────────────
        if nuevo_ok != self._db_ok:
            if nuevo_ok:
                print(
                    "\n[MONITOR] ✔ DB local disponible de nuevo — "
                    "retomando trafico_principal.db"
                )
            else:
                print(
                    "\n[MONITOR] ⚠ DB local no disponible — "
                    "redirigiendo consultas a réplica en PC2 (failover activo)"
                )

        self._db_ok             = nuevo_ok
        self._ultimo_chequeo_db = ahora
        return self._db_ok

    # ------------------------------------------------------------------
    # Consultas locales (fuente primaria)
    # ------------------------------------------------------------------

    def _ruta_db(self) -> Path:
        base = Path(__file__).parent.parent
        return base / self.config["db"]["ruta"]

    def _leer_estados_local(self) -> list[tuple]:
        """Retorna los estados actuales desde la DB principal de PC3."""
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

    def _leer_resumen_local(self) -> dict:
        """Retorna conteo de intersecciones por estado desde la DB local."""
        ruta = self._ruta_db()
        if not ruta.exists():
            return {}
        try:
            conn  = sqlite3.connect(str(ruta))
            rows  = conn.execute("""
                SELECT estado_trafico, COUNT(*) AS n
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
    # Consultas remotas (fuente secundaria / failover)
    # ------------------------------------------------------------------

    def _leer_estados_remoto(self) -> list[tuple]:
        """
        Consulta los estados desde la réplica de PC2 vía ZMQ.

        Retorna una lista de tuplas en el mismo formato que _leer_estados_local().
        Los campos 'motivo' y 'ultimo_sensor' serán None en modo failover
        (la réplica de PC2 no los almacena en la tabla estados).
        """
        resp = self._enviar_consulta_remota("estados")
        if not resp or resp.get("status") != "ok":
            return []
        return [tuple(f) for f in resp.get("filas", [])]

    def _leer_resumen_remoto(self) -> dict:
        """Consulta el resumen desde la réplica de PC2 vía ZMQ."""
        resp = self._enviar_consulta_remota("resumen")
        if not resp or resp.get("status") != "ok":
            return {}
        return resp.get("resumen", {})

    # ------------------------------------------------------------------
    # API pública de lectura — despacho reactivo local / remoto
    # ------------------------------------------------------------------

    def _leer_estados(self) -> list[tuple]:
        """
        Retorna los estados actuales con conmutación dinámica de fuente.

        Selecciona el lector apropiado mediante un despacho funcional
        (sin estructuras de control imperativas anidadas), de modo que la
        lógica de failover queda encapsulada en _db_local_ok().
        """
        lector: Callable[[], list[tuple]] = (
            self._leer_estados_local if self._db_local_ok()
            else self._leer_estados_remoto
        )
        return lector()

    def _leer_resumen(self) -> dict:
        """
        Retorna el resumen de estados con conmutación dinámica de fuente.
        """
        lector: Callable[[], dict] = (
            self._leer_resumen_local if self._db_local_ok()
            else self._leer_resumen_remoto
        )
        return lector()

    # ------------------------------------------------------------------
    # Visualización
    # ------------------------------------------------------------------

    def _imprimir_estados(self) -> None:
        rows = self._leer_estados()
        if not rows:
            print("[MONITOR] Base de datos vacía aún.")
            return

        fuente = "réplica PC2  ⟵ FAILOVER ACTIVO" if not self._db_ok else "DB local"
        print(f"\n  [Fuente de datos: {fuente}]")

        # Agrupar por intersección base (ej. INT_A1)
        agrupados: dict = {}
        for r in rows:
            posicion, estado, motivo, sensor, ts = r
            if posicion.endswith("_NS") or posicion.endswith("_EO"):
                base      = posicion[:-3]
                direccion = posicion[-2:]
            else:
                base      = posicion
                direccion = "UNICA"

            if base not in agrupados:
                agrupados[base] = {
                    "NS": {"estado": "SIN DATOS", "sensor": "--"},
                    "EO": {"estado": "SIN DATOS", "sensor": "--"},
                }

            agrupados[base][direccion if direccion in ("NS", "EO") else "NS"] = {
                "estado": estado,
                "sensor": sensor or "N/A",
            }

        print()
        print(f"  {'CRUCE':<10} | {'EJE NORTE-SUR (CARRERA)':<35} | {'EJE ESTE-OESTE (CALLE)':<35}")
        print("  " + "-" * 88)
        for base, datos in sorted(agrupados.items()):
            ns_txt = (
                f"{datos.get('NS', {}).get('estado', 'SIN DATOS')} "
                f"({datos.get('NS', {}).get('sensor', '--')})"
            )
            eo_txt = (
                f"{datos.get('EO', {}).get('estado', 'SIN DATOS')} "
                f"({datos.get('EO', {}).get('sensor', '--')})"
            )
            print(f"  {base:<10} | NS: {ns_txt:<31} | EO: {eo_txt:<31}")
        print()

    def _imprimir_resumen(self) -> None:
        r = self._leer_resumen()
        if not r:
            return
        ts         = time.strftime("%H:%M:%S")
        normal     = r.get("NORMAL",     0)
        congestion = r.get("CONGESTION", 0)
        ola_verde  = r.get("OLA_VERDE",  0)
        total_ev   = r.get("_total_eventos", 0)
        fuente     = "PC2-réplica" if not self._db_ok else "local"
        print(
            f"\n[MONITOR] {ts} | Intersecciones: "
            f"NORMAL={normal} | CONGESTION={congestion} | OLA_VERDE={ola_verde} "
            f"| Eventos en BD={total_ev} | Fuente={fuente}"
        )

    # ------------------------------------------------------------------
    # Consola interactiva
    # ------------------------------------------------------------------

    def _procesar_comando_consola(self, linea: str) -> bool:
        """Procesa una línea de la consola. Retorna False si debe salir."""
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
            print("    ola <INT_XX_Dir>     -> Activar OLA_VERDE en una dirección (ej: ola INT_C3_NS)")
            print("    normal <INT_XX_Dir>  -> Forzar NORMAL en una dirección (ej: normal INT_C3_EO)")
            print("    estado               -> Ver tabla de estados actuales (agrupados por cruce)")
            print("    ayuda                -> Esta ayuda")
            print("    salir                -> Detener PC3")
            print()

        elif cmd in ("ola", "normal") and len(partes) >= 2:
            posicion = partes[1].upper()
            if not (posicion.endswith("_NS") or posicion.endswith("_EO")):
                print(
                    f"[MONITOR] Error: Debes incluir la dirección (_NS o _EO) "
                    f"en la posición (ej. {posicion}_NS)"
                )
                return True

            comando = "OLA_VERDE" if cmd == "ola" else "NORMAL"
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
        # Inicializar ambos canales ZMQ
        self._conectar_pc2()
        self._conectar_consulta_pc2()

        print()
        print(
            "[MONITOR] Monitor activo. "
            "Comandos: ola <INT_XX_Dir> | normal <INT_XX_Dir> | estado | ayuda | salir"
        )
        print(
            f"[MONITOR] Failover: re-chequeo DB local cada {self.DB_RECHEQUEO_INTERVALO} s | "
            f"timeout canal remoto {self.CONSULTA_TIMEOUT_MS // 1000} s"
        )
        print()

        ultimo_reporte = 0.0

        while not self.stop_event.is_set():
            ahora = time.time()
            if ahora - ultimo_reporte >= self.INTERVALO_MONITOR:
                self._imprimir_resumen()
                ultimo_reporte = ahora

            time.sleep(1)

        # Cierre limpio de los dos sockets
        for sock in (self._sock_consulta, self._sock_pc2):
            if sock:
                try:
                    sock.close()
                except Exception:
                    pass

        if self._ctx:
            self._ctx.term()

        print("[MONITOR] Monitor detenido.")
