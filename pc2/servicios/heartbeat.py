"""
servicios/heartbeat.py
======================
Módulo de heartbeat activo del PC2.

Expone tres componentes:

1. EstadoNodos
   Objeto thread-safe que centraliza la disponibilidad conocida de PC1 y PC3.
   Otros hilos (Persistencia, Suscriptor) lo leen consultando únicamente
   un booleano en memoria — sin necesidad de abrir sockets adicionales.

2. _SondeoNodo  (clase interna)
   Encapsula el socket REQ hacia un nodo concreto y la lógica ping/pong.
   Recrea el socket REQ automáticamente tras cada timeout (el socket REQ
   de ZMQ queda en estado inválido tras un timeout y debe recrearse).

3. HeartbeatCliente
   Hilo que sondea PC1 (puerto 5565) y PC3 (puerto 5566) cada
   ``heartbeat.intervalo_seg`` segundos (10 s), usando
   ``threading.Event.wait(timeout=N)`` en lugar de ``time.sleep()``
   para permitir parada limpia e inmediata.
   Actualiza EstadoNodos e imprime eventos de SUBIDA / CAÍDA con timestamp.

Integración:
  - analitica.py crea un EstadoNodos y lo pasa a Persistencia, Suscriptor
    y HeartbeatCliente.
  - Persistencia consulta ``estado_nodos.pc3_disponible`` antes de cada PUSH.
  - Suscriptor consulta ``estado_nodos.pc1_disponible`` para advertencias.
"""

import threading
import time
from datetime import datetime, timezone

import zmq


# ---------------------------------------------------------------------------

class EstadoNodos:
    """
    Estado de disponibilidad de PC1 y PC3, consultable sin red.

    Thread-safe mediante ``threading.Lock``.  Solo ``HeartbeatCliente``
    escribe; ``Persistencia`` y ``Suscriptor`` únicamente leen las propiedades.
    """

    def __init__(self) -> None:
        self._lock      = threading.Lock()
        # Estado inicial optimista: asumimos nodos arrancados antes que PC2
        self._pc1_ok    = True
        self._pc3_ok    = True
        self._pc3_db_ok = True
        self._pc1_ts    = 0.0
        self._pc3_ts    = 0.0

    # ── Lectura (Persistencia y Suscriptor) ─────────────────────────────────

    @property
    def pc1_disponible(self) -> bool:
        """True si el último heartbeat de PC1 fue exitoso."""
        with self._lock:
            return self._pc1_ok

    @property
    def pc3_disponible(self) -> bool:
        """True si PC3 está vivo Y su base de datos responde."""
        with self._lock:
            return self._pc3_ok and self._pc3_db_ok

    # ── Escritura (solo HeartbeatCliente) ───────────────────────────────────

    def actualizar_pc1(self, disponible: bool) -> None:
        with self._lock:
            self._pc1_ok = disponible
            self._pc1_ts = time.time()

    def actualizar_pc3(self, disponible: bool, db_ok: bool) -> None:
        with self._lock:
            self._pc3_ok    = disponible
            self._pc3_db_ok = db_ok
            self._pc3_ts    = time.time()

    def resumen(self) -> str:
        with self._lock:
            return (
                f"PC1={'OK' if self._pc1_ok else 'CAÍDO'} | "
                f"PC3={'OK' if self._pc3_ok else 'CAÍDO'} "
                f"(DB={'OK' if self._pc3_db_ok else 'FALLO'})"
            )


# ---------------------------------------------------------------------------

class _SondeoNodo:
    """
    Encapsula la comunicación REQ ping/pong con un único nodo.

    Gestiona el ciclo de vida del socket ZMQ REQ: creación, envío de ping,
    detección de timeout y reconexión automática.
    """

    def __init__(
        self,
        nombre: str,
        ctx: zmq.Context,
        host: str,
        port: int,
        timeout_ms: int,
    ) -> None:
        self.nombre   = nombre
        self._ctx     = ctx
        self._host    = host
        self._port    = port
        self._timeout = timeout_ms
        self._sock: zmq.Socket | None = None
        self._reconectar()

    def _reconectar(self) -> None:
        """Cierra el socket actual (si existe) y crea uno nuevo."""
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
        sock = self._ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.RCVTIMEO, self._timeout)
        sock.setsockopt(zmq.SNDTIMEO, self._timeout)
        sock.setsockopt(zmq.LINGER,   0)
        sock.connect(f"tcp://{self._host}:{self._port}")
        self._sock = sock

    @staticmethod
    def _ts() -> str:
        return datetime.now(timezone.utc).astimezone().isoformat(timespec="microseconds")

    def ping(self) -> dict | None:
        """
        Envía PING y retorna el PONG como dict.

        Retorna None si hay timeout o error de red.
        El socket REQ se recrea automáticamente para que el próximo
        intento parta de un estado limpio.
        """
        try:
            self._sock.send_json({"tipo": "ping", "timestamp": self._ts()})
            return self._sock.recv_json()
        except zmq.Again:
            self._reconectar()
            return None
        except Exception:
            self._reconectar()
            return None

    def cerrar(self) -> None:
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------

class HeartbeatCliente(threading.Thread):
    """
    Hilo que sondea PC1 y PC3 cada ``intervalo_seg`` segundos.

    - Usa ``threading.Event.wait(timeout)`` para que Ctrl+C lo detenga
      de forma inmediata sin esperar al final del intervalo.
    - Solo imprime en consola cuando el estado de un nodo cambia
      (SUBIDA o CAÍDA), evitando spam en el log.

    Parámetros
    ----------
    config : dict
        Configuración de PC2.  Sección ``heartbeat`` requerida.
    estado_nodos : EstadoNodos
        Objeto compartido que se actualiza en cada ciclo.
    stop_event : threading.Event
        Evento de parada compartido con los demás servicios.
    """

    def __init__(
        self,
        config: dict,
        estado_nodos: EstadoNodos,
        stop_event: threading.Event,
    ) -> None:
        super().__init__(name="HeartbeatCliente", daemon=True)
        self.config       = config
        self.estado_nodos = estado_nodos
        self.stop_event   = stop_event

        hb = config["heartbeat"]
        self._intervalo  = hb["intervalo_seg"]   # 10 s
        self._timeout_ms = hb["timeout_ms"]       # 10 000 ms

        # Estado previo para detectar transiciones (solo se loguea en cambio)
        self._prev_pc1_ok = True
        self._prev_pc3_ok = True

    @staticmethod
    def _ts_corto() -> str:
        return datetime.now(timezone.utc).astimezone().strftime("%H:%M:%S")

    def _loguear_cambio(self, nodo: str, ahora_ok: bool, detalle: str = "") -> None:
        ts = self._ts_corto()
        if ahora_ok:
            print(f"[HEARTBEAT] {ts} ✔ {nodo} disponible de nuevo{detalle}")
        else:
            print(f"[HEARTBEAT] {ts} ⚠ {nodo} NO DISPONIBLE{detalle}")

    def run(self) -> None:
        hb  = self.config["heartbeat"]
        ctx = zmq.Context()

        sondeo_pc1 = _SondeoNodo(
            "PC1", ctx,
            hb["pc1_host"], hb["pc1_hb_port"],
            self._timeout_ms,
        )
        sondeo_pc3 = _SondeoNodo(
            "PC3", ctx,
            hb["pc3_host"], hb["pc3_hb_port"],
            self._timeout_ms,
        )

        print(
            f"[HEARTBEAT] Cliente activo — "
            f"PC1 tcp://{hb['pc1_host']}:{hb['pc1_hb_port']} | "
            f"PC3 tcp://{hb['pc3_host']}:{hb['pc3_hb_port']} | "
            f"intervalo={self._intervalo}s | timeout={self._timeout_ms // 1000}s"
        )

        while not self.stop_event.is_set():
            # Esperar el intervalo o hasta que stop_event sea activado
            self.stop_event.wait(timeout=self._intervalo)
            if self.stop_event.is_set():
                break

            # ── Sondeo PC1 ────────────────────────────────────────────────────
            pong_pc1 = sondeo_pc1.ping()
            pc1_ok   = (
                pong_pc1 is not None
                and pong_pc1.get("tipo") == "pong"
                and pong_pc1.get("broker_ok", False)
            )
            self.estado_nodos.actualizar_pc1(pc1_ok)

            if pc1_ok != self._prev_pc1_ok:
                detalle = ""
                if pc1_ok and pong_pc1:
                    detalle = (
                        f" — broker_ok=True | "
                        f"sensores={pong_pc1.get('sensores_activos')}"
                        f"/{pong_pc1.get('sensores_total')} | "
                        f"uptime={pong_pc1.get('uptime_s')}s"
                    )
                elif pong_pc1 is None:
                    detalle = " — sin respuesta (nodo caído o timeout)"
                else:
                    detalle = f" — broker_ok={pong_pc1.get('broker_ok')}"
                self._loguear_cambio("PC1", pc1_ok, detalle)
                self._prev_pc1_ok = pc1_ok

            # ── Sondeo PC3 ────────────────────────────────────────────────────
            pong_pc3 = sondeo_pc3.ping()
            pc3_vivo = pong_pc3 is not None and pong_pc3.get("tipo") == "pong"
            db_ok    = pong_pc3.get("db_ok", False) if pc3_vivo else False
            pc3_ok   = pc3_vivo and db_ok

            self.estado_nodos.actualizar_pc3(pc3_vivo, db_ok)

            if pc3_ok != self._prev_pc3_ok:
                detalle = ""
                if pc3_ok and pong_pc3:
                    detalle = (
                        f" — db_ok=True | "
                        f"eventos={pong_pc3.get('eventos')} | "
                        f"uptime={pong_pc3.get('uptime_s')}s"
                    )
                elif not pc3_vivo:
                    detalle = " — sin respuesta (nodo caído o timeout)"
                else:
                    detalle = " — PC3 vivo pero DB no responde (db_ok=False)"
                self._loguear_cambio("PC3", pc3_ok, detalle)
                self._prev_pc3_ok = pc3_ok

        # Cierre limpio
        sondeo_pc1.cerrar()
        sondeo_pc3.cerrar()
        ctx.term()
        print("[HEARTBEAT] Cliente detenido.")
