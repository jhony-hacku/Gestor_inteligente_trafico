"""
servicios/motor_reglas.py
=========================
Hilo motor de reglas de trafico (nucleo de PC2).

Consume eventos de la cola_eventos, mantiene el ultimo estado
conocido de cada interseccion y aplica las siguientes reglas:

  Camara:
    - Vp < velocidad_ambulancia_kmh  ->  OLA_VERDE  (ambulancia detectada)
    - Q > cola_max OR Vp < vel_min   ->  CONGESTION
    - resto                          ->  NORMAL

  Espira Inductiva:
    - tasa > conteo_congestion/min   ->  CONGESTION
    - resto                          ->  NORMAL

  GPS:
    - Vp < velocidad_ambulancia_kmh  ->  OLA_VERDE  (ambulancia detectada)
    - D > densidad_max OR Vp < vel_min -> CONGESTION
    - resto                          ->  NORMAL

  Comando PC3 (fuente=PC3_MANUAL):
    - Inyecta directamente el estado indicado en el mensaje

Cada cambio de estado se notifica a:
  - cola_semaforos   (para actuacion en semaforos)
  - cola_persistencia (para registro en PC3 y replica local)
"""

import queue
import threading
from datetime import datetime, timezone


# Constantes de estado
NORMAL     = "NORMAL"
CONGESTION = "CONGESTION"
OLA_VERDE  = "OLA_VERDE"


class MotorReglas(threading.Thread):
    """
    Motor de reglas que clasifica el trafico por interseccion.

    Parametros
    ----------
    config : dict
        Configuracion de PC2 (umbrales).
    cola_eventos : queue.Queue
        Entrada de eventos crudos del suscriptor y comandos de PC3.
    cola_semaforos : queue.Queue
        Salida de comandos para el controlador de semaforos.
    cola_persistencia : queue.Queue
        Salida de eventos procesados para el servicio de persistencia.
    stop_event : threading.Event
        Evento de parada compartido.
    """

    def __init__(self, config: dict,
                 cola_eventos: queue.Queue,
                 cola_semaforos: queue.Queue,
                 cola_persistencia: queue.Queue,
                 stop_event: threading.Event) -> None:
        super().__init__(name="Motor-Reglas", daemon=True)
        self.config           = config
        self.cola_eventos     = cola_eventos
        self.cola_semaforos   = cola_semaforos
        self.cola_persistencia = cola_persistencia
        self.stop_event       = stop_event
        self.umbrales         = config["umbrales"]

        # Estado por interseccion: posicion -> {"estado": str, "timestamp": str}
        self._estado_intersecciones: dict[str, dict] = {}

        # Contadores para estadisticas
        self._total_eventos   = 0
        self._total_cambios   = 0

    @staticmethod
    def _ts() -> str:
        return datetime.now(timezone.utc).astimezone().isoformat(timespec="microseconds")

    # ------------------------------------------------------------------
    # Evaluacion por tipo de sensor
    # ------------------------------------------------------------------

    def _evaluar_camara(self, evento: dict) -> tuple[str, str]:
        cola = evento.get("longitud_cola", 0)
        vel  = evento.get("velocidad_promedio", 999.0)
        u    = self.umbrales

        if vel < u["velocidad_ambulancia_kmh"]:
            return OLA_VERDE, f"Posible ambulancia detectada por camara (Vp={vel} km/h)"
        if cola > u["cola_max"]:
            return CONGESTION, f"Cola larga: Q={cola} > {u['cola_max']} vehiculos"
        if vel < u["velocidad_min_kmh"]:
            return CONGESTION, f"Velocidad baja: Vp={vel} < {u['velocidad_min_kmh']} km/h"
        return NORMAL, f"Flujo normal: Q={cola}, Vp={vel} km/h"

    def _evaluar_espira(self, evento: dict) -> tuple[str, str]:
        conteo   = evento.get("conteo_vehicular", 0)
        intervalo = evento.get("intervalo_seg", 30)
        tasa_min = (conteo / intervalo) * 60  # vehiculos por minuto
        umbral   = self.umbrales["conteo_congestion_por_minuto"]

        if tasa_min >= umbral:
            return CONGESTION, f"Alta densidad: {tasa_min:.0f} veh/min > {umbral}"
        return NORMAL, f"Flujo normal: {tasa_min:.0f} veh/min"

    def _evaluar_gps(self, evento: dict) -> tuple[str, str]:
        densidad = evento.get("densidad", 0.0)
        vel      = evento.get("velocidad_promedio", 999.0)
        nivel    = evento.get("nivel_congestion", "BAJA")
        u        = self.umbrales

        if vel < u["velocidad_ambulancia_kmh"]:
            return OLA_VERDE, f"Posible ambulancia detectada por GPS (Vp={vel} km/h)"
        if densidad > u["densidad_max"]:
            return CONGESTION, f"Densidad critica: D={densidad:.3f} > {u['densidad_max']} [{nivel}]"
        if vel < u["velocidad_min_kmh"]:
            return CONGESTION, f"Velocidad baja: Vp={vel} < {u['velocidad_min_kmh']} km/h [{nivel}]"
        return NORMAL, f"D={densidad:.3f}, Vp={vel} km/h [{nivel}]"

    # ------------------------------------------------------------------
    # Procesamiento de eventos
    # ------------------------------------------------------------------

    def _procesar_evento(self, evento: dict) -> None:
        self._total_eventos += 1
        fuente   = evento.get("_fuente", "sensor")
        posicion = evento.get("posicion", "DESCONOCIDA")
        ts       = self._ts()

        # Comando manual desde PC3
        if fuente == "PC3_MANUAL":
            nuevo_estado = evento.get("comando", OLA_VERDE)
            motivo       = f"Orden directa de PC3: {evento.get('comando', OLA_VERDE)}"
            self._actualizar_estado(posicion, nuevo_estado, motivo, evento, ts)
            return

        tipo = evento.get("tipo")
        if tipo == "camara":
            nuevo_estado, motivo = self._evaluar_camara(evento)
        elif tipo == "espira":
            nuevo_estado, motivo = self._evaluar_espira(evento)
        elif tipo == "gps":
            nuevo_estado, motivo = self._evaluar_gps(evento)
        else:
            return  # tipo desconocido, ignorar

        self._actualizar_estado(posicion, nuevo_estado, motivo, evento, ts)

    def _actualizar_estado(self, posicion: str, nuevo_estado: str,
                           motivo: str, evento: dict, ts: str) -> None:
        estado_previo = self._estado_intersecciones.get(posicion, {}).get("estado")

        self._estado_intersecciones[posicion] = {
            "estado":    nuevo_estado,
            "timestamp": ts,
            "motivo":    motivo,
        }

        # Notificar al controlador de semaforos SIEMPRE (para trazabilidad)
        cmd_semaforo = {
            "posicion":  posicion,
            "estado":    nuevo_estado,
            "motivo":    motivo,
            "timestamp": ts,
            "sensor_id": evento.get("sensor_id", "N/A"),
            "tipo":      evento.get("tipo", "N/A"),
        }
        self.cola_semaforos.put_nowait(cmd_semaforo)

        # Persistir evento enriquecido
        evento_procesado = {
            **evento,
            "estado_trafico":    nuevo_estado,
            "motivo":            motivo,
            "timestamp_proceso": ts,
        }
        self.cola_persistencia.put_nowait(evento_procesado)

        # Imprimir cambios de estado en consola
        if nuevo_estado != estado_previo:
            self._total_cambios += 1
            estado_ant = estado_previo or "INICIO"
            print(f"\n[ANALITICA] {ts[:19]} | {posicion}")
            print(f"  Estado : {estado_ant} --> {nuevo_estado}")
            print(f"  Motivo : {motivo}")
            print(f"  Sensor : {evento.get('sensor_id', 'N/A')} ({evento.get('tipo', 'N/A')})")

    # ------------------------------------------------------------------
    # Ciclo principal del hilo
    # ------------------------------------------------------------------

    def run(self) -> None:
        print("[MOTOR] Motor de reglas activo. Esperando eventos de sensores...")
        while not self.stop_event.is_set():
            try:
                evento = self.cola_eventos.get(timeout=1.0)
                self._procesar_evento(evento)
            except queue.Empty:
                continue
            except Exception as e:
                if not self.stop_event.is_set():
                    print(f"[MOTOR] Error procesando evento: {e}")

        print(f"[MOTOR] Detenido. Eventos procesados: {self._total_eventos}, "
              f"cambios de estado: {self._total_cambios}.")
