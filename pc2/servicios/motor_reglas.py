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
from collections import deque
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
    # Gestión de Memoria y Evaluador Avanzado
    # ------------------------------------------------------------------

    def _get_o_crear_memoria(self, posicion: str) -> dict:
        if posicion not in self._estado_intersecciones:
            self._estado_intersecciones[posicion] = {
                "estado": NORMAL,
                "timestamp": self._ts(),
                "motivo": "Inicialización",
                "sensores": {
                    "camara": {},
                    "espira": {},
                    "gps": {}
                },
                "historial_camara_cola": deque(maxlen=4)
            }
        return self._estado_intersecciones[posicion]

    def _evaluar_eje(self, posicion: str, evento_disparador: dict, ts: str) -> None:
        memoria = self._estado_intersecciones[posicion]
        sens = memoria["sensores"]
        u = self.umbrales
        cola_max = float(u.get("cola_max", 20)) # Capacidad máxima asumida por cuadra
        
        cam = sens["camara"]
        gps = sens["gps"]
        esp = sens["espira"]
        
        # Prioridad Cero: Ambulancias
        if cam.get("velocidad_promedio", 999) < u["velocidad_ambulancia_kmh"]:
            self._actualizar_estado(posicion, OLA_VERDE, "Ambulancia detectada (Cámara)", evento_disparador, ts)
            return
        if gps.get("velocidad_promedio", 999) < u["velocidad_ambulancia_kmh"]:
            self._actualizar_estado(posicion, OLA_VERDE, "Ambulancia detectada (GPS)", evento_disparador, ts)
            return

        # Variables para las reglas analíticas
        ocupacion_camara = cam.get("longitud_cola", 0) / cola_max if cola_max > 0 else 0
        gps_vel = gps.get("velocidad_promedio", 999.0)
        espira_tasa = esp.get("tasa_min", 0.0)
        
        # Regla 1: Correlación Alternativa por Umbrales Críticos
        if ocupacion_camara > 0.95:
            self._actualizar_estado(posicion, CONGESTION, f"Umbral crítico: Ocupación cámara > 95% ({(ocupacion_camara*100):.1f}%)", evento_disparador, ts)
            return
            
        # Regla 2: Tendencia Temporal e Histórico Local (Cámara)
        historial = memoria["historial_camara_cola"]
        if len(historial) == historial.maxlen:
            creciente = True
            for i in range(1, len(historial)):
                if historial[i] <= historial[i-1]:
                    creciente = False
                    break
            
            if creciente:
                primero = historial[0]
                ultimo = historial[-1]
                # Incremento continuo mayor al 25% de la capacidad máxima
                delta = (ultimo - primero) / cola_max if cola_max > 0 else 0
                if delta > 0.25:
                    self._actualizar_estado(posicion, CONGESTION, f"Tendencia predictiva: Incremento continuo del {(delta*100):.1f}%", evento_disparador, ts)
                    return

        # Regla 3: Regla Combinada Estándar (Cámara + GPS + Espira)
        # Ocupación de cámara > 75%, GPS vel < 15 km/h, Espira densidad > 20 veh/min
        if (ocupacion_camara > 0.75 and gps_vel < 15.0 and espira_tasa > 20.0):
            motivo = f"Regla combinada: Ocup={ocupacion_camara*100:.1f}%, GPS_V={gps_vel:.1f}, Esp_Tasa={espira_tasa:.1f}"
            self._actualizar_estado(posicion, CONGESTION, motivo, evento_disparador, ts)
            return
            
        # Default
        self._actualizar_estado(posicion, NORMAL, "Flujo normal según analítica avanzada", evento_disparador, ts)

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
            self._get_o_crear_memoria(posicion)
            self._actualizar_estado(posicion, nuevo_estado, motivo, evento, ts)
            return

        tipo = evento.get("tipo")
        if tipo not in ("camara", "espira", "gps"):
            return  # tipo desconocido, ignorar

        memoria_eje = self._get_o_crear_memoria(posicion)

        # Actualizar la memoria con el último dato del sensor correspondiente
        if tipo == "camara":
            memoria_eje["sensores"]["camara"] = {
                "longitud_cola": evento.get("longitud_cola", 0),
                "velocidad_promedio": evento.get("velocidad_promedio", 999.0)
            }
            memoria_eje["historial_camara_cola"].append(evento.get("longitud_cola", 0))
        elif tipo == "espira":
            conteo = evento.get("conteo_vehicular", 0)
            intervalo = evento.get("intervalo_seg", 30)
            tasa_min = (conteo / intervalo) * 60 if intervalo > 0 else 0
            memoria_eje["sensores"]["espira"] = {
                "tasa_min": tasa_min
            }
        elif tipo == "gps":
            memoria_eje["sensores"]["gps"] = {
                "velocidad_promedio": evento.get("velocidad_promedio", 999.0),
                "densidad": evento.get("densidad", 0.0)
            }

        # Ejecutar evaluador avanzado con la memoria consolidada
        self._evaluar_eje(posicion, evento, ts)

    def _actualizar_estado(self, posicion: str, nuevo_estado: str,
                           motivo: str, evento: dict, ts: str) -> None:
        memoria = self._get_o_crear_memoria(posicion)
        estado_previo = memoria.get("estado")

        memoria["estado"] = nuevo_estado
        memoria["timestamp"] = ts
        memoria["motivo"] = motivo

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

        # Lógica de seguridad vial cruzada (2 semaforos por interseccion)
        if nuevo_estado in (CONGESTION, OLA_VERDE):
            pos_opuesta = None
            if posicion.endswith("_NS"):
                pos_opuesta = posicion[:-3] + "_EO"
            elif posicion.endswith("_EO"):
                pos_opuesta = posicion[:-3] + "_NS"
            
            if pos_opuesta:
                cmd_opuesto = {
                    "posicion":  pos_opuesta,
                    "estado":    "ROJO_FORZADO",
                    "motivo":    f"Seguridad: eje transversal en {nuevo_estado}",
                    "timestamp": ts,
                    "sensor_id": "SISTEMA",
                    "tipo":      "seguridad_vial",
                }
                self.cola_semaforos.put_nowait(cmd_opuesto)
                memoria_opuesta = self._get_o_crear_memoria(pos_opuesta)
                memoria_opuesta["estado"] = "ROJO_FORZADO"
                memoria_opuesta["timestamp"] = ts
                memoria_opuesta["motivo"] = cmd_opuesto["motivo"]

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
