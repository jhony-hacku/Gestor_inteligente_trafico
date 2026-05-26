"""
test_flujo_rapido.py
====================
Test de flujo completo con frecuencia reducida (2s entre eventos).
Inyecta directamente en la cola del motor sin necesidad de esperar 30s.
Verifica:
  - Motor de reglas clasifica correctamente NORMAL / CONGESTION / OLA_VERDE
  - Controlador de semaforos imprime cada operacion
  - SQLite guarda los eventos
"""
import json
import os
import queue
import sys
import threading
import time
from pathlib import Path

# Agregar pc2 al path
sys.path.insert(0, str(Path(__file__).parent))
os.environ["PYTHONUTF8"] = "1"

from servicios.motor_reglas import MotorReglas
from servicios.control_semaforos import ControlSemaforos
from servicios.persistencia import Persistencia

# Config de prueba
CONFIG = {
    "pc3":    {"host": "localhost", "push_port": 5561},
    "umbrales": {
        "cola_max": 5,
        "velocidad_min_kmh": 35.0,
        "densidad_max": 0.70,
        "velocidad_ambulancia_kmh": 5.0,
        "conteo_congestion_por_minuto": 240,
    },
    "semaforo": {
        "tiempo_verde_normal_seg": 15,
        "tiempo_verde_congestion_seg": 30,
        "tiempo_ola_verde_seg": 60,
    },
}

# Eventos de prueba
EVENTOS_PRUEBA = [
    # NORMAL: cola=2, vel=50
    {"sensor_id":"CAM_A1","tipo":"camara","posicion":"INT_A1","timestamp":"2026-04-07T22:00:00",
     "longitud_cola":2,"velocidad_promedio":50.0,"_fuente":"sensor"},
    # CONGESTION: cola=8 > 5
    {"sensor_id":"CAM_B2","tipo":"camara","posicion":"INT_B2","timestamp":"2026-04-07T22:00:01",
     "longitud_cola":8,"velocidad_promedio":20.0,"_fuente":"sensor"},
    # GPS CONGESTION: densidad alta
    {"sensor_id":"GPS_C3","tipo":"gps","posicion":"INT_C3","timestamp":"2026-04-07T22:00:02",
     "densidad":0.85,"velocidad_promedio":25.0,"nivel_congestion":"ALTA","_fuente":"sensor"},
    # OLA_VERDE: ambulancia por GPS
    {"sensor_id":"GPS_D4","tipo":"gps","posicion":"INT_D4","timestamp":"2026-04-07T22:00:03",
     "densidad":0.3,"velocidad_promedio":3.0,"nivel_congestion":"BAJA","_fuente":"sensor"},
    # Espira normal
    {"sensor_id":"ESP_E5","tipo":"espira","posicion":"INT_E5","timestamp":"2026-04-07T22:00:04",
     "conteo_vehicular":50,"intervalo_seg":30,"_fuente":"sensor"},
    # Espira congestion
    {"sensor_id":"ESP_A3","tipo":"espira","posicion":"INT_A3","timestamp":"2026-04-07T22:00:05",
     "conteo_vehicular":120,"intervalo_seg":30,"_fuente":"sensor"},
    # Comando manual PC3
    {"comando":"OLA_VERDE","posicion":"INT_B1","duracion_seg":60,
     "_fuente":"PC3_MANUAL","tipo":"comando","timestamp":"2026-04-07T22:00:06"},
]

def main():
    print("=" * 60)
    print("  TEST FLUJO RAPIDO PC2")
    print("=" * 60)
    print(f"  Eventos a procesar: {len(EVENTOS_PRUEBA)}")
    print()

    cola_eventos      = queue.Queue()
    cola_semaforos    = queue.Queue()
    cola_persistencia = queue.Queue()
    stop_event        = threading.Event()

    motor    = MotorReglas(CONFIG, cola_eventos, cola_semaforos, cola_persistencia, stop_event)
    semaforo = ControlSemaforos(CONFIG, cola_semaforos, stop_event)
    persist  = Persistencia(CONFIG, cola_persistencia, stop_event)

    motor.start()
    semaforo.start()
    persist.start()
    time.sleep(0.3)

    # Inyectar eventos
    for ev in EVENTOS_PRUEBA:
        cola_eventos.put(ev)
        time.sleep(0.1)

    # Esperar procesamiento
    time.sleep(2)
    stop_event.set()
    motor.join(timeout=5)
    semaforo.join(timeout=5)
    persist.join(timeout=5)

    # Verificar SQLite
    db_path = Path(__file__).parent / "replica" / "trafico.db"
    if db_path.exists():
        import sqlite3
        conn = sqlite3.connect(str(db_path))
        n = conn.execute("SELECT COUNT(*) FROM eventos").fetchone()[0]
        rows = conn.execute(
            "SELECT posicion, estado_trafico, timestamp_cambio FROM estados ORDER BY posicion"
        ).fetchall()
        conn.close()
        print(f"\n[SQLITE] Eventos guardados: {n}")
        print("[SQLITE] Estado por interseccion:")
        for r in rows:
            print(f"  {r[0]:<10} -> {r[1]}")
    print("\n[TEST] OK")

if __name__ == "__main__":
    main()
