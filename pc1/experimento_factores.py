"""
pc1/experimento_factores.py
===========================
Ejecuta el PC1 forzando los factores del diseño de experimentos (DoE).
"""
import sys
import threading
import time
import zmq

from sensores.camara import SensorCamara
from sensores.espira import SensorEspira
from sensores.gps import SensorGPS
from gestor import BrokerMultihilo, cargar_config

def correr_pc1_experimento(nivel: int):
    config = cargar_config()
    ctx = zmq.Context()
    stop_event = threading.Event()
    
    # ── Configurar Factores según el Nivel ──
    sensores_cfg = []
    
    if nivel == 1:
        print("[EXPERIMENTO] NIVEL 1: 1 sensor de cada tipo cada 10 seg")
        # 1 CAM, 1 ESP, 1 GPS = 3 totales. Frecuencia: 10s.
        frecuencia = 10
        sensores_cfg = [
            {"sensor_id": "CAM_EXP1_NS", "tipo": "camara", "posicion": "INT_EXP1_NS", "frecuencia_seg": frecuencia, "rango_cola": [0,5], "rango_velocidad": [30,60]},
            {"sensor_id": "ESP_EXP1_NS", "tipo": "espira", "posicion": "INT_EXP1_NS", "frecuencia_seg": frecuencia, "conteo_max": 50},
            {"sensor_id": "GPS_EXP1_NS", "tipo": "gps", "posicion": "INT_EXP1_NS", "frecuencia_seg": frecuencia, "densidad_max": 0.5},
        ]
    elif nivel == 2:
        print("[EXPERIMENTO] NIVEL 2: 2 sensores de cada tipo cada 5 seg")
        # 2 CAM, 2 ESP, 2 GPS = 6 totales. Frecuencia: 5s.
        frecuencia = 5
        sensores_cfg = [
            {"sensor_id": "CAM_EXP1_NS", "tipo": "camara", "posicion": "INT_EXP1_NS", "frecuencia_seg": frecuencia, "rango_cola": [0,5], "rango_velocidad": [30,60]},
            {"sensor_id": "ESP_EXP1_NS", "tipo": "espira", "posicion": "INT_EXP1_NS", "frecuencia_seg": frecuencia, "conteo_max": 50},
            {"sensor_id": "GPS_EXP1_NS", "tipo": "gps", "posicion": "INT_EXP1_NS", "frecuencia_seg": frecuencia, "densidad_max": 0.5},
            {"sensor_id": "CAM_EXP1_EO", "tipo": "camara", "posicion": "INT_EXP1_EO", "frecuencia_seg": frecuencia, "rango_cola": [0,5], "rango_velocidad": [30,60]},
            {"sensor_id": "ESP_EXP1_EO", "tipo": "espira", "posicion": "INT_EXP1_EO", "frecuencia_seg": frecuencia, "conteo_max": 50},
            {"sensor_id": "GPS_EXP1_EO", "tipo": "gps", "posicion": "INT_EXP1_EO", "frecuencia_seg": frecuencia, "densidad_max": 0.5},
        ]
    else:
        print("Nivel inválido. Usa 1 o 2.")
        sys.exit(1)

    print("[BROKER] Iniciando...")
    broker = BrokerMultihilo(config, ctx, stop_event).arrancar()
    time.sleep(1)
    
    hilos = []
    TIPO_MAP = {"camara": SensorCamara, "espira": SensorEspira, "gps": SensorGPS}
    
    for cfg in sensores_cfg:
        clase = TIPO_MAP[cfg["tipo"]]
        h = clase(cfg, config["broker"]["xsub_port"], stop_event, ctx)
        h.start()
        hilos.append(h)
        
    print(f"\n[GESTOR] {len(hilos)} sensores corriendo. En 2 minutos se detendrá solo...")
    
    # Temporizador de 120 segundos exactos (2 minutos) para el experimento
    inicio = time.time()
    while time.time() - inicio < 120:
        time.sleep(1)
        
    print("\n[GESTOR] 2 Minutos completados. Apagando sensores...")
    stop_event.set()
    for h in hilos:
        h.join()
        
    broker.detener(1)
    ctx.term()
    print("[EXPERIMENTO PC1] Finalizado.")

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Uso: python experimento_factores.py <1 o 2>")
        sys.exit(1)
    correr_pc1_experimento(int(sys.argv[1]))
