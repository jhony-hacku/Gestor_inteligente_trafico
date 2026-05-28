"""
pc1/inyector_experimento.py
===========================
Genera exactamente el tráfico requerido por el experimento del profesor.
"""

import sys
import time
import json
import zmq
import random
from datetime import datetime, timezone

def generar_evento(sensor_id, tipo, posicion):
    ts = datetime.now(timezone.utc).astimezone().isoformat(timespec="microseconds")
    base = {
        "sensor_id": sensor_id,
        "tipo": tipo,
        "posicion": posicion,
        "timestamp": ts,
        "_fuente": "sensor"
    }
    if tipo == "camara":
        base["longitud_cola"] = random.randint(0, 10)
        base["velocidad_promedio"] = round(random.uniform(10, 60), 1)
    elif tipo == "espira":
        base["conteo_vehicular"] = random.randint(5, 30)
        base["intervalo_seg"] = 30
    elif tipo == "gps":
        base["velocidad_promedio"] = round(random.uniform(10, 60), 1)
        base["densidad"] = round(random.uniform(0.1, 0.5), 2)
    return base

def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ["1", "2"]:
        print("Uso: python inyector_experimento.py [1|2]")
        print("  1 = 1 sensor de cada tipo cada 10 seg")
        print("  2 = 2 sensores de cada tipo cada 5 seg")
        sys.exit(1)
        
    escenario = sys.argv[1]
    
    ctx = zmq.Context()
    sock = ctx.socket(zmq.XPUB)
    sock.bind("tcp://*:5560")
    
    # Configurar sensores según el escenario
    if escenario == "1":
        frecuencia = 10
        sensores = [
            ("CAM_EXP_1", "camara", "INT_EXP_NS"),
            ("ESP_EXP_1", "espira", "INT_EXP_NS"),
            ("GPS_EXP_1", "gps",    "INT_EXP_NS")
        ]
        print(f"[*] INICIANDO ESCENARIO 1: {len(sensores)} sensores totales | Frecuencia: {frecuencia}s")
    else:
        frecuencia = 5
        sensores = [
            ("CAM_EXP_1", "camara", "INT_EXP_NS"),
            ("ESP_EXP_1", "espira", "INT_EXP_NS"),
            ("GPS_EXP_1", "gps",    "INT_EXP_NS"),
            ("CAM_EXP_2", "camara", "INT_EXP_EO"),
            ("ESP_EXP_2", "espira", "INT_EXP_EO"),
            ("GPS_EXP_2", "gps",    "INT_EXP_EO")
        ]
        print(f"[*] INICIANDO ESCENARIO 2: {len(sensores)} sensores totales | Frecuencia: {frecuencia}s")

    print("[*] Esperando 5 segundos para que PC2 se conecte...")
    time.sleep(5)
    print("[*] ¡Inyectando datos! Mantén este script corriendo por 2 minutos exactos.")
    
    inicio = time.time()
    try:
        while True:
            for s_id, s_tipo, s_pos in sensores:
                ev = generar_evento(s_id, s_tipo, s_pos)
                payload = json.dumps(ev).encode("utf-8")
                topico = f"{s_tipo}.{s_pos}".encode("utf-8")
                sock.send_multipart([topico, payload])
                
            elapsed = time.time() - inicio
            print(f"  [{elapsed:5.1f}s] Enviados {len(sensores)} eventos...")
            time.sleep(frecuencia)
    except KeyboardInterrupt:
        print("\n[*] Inyección terminada.")
        sock.close()
        ctx.term()

if __name__ == "__main__":
    main()
