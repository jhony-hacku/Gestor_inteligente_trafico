"""
pc3/experimento_medicion.py
===========================
Mide las 2 variables dependientes del experimento:
1. Eventos almacenados en la DB tras 2 minutos.
2. Latencia (tiempo) desde que se envía un comando hasta que el semáforo acata el cambio.
"""
import sys
import time
import sqlite3
import json
from pathlib import Path

import zmq
from cripto import aplicar_curve_cliente

def cargar_config_pc3():
    ruta = Path(__file__).parent / "config" / "pc3_config.json"
    with open(ruta, "r") as f:
        return json.load(f)

def medir_latencia_comando():
    """Mide milisegundos desde que sale el comando hasta que PC2 lo procesa y responde."""
    print("\n[MEDICIÓN 2] Evaluando latencia de comando de semáforo...")
    config = cargar_config_pc3()
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, 5000)
    
    # IMPORTANTE: Aplicar la misma clave CurveZMQ de PC3
    keys_dir = Path(__file__).parent / "keys"
    try:
        aplicar_curve_cliente(sock, keys_dir)
    except Exception as e:
        print(f"Error de claves: {e}. Asegurate de estar ejecutando esto en PC3.")
        return

    host = config["pc2"]["host"]
    port = config["pc2"]["rep_port"]
    sock.connect(f"tcp://{host}:{port}")
    
    # Payload del comando
    comando = {"comando": "OLA_VERDE", "posicion": "INT_EXP1_NS", "duracion_seg": 60}
    
    inicio = time.time()
    try:
        sock.send_json(comando)
        # recv_json() se desbloquea en el instante exacto en que PC2 envía la orden 
        # a la cola de semáforos y acusa recibo.
        respuesta = sock.recv_json()
        fin = time.time()
        
        latencia_ms = (fin - inicio) * 1000
        print(f"  Resultado de PC2: {respuesta.get('mensaje', 'OK')}")
        print(f"  --> Latencia de actuación del semáforo: {latencia_ms:.2f} milisegundos")
    except zmq.Again:
        print("  --> ERROR: Timeout al enviar el comando. (PC2 no respondió)")
    finally:
        sock.close()
        ctx.term()

def medir_almacenamiento():
    """Cuenta cuántas solicitudes hay en la base de datos local de PC3."""
    config = cargar_config_pc3()
    ruta_db = Path(__file__).parent / config["db"]["ruta"]
    
    print("\n[MEDICIÓN 1] Evaluando solicitudes almacenadas en la BD...")
    if not ruta_db.exists():
        print(f"  --> La base de datos {ruta_db.name} está vacía (0 eventos).")
        return
        
    conn = sqlite3.connect(str(ruta_db))
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM eventos")
    total = cur.fetchone()[0]
    conn.close()
    
    print(f"  --> Eventos totales en la BD de PC3: {total}")
    print(f"  (Teóricamente para el Nivel 1 deberían ser ~36, Nivel 2 ~144)")

if __name__ == '__main__':
    print("="*60)
    print(" MEDICIÓN DE VARIABLES DEPENDIENTES (PC3)")
    print("="*60)
    
    if len(sys.argv) > 1 and sys.argv[1] == "latencia":
        medir_latencia_comando()
    elif len(sys.argv) > 1 and sys.argv[1] == "db":
        medir_almacenamiento()
    else:
        print("Para ejecutar, usa uno de estos argumentos:")
        print("  python experimento_medicion.py db       (Mide la variable 1)")
        print("  python experimento_medicion.py latencia (Mide la variable 2)")
