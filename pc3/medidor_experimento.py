"""
pc3/medidor_experimento.py
==========================
Mide las 2 variables dependientes del experimento en PC3:
1. Cantidad de eventos en la BD tras 2 minutos.
2. Latencia (tiempo desde orden de usuario hasta cambio en semáforo).
"""

import sys
import time
import sqlite3
import zmq
from pathlib import Path

# Cargar cripto para comandos
try:
    from servicios.cripto import aplicar_curve_cliente
except ImportError:
    print("Error: Ejecuta este script desde la carpeta pc3/")
    sys.exit(1)

def medir_latencia(host, puerto_comandos, keys_dir):
    """Envía 5 comandos espaciados para sacar el promedio de latencia."""
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    
    # Timeout para evitar bloqueos
    sock.setsockopt(zmq.RCVTIMEO, 2000)
    sock.setsockopt(zmq.SNDTIMEO, 2000)
    
    # Aplicar criptografía obligatoria
    aplicar_curve_cliente(sock, keys_dir)
    sock.connect(f"tcp://{host}:{puerto_comandos}")
    
    latencias = []
    print("\n[Midiendo Latencia de Cambio de Semáforo]")
    for i in range(5):
        # El usuario hace clic en el botón a este instante exacto:
        t_inicio = time.perf_counter()
        
        sock.send_json({
            "comando": "OLA_VERDE", 
            "posicion": f"INT_EXP_NS", 
            "duracion_seg": 60
        })
        
        try:
            sock.recv_json()
            # El servidor respondió que procesó y encoló el comando
            t_fin = time.perf_counter()
            lat_ms = (t_fin - t_inicio) * 1000
            latencias.append(lat_ms)
            print(f"  Prueba {i+1}/5: {lat_ms:.2f} ms")
        except zmq.Again:
            print(f"  Prueba {i+1}/5: Timeout (El servidor no respondió)")
            
        time.sleep(1) # Pausa entre clics
        
    sock.close()
    ctx.term()
    
    if latencias:
        promedio = sum(latencias) / len(latencias)
        print(f"► LATENCIA PROMEDIO DE RESPUESTA: {promedio:.2f} milisegundos")
    else:
        print("► NO SE PUDO MEDIR LA LATENCIA (Todos fallaron)")

def medir_bd(db_path):
    """Cuenta las filas exactas en la tabla eventos."""
    print("\n[Midiendo Cantidad de Solicitudes en BD]")
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM eventos;")
        total = cursor.fetchone()[0]
        conn.close()
        print(f"► TOTAL DE SOLICITUDES ALMACENADAS: {total} eventos")
    except Exception as e:
        print(f"► Error leyendo BD: {e}")

def main():
    print("="*60)
    print(" MEDIDOR DE VARIABLES - EXPERIMENTO DEL PROFESOR (PC3)")
    print("="*60)
    print("Asegúrate de haber esperado EXACTAMENTE 2 MINUTOS desde que")
    print("iniciaste el inyector en PC1 antes de correr esto.\n")
    
    base_dir = Path(__file__).parent
    db_path = base_dir / "db" / "trafico_principal.db"
    keys_dir = base_dir / "keys"
    
    # Para redes físicas, cambiar localhost por la IP de PC2 si es necesario
    host_pc2 = "10.43.100.91" 
    
    # Si la IP del PC2 en esta red es distinta, avisa al usuario
    import json
    try:
        with open(base_dir / "config" / "pc3_config.json") as f:
            cfg = json.load(f)
            host_pc2 = cfg["pc2"]["host"]
    except Exception:
        pass
        
    print(f"Usando IP de PC2: {host_pc2}")
    
    medir_bd(db_path)
    medir_latencia(host_pc2, 5563, keys_dir)
    print("="*60)

if __name__ == "__main__":
    main()
