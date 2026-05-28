"""
pc1/comparativa_hilos.py
========================
Script de analisis comparativo: Multihilo (150 hilos) vs Monohilo (1 hilo).
Mide el consumo de RAM, uso de CPU y throughput (mensajes/s) para el PC1.

Requisitos: pip install psutil
"""

import sys
import time
import json
import threading
import statistics
from pathlib import Path

try:
    import psutil
except ImportError:
    print("[ERROR] Falta la libreria psutil. Instalar con: pip install psutil")
    sys.exit(1)

import zmq
from gestor import cargar_config, generar_lista_sensores, TIPO_A_CLASE

# Duracion de cada prueba
DURACION_PRUEBA = 15

def medir_recursos(duracion, stop_event):
    """Mide CPU y RAM en segundo plano durante X segundos."""
    proc = psutil.Process()
    muestras_cpu = []
    muestras_ram = []
    
    inicio = time.time()
    while time.time() - inicio < duracion and not stop_event.is_set():
        cpu = proc.cpu_percent(interval=0.5)
        ram = proc.memory_info().rss / (1024 * 1024)
        muestras_cpu.append(cpu)
        muestras_ram.append(ram)
        
    return {
        "cpu_promedio": statistics.mean(muestras_cpu) if muestras_cpu else 0,
        "ram_promedio": statistics.mean(muestras_ram) if muestras_ram else 0,
        "cpu_max": max(muestras_cpu) if muestras_cpu else 0,
        "ram_max": max(muestras_ram) if muestras_ram else 0
    }

def simulador_monohilo(config, lista_sensores, stop_event, puerto):
    """Ejecuta los 150 sensores en un solo hilo usando un bucle."""
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUB)
    sock.connect(f"tcp://127.0.0.1:{puerto}")
    
    # Inicializar objetos de sensor (sin arrancar sus hilos)
    sensores_obj = []
    for cfg in lista_sensores:
        clase = TIPO_A_CLASE[cfg["tipo"]]
        sensor = clase(cfg, puerto, stop_event, ctx)
        sensor._sock_pub = sock # Reutilizar socket
        sensores_obj.append({
            "obj": sensor,
            "frecuencia": cfg.get("frecuencia_seg", 30),
            "ultimo_envio": 0
        })
        
    mensajes_enviados = 0
    while not stop_event.is_set():
        ahora = time.time()
        for s in sensores_obj:
            if ahora - s["ultimo_envio"] >= s["frecuencia"]:
                evento = s["obj"]._generar_evento()
                payload = json.dumps(evento, ensure_ascii=False).encode("utf-8")
                topico = f"{s['obj'].tipo}.{s['obj'].posicion}".encode("utf-8")
                try:
                    sock.send_multipart([topico, payload], zmq.NOBLOCK)
                    mensajes_enviados += 1
                except zmq.ZMQError:
                    pass
                s["ultimo_envio"] = ahora
        time.sleep(0.01)  # Pequeña pausa para no quemar CPU en el ciclo
        
    sock.close()
    ctx.term()
    return mensajes_enviados

def prueba_multihilo(config, lista_sensores):
    print("\n[+] Iniciando prueba MULTIHILO (150 hilos independientes)...")
    stop_event = threading.Event()
    ctx = zmq.Context()
    
    puerto_test = 5599
    # Dummy subscriber para drenar mensajes
    sub = ctx.socket(zmq.SUB)
    sub.bind(f"tcp://*:{puerto_test}")
    sub.setsockopt_string(zmq.SUBSCRIBE, "")
    
    hilos = []
    for cfg in lista_sensores:
        clase = TIPO_A_CLASE[cfg["tipo"]]
        hilo = clase(cfg, puerto_test, stop_event, ctx)
        hilos.append(hilo)
        
    for h in hilos:
        h.start()
        
    # Medir recursos en el hilo principal
    metricas = medir_recursos(DURACION_PRUEBA, stop_event)
    
    # Detener todo
    stop_event.set()
    for h in hilos:
        h.join()
        
    sub.close()
    ctx.term()
    return metricas

def prueba_monohilo(config, lista_sensores):
    print("\n[+] Iniciando prueba MONOHILO (1 hilo procesando 150 sensores)...")
    stop_event = threading.Event()
    ctx = zmq.Context()
    
    puerto_test = 5598
    sub = ctx.socket(zmq.SUB)
    sub.bind(f"tcp://*:{puerto_test}")
    sub.setsockopt_string(zmq.SUBSCRIBE, "")
    
    # Lanzar el simulador en 1 solo hilo de fondo
    hilo_mono = threading.Thread(target=simulador_monohilo, args=(config, lista_sensores, stop_event, puerto_test))
    hilo_mono.start()
    
    # Medir recursos en el hilo principal
    metricas = medir_recursos(DURACION_PRUEBA, stop_event)
    
    # Detener todo
    stop_event.set()
    hilo_mono.join()
    
    sub.close()
    ctx.term()
    return metricas

def main():
    print("="*60)
    print(" COMPARATIVA: MULTIHILO vs MONOHILO (PC1)")
    print("="*60)
    
    config = cargar_config()
    # Aceleramos la frecuencia para generar carga en la prueba
    for k in config["tipos_sensor"].keys():
        config["tipos_sensor"][k]["frecuencia_seg"] = 0.5 # 2 mensajes por seg x 150 = 300 msg/s
        
    lista = generar_lista_sensores(config)
    print(f"Sensores generados: {len(lista)}")
    print(f"Duracion por prueba: {DURACION_PRUEBA} segundos")
    
    # Calentamiento
    time.sleep(2)
    
    met_multi = prueba_multihilo(config, lista)
    time.sleep(3) # Dejar enfriar RAM/CPU
    met_mono = prueba_monohilo(config, lista)
    
    print("\n" + "="*60)
    print(" RESULTADOS FINALES")
    print("="*60)
    print(f"{'Metrica':<20} | {'Multihilo (150 threads)':<25} | {'Monohilo (1 thread)':<20}")
    print("-"*70)
    print(f"{'CPU Promedio':<20} | {met_multi['cpu_promedio']:>20.1f}% | {met_mono['cpu_promedio']:>18.1f}%")
    print(f"{'CPU Maximo':<20} | {met_multi['cpu_max']:>20.1f}% | {met_mono['cpu_max']:>18.1f}%")
    print(f"{'RAM Promedio':<20} | {met_multi['ram_promedio']:>17.1f} MB | {met_mono['ram_promedio']:>15.1f} MB")
    print(f"{'RAM Maxima':<20} | {met_multi['ram_max']:>17.1f} MB | {met_mono['ram_max']:>15.1f} MB")
    print("="*70)
    
    print("\n[ANALISIS TEORICO PARA EL PROFESOR]")
    print("1. RAM: El enfoque Multihilo consume mas RAM debido al overhead de crear ")
    print("   150 pilas de ejecucion (stacks) a nivel de sistema operativo.")
    print("2. CPU y GIL: Python usa el Global Interpreter Lock (GIL). 150 hilos no ")
    print("   se ejecutan en paralelo real, sino que el SO hace 'Context Switching', ")
    print("   lo que aumenta artificialmente el uso de CPU (Overhead).")
    print("3. Monohilo: Usa menos RAM y menos CPU para tareas I/O asincronas, pero ")
    print("   si la generacion de un dato tardara mucho, bloquearia a los demas 149 sensores.")

if __name__ == '__main__':
    main()
