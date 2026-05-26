# Gestor Inteligente de Tráfico Urbano

Sistema distribuido de gestión de tráfico urbano sobre una cuadrícula **5×5** de intersecciones, implementado en tres nodos (PC1, PC2, PC3) comunicados mediante **ZeroMQ**.

---

## Arquitectura del Sistema

```
┌─────────────────────────────────────────────────────────────────────┐
│  PC1 · 10.43.99.192          PC2 · 10.43.100.91       PC3 · 10.43.99.78  │
│                                                                     │
│  75 Sensores (hilos)         Motor de Reglas           Base de Datos│
│  ┌──────────────────┐        ┌─────────────────┐      ┌───────────┐│
│  │ CAM / ESP / GPS  │ ──SUB──│  Analítica      │─PUSH─│  SQLite   ││
│  │ por intersección │        │  + Semáforos    │      │ Principal ││
│  └──────────────────┘        │  + Replica DB   │      └───────────┘│
│         │                    └─────────────────┘           │        │
│  ┌──────────────┐                   │ REQ/REP              │        │
│  │  Broker ZMQ  │             ◄─────┘  comandos ──────────┘        │
│  │  XSUB/XPUB   │                                                   │
│  └──────────────┘                                                   │
└─────────────────────────────────────────────────────────────────────┘
```

### Comunicación entre nodos

| Conexión | Patrón ZMQ | Puerto | Descripción |
|----------|-----------|--------|-------------|
| Sensores → Broker | PUB → XSUB | 5559 | Publicación interna en PC1 |
| PC1 → PC2 | XPUB ← SUB | 5560 | Eventos de sensores hacia analítica |
| PC2 → PC3 | PUSH → PULL | 5561 | Datos procesados hacia BD principal |
| PC3 → PC2 | REQ → REP | 5563 | Comandos manuales (OLA_VERDE, etc.) |

---

## Nodos

### PC1 — Simulador de Tráfico (`10.43.99.192`)

Simula una ciudad de 5×5 intersecciones con **75 sensores** concurrentes (1 cámara + 1 espira inductiva + 1 GPS por intersección), cada uno generando eventos cada 30 segundos.

**Sensores y métricas:**

| Tipo | ID | Métricas |
|------|----|---------|
| Cámara | `CAM_A1` … `CAM_E5` | `longitud_cola`, `velocidad_promedio` |
| Espira | `ESP_A1` … `ESP_E5` | `conteo_vehicular`, `intervalo_seg` |
| GPS    | `GPS_A1` … `GPS_E5` | `densidad`, `velocidad_promedio`, `nivel_congestion` |

**Estructura:**
```
pc1/
├── gestor.py               # Punto de entrada
├── requirements.txt
├── config/
│   └── ciudad.json         # Tamaño grilla, frecuencias, rangos
└── sensores/
    ├── base.py             # Clase base (threading.Thread + ZMQ PUB)
    ├── camara.py
    ├── espira.py
    └── gps.py
```

---

### PC2 — Servicio de Analítica (`10.43.100.91`)

Consume los eventos del broker de PC1, aplica reglas de tráfico en tiempo real, controla semáforos, persiste datos en PC3 y mantiene una réplica local SQLite.

**Estados de tráfico:**

| Estado | Condición | Semáforo |
|--------|-----------|---------|
| `NORMAL` | Q ≤ 5 veh AND Vp ≥ 35 km/h AND D ≤ 0.70 | Verde 15s / Rojo 15s |
| `CONGESTION` | Q > 5 OR Vp < 35 km/h OR D > 0.70 | Verde 30s extendido |
| `OLA_VERDE` | Ambulancia (Vp < 5 km/h) o comando PC3 | Verde 60s continuo |

**Estructura:**
```
pc2/
├── analitica.py            # Punto de entrada
├── requirements.txt
├── config/
│   └── pc2_config.json     # IPs, umbrales, tiempos de semáforo
├── servicios/
│   ├── suscriptor.py       # Hilo SUB ← PC1 XPUB :5560
│   ├── motor_reglas.py     # Hilo motor de reglas por intersección
│   ├── control_semaforos.py# Hilo actuador de semáforos
│   ├── persistencia.py     # Hilo PUSH → PC3 :5561 + SQLite local
│   └── servidor_control.py # Hilo REP :5563 ← comandos de PC3
└── replica/
    └── trafico.db          # Réplica SQLite (creada en runtime)
```

---

### PC3 — Servidor de Base de Datos (`10.43.99.78`)

Recibe todos los eventos procesados de PC2 vía PULL, los almacena en la base de datos SQLite principal y ofrece una consola de monitoreo para enviar comandos a PC2.

**Tablas SQLite:**

| Tabla | Contenido |
|-------|-----------|
| `eventos` | Histórico completo de todos los eventos con estado_trafico |
| `estados` | Último estado conocido por intersección (upsert) |
| `semaforos` | Registro de cada cambio de semáforo |

**Estructura:**
```
pc3/
├── servidor_db.py          # Punto de entrada con consola interactiva
├── requirements.txt
├── config/
│   └── pc3_config.json     # Puertos e IP de PC2
├── servicios/
│   ├── receptor.py         # Hilo PULL :5561 ← eventos de PC2
│   ├── almacenador.py      # Hilo escritura SQLite (batch de 50 eventos)
│   └── monitor_comandos.py # Monitoreo + REQ hacia PC2
└── db/
    └── trafico_principal.db# BD principal (creada en runtime)
```

---

## Configuración de IPs

Modifica **un solo archivo por nodo** antes de ejecutar:

### PC1 — `pc1/config/ciudad.json`
> PC1 no necesita IPs de otros nodos (solo hace `bind`).  
> Cambia los puertos si hay conflicto de red.

```json
{
  "broker": {
    "xsub_port": 5559,
    "xpub_port": 5560
  }
}
```

### PC2 — `pc2/config/pc2_config.json`
```json
{
  "pc1": { "xpub_host": "10.43.99.192", "xpub_port": 5560 },
  "pc3": { "host": "10.43.99.78",       "push_port": 5561 },
  "servidor_comandos": { "rep_port": 5563 }
}
```

### PC3 — `pc3/config/pc3_config.json`
```json
{
  "receptor": { "pull_port": 5561 },
  "pc2":      { "host": "10.43.100.91", "rep_port": 5563 }
}
```

---

## Instalación y Ejecución

### Requisitos
- Python 3.10+
- `pyzmq >= 25.0` (único paquete externo)
- `sqlite3` — incluido en Python (sin instalación)

### PC1 (en `10.43.99.192`)

```bash
cd Gestor_inteligente_trafico/pc1
pip3 install -r requirements.txt
python3 gestor.py
```

### PC2 (en `10.43.100.91`)

```bash
cd Gestor_inteligente_trafico/pc2
pip3 install -r requirements.txt
python3 analitica.py
```

### PC3 (en `10.43.99.78`)

```bash
cd Gestor_inteligente_trafico/pc3
pip3 install -r requirements.txt
python3 servidor_db.py
```

> **Orden de arranque recomendado:** PC3 → PC1 → PC2

---

## Firewall (Linux)

```bash
# PC1: abrir puertos del broker
sudo ufw allow 5559/tcp
sudo ufw allow 5560/tcp

# PC2: abrir puerto de comandos
sudo ufw allow 5563/tcp

# PC3: abrir puerto de recepción
sudo ufw allow 5561/tcp
```

---

## Consola Interactiva (PC3)

Al ejecutar `servidor_db.py`, queda disponible una consola:

```
PC3> estado              # Ver tabla de estados de todas las intersecciones
PC3> ola INT_C3          # Activar OLA_VERDE en INT_C3 (via PC2)
PC3> normal INT_B2       # Forzar NORMAL en INT_B2 (via PC2)
PC3> ayuda               # Ver todos los comandos
PC3> salir               # Detener PC3
```

---

## Pruebas Locales (todo en una sola máquina)

Cambia `pc2_config.json`:
```json
"pc1": { "xpub_host": "localhost" },
"pc3": { "host": "localhost" }
```
Y `pc3_config.json`:
```json
"pc2": { "host": "localhost" }
```

Luego en terminales separadas:
```bash
# Terminal 1
python3 pc1/gestor.py

# Terminal 2
python3 pc2/analitica.py

# Terminal 3
python3 pc3/servidor_db.py
```

### Scripts de verificación

```bash
# Verificar PC1 (captura 9 mensajes del broker)
python3 pc1/test_subscriber.py

# Verificar PC2 (inyección directa en motor de reglas)
python3 pc2/test_flujo_rapido.py

# Verificar PC3 (PUSH simulado + comprobación SQLite)
python3 pc3/test_pc3_local.py
```

---

## Ejemplo de Salida

### PC1
```
[ON]  [CAM_A1    ] activo en INT_A1 - cada 30s
[ON]  [ESP_A1    ] activo en INT_A1 - cada 30s
[ON]  [GPS_A1    ] activo en INT_A1 - cada 30s
...
[BROKER] XSUB escuchando en tcp://*:5559
[BROKER] XPUB publicando en tcp://*:5560
```

### PC2
```
[ANALITICA] 22:41:35 | INT_B2
  Estado : NORMAL --> CONGESTION
  Motivo : Cola larga: Q=8 > 5 vehiculos
  Sensor : CAM_B2 (camara)
[SEMAFORO] 22:41:35 | INT_B2   | VERDE=30s (extendido) / ROJO=5s | Estado: CONGESTION
```

### PC3
```
[ALMACENADOR] +50 eventos | Total: 150
[MONITOR] 22:45:00 | NORMAL=18 | CONGESTION=6 | OLA_VERDE=1 | Eventos en BD=450
```

---

## Resiliencia ante Fallos

| Fallo | Comportamiento |
|-------|---------------|
| PC3 caído | PC2 continúa operando con réplica SQLite local |
| PC1 caído | PC2 mantiene últimos estados conocidos, no procesa nuevos eventos |
| PC2 caído | PC1 sigue generando datos (el broker los descarta si no hay suscriptor) |
| Cualquier sensor caído | El hilo se detiene, el resto sigue activo independientemente |