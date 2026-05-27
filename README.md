# Gestor Inteligente de Tráfico Urbano

Sistema distribuido de gestión de tráfico urbano sobre una cuadrícula **5×5** de intersecciones, implementado en tres nodos (PC1, PC2, PC3) comunicados mediante **ZeroMQ**.

---

## Arquitectura del Sistema

```
┌─────────────────────────────────────────────────────────────────────────┐
│  PC1 · 10.43.99.192           PC2 · 10.43.100.91        PC3 · 10.43.99.78  │
│                                                                         │
│  150 Sensores (hilos)         Motor de Reglas             Base de Datos │
│  ┌──────────────────┐         ┌─────────────────────┐   ┌────────────┐ │
│  │ CAM / ESP / GPS  │──SUB────│  Analítica Avanzada │─PUSH─│  SQLite  │ │
│  │ por eje (_NS/_EO)│         │  + Semáforos        │   │ Principal  │ │
│  └──────────────────┘         │  + Réplica DB local │   └────────────┘ │
│         │                     │  + Heartbeat        │         │        │
│  ┌──────────────┐             │  + Failover/Failback│    ┌────┴─────┐  │
│  │  Broker ZMQ  │       ◄─────│  ServidorControl    │    │   GUI    │  │
│  │  XSUB/XPUB   │             │  ServidorConsulta   │    │ Tkinter  │  │
│  │  Heartbeat   │             └─────────────────────┘    └──────────┘  │
│  │  Servidor    │                     │ REQ/REP (5563)                  │
│  └──────────────┘             ◄───────┘  comandos  ─────────────────── │
└─────────────────────────────────────────────────────────────────────────┘
```

### Comunicación entre nodos

| Conexión | Patrón ZMQ | Puerto | Descripción |
|----------|-----------|--------|-------------|
| Sensores → Broker | PUB → XSUB | 5559 | Publicación interna en PC1 |
| PC1 → PC2 | XPUB ← SUB | 5560 | Eventos de sensores hacia analítica |
| PC2 → PC3 | PUSH → PULL | 5561 | Datos procesados hacia BD principal |
| PC3 → PC2 | REQ → REP | 5563 | Comandos manuales (OLA_VERDE, etc.) |
| PC2 → PC3 | REQ → REP | 5564 | Consulta de réplica (failover de lectura) |
| PC2 ↔ PC1 | REQ ↔ REP | 5565 | Heartbeat activo (sondeo PC1) |
| PC2 ↔ PC3 | REQ ↔ REP | 5566 | Heartbeat activo (sondeo PC3) |

---

## Nodos

### PC1 — Simulador de Tráfico (`10.43.99.192`)

Simula una ciudad de 5×5 intersecciones con **150 sensores** concurrentes (1 cámara + 1 espira + 1 GPS por eje direccional — **NS** y **EO** — de cada intersección), cada uno generando eventos cada 30 segundos.

**Modelo de Ejes Dobles:**

Cada intersección física (ej. `INT_A1`) tiene dos ejes independientes:
- `INT_A1_NS` — Eje Norte-Sur (carrera)
- `INT_A1_EO` — Eje Este-Oeste (calle)

Cada eje tiene sus propios 3 sensores, dando **6 sensores por intersección** y **150 en total**.

**Sensores y métricas:**

| Tipo | ID de ejemplo | Métricas |
|------|--------------|---------|
| Cámara | `CAM_A1_NS` | `longitud_cola`, `velocidad_promedio` |
| Espira | `ESP_A1_NS` | `conteo_vehicular`, `intervalo_seg` |
| GPS    | `GPS_A1_NS` | `densidad`, `velocidad_promedio`, `nivel_congestion` |

**Estructura:**
```
pc1/
├── gestor.py                  # Punto de entrada + Broker ZMQ + Heartbeat
├── heartbeat_servidor.py      # Servidor de sondeo para PC2 (puerto 5565)
├── requirements.txt
├── config/
│   └── ciudad.json            # Tamaño grilla, frecuencias, rangos
└── sensores/
    ├── base.py                # Clase base (threading.Thread + ZMQ PUB)
    ├── camara.py
    ├── espira.py
    └── gps.py
```

---

### PC2 — Servicio de Analítica (`10.43.100.91`)

Consume los eventos del broker de PC1, aplica reglas de analítica avanzada en tiempo real, controla semáforos, persiste datos en PC3, mantiene una réplica local SQLite con sincronización automática, y supervisa continuamente el estado de los nodos vecinos.

**Estados de tráfico:**

| Estado | Condición | Semáforo |
|--------|-----------|---------| 
| `NORMAL` | Flujo regular según analítica combinada | Verde 15s / Rojo 15s |
| `CONGESTION` | Detectado por cualquiera de las 3 reglas avanzadas | Verde 30s extendido |
| `OLA_VERDE` | Ambulancia (Vp < 5 km/h) o comando PC3 | Verde 60s continuo |
| `ROJO_FORZADO` | Eje opuesto en CONGESTION u OLA_VERDE | Verde 0s (rojo total) |

**Motor de Reglas Avanzado (`motor_reglas.py`):**

El motor ahora utiliza una **memoria temporal por eje** que almacena las últimas lecturas de los 3 sensores. La decisión se toma por **cascada de prioridades** sobre el eje al completo, no por sensor individual:

| Prioridad | Regla | Umbral |
|-----------|-------|--------|
| 0 | Detección de Ambulancia | Vp < 5 km/h en Cámara o GPS |
| 1 | Umbral Crítico Absoluto | Ocupación cámara > 95% de `cola_max` |
| 2 | Tendencia Predictiva | 4 lecturas cámara estrictamente crecientes y delta > 25% de capacidad |
| 3 | Regla Combinada Estándar | Cámara > 75% Y GPS Vp < 15 km/h Y Espira > 20 veh/min (simultáneamente) |
| — | Default | NORMAL |

**Exclusión Mutua Vial (`ROJO_FORZADO`):** Al dispararse `CONGESTION` u `OLA_VERDE` en un eje, el eje transversal recibe automáticamente `ROJO_FORZADO` para evitar colisiones.

**Persistencia con Failback Automático (`persistencia.py`):**

- Marca cada evento con `sincronizado = 0` (pendiente) o `1` (sincronizado) en la réplica SQLite local.
- Si PC3 no está disponible: guarda con `sincronizado = 0` y sigue operando.
- Al recuperarse PC3: lanza automáticamente un hilo de **Batch Flush** que envía todos los registros pendientes en orden cronológico, marcando `sincronizado = 1` tras cada envío exitoso.
- Si PC3 cae durante el vaciado: el hilo aborta y retoma en el siguiente ciclo de recuperación.

**Heartbeat Activo (`heartbeat.py`):**

- Sondea PC1 (puerto 5565) y PC3 (puerto 5566) cada 10 segundos.
- El objeto `EstadoNodos` centraliza la disponibilidad de ambos nodos (thread-safe).
- La `Persistencia` y el `Suscriptor` consultan `EstadoNodos` para tomar decisiones sin abrir sockets adicionales.
- Detecta y notifica en consola las transiciones SUBIDA/CAÍDA de cada nodo.

**Servidor de Consulta de Réplica (`servidor_consulta.py`):**

- Expone un canal REP (puerto 5564) que sirve la réplica local SQLite de PC2.
- Permite al monitor de PC3 leer datos cuando su base de datos principal no responde (**failover de lectura transparente**).

**Estructura:**
```
pc2/
├── analitica.py               # Punto de entrada (7 hilos)
├── requirements.txt
├── config/
│   └── pc2_config.json        # IPs, umbrales, tiempos de semáforo, heartbeat
├── servicios/
│   ├── suscriptor.py          # Hilo SUB ← PC1 XPUB :5560
│   ├── motor_reglas.py        # Analítica avanzada por eje (3 reglas + memoria temporal)
│   ├── control_semaforos.py   # Hilo actuador de semáforos (incluye ROJO_FORZADO)
│   ├── persistencia.py        # Hilo PUSH → PC3 + SQLite + Failback automático
│   ├── servidor_control.py    # Hilo REP :5563 ← comandos de PC3
│   ├── servidor_consulta.py   # Hilo REP :5564 ← consulta de réplica (failover)
│   └── heartbeat.py           # HeartbeatCliente + EstadoNodos
└── replica/
    └── trafico.db             # Réplica SQLite (columna `sincronizado` para Failback)
```

---

### PC3 — Servidor de Base de Datos (`10.43.99.78`)

Recibe todos los eventos procesados de PC2 vía PULL, los almacena en SQLite, ofrece monitoreo con conmutación dinámica de fuente de datos (failover a réplica de PC2) y una **Interfaz Gráfica (GUI)** de monitoreo y control.

**Tablas SQLite (Base de Datos Principal):**

| Tabla | Contenido |
|-------|-----------| 
| `eventos` | Histórico completo con `estado_trafico` y `motivo` |
| `estados` | Último estado conocido por eje de intersección (upsert) |
| `semaforos` | Registro de cada cambio de semáforo |

**Monitor con Failover Dinámico (`monitor_comandos.py`):**

- En cada reporte, verifica si la BD local responde (resultado cacheado 15 s).
- Si la BD local no está disponible, redirige consultas automáticamente a la réplica de PC2 vía ZMQ (puerto 5564) — **failover transparente**.
- Cuando la BD local vuelve a responder, retoma sin intervención manual.
- Muestra el resumen agrupado por intersección con estados independientes NS y EO.

**Interfaz Gráfica de Usuario (`gui.py`):**

Nueva interfaz Tkinter que reemplaza la consola interactiva del hilo principal.

| Panel | Descripción |
|-------|-------------|
| Grilla 5×5 de Intersecciones | Cada celda muestra indicadores de color independientes para NS y EO |
| Consola del Operador | Selector de intersección, selector de eje, botón OLA_VERDE y botón NORMAL |
| Tab "Decisiones y Semáforos" | Log en tiempo real de cada cambio de estado (solo cuando cambia) |
| Tab "Heartbeat y Failover" | LED de estado de red, alertas de failover y reconexión |

**Colores de los Indicadores:**

| Color | Estado |
|-------|--------|
| 🟢 Verde | `CONGESTION` o `OLA_VERDE` |
| 🔴 Rojo | `ROJO_FORZADO` |
| 🟡 Amarillo | `NORMAL` |
| ⬛ Gris | Sin datos reportados aún |

**Arquitectura de la GUI (seguridad entre hilos):**
- Consulta la BD local directamente (`_leer_estados_local`) cada 500 ms usando `root.after()`, sin bloquear la ventana principal.
- El envío de comandos ZMQ (OLA_VERDE/NORMAL) se ejecuta en un hilo daemon independiente con su propio socket REQ, para no interferir con los hilos del backend.
- El LED de estado de red refleja el flag `_db_ok` del `MonitorComandos` en tiempo real.

**Estructura:**
```
pc3/
├── servidor_db.py             # Punto de entrada (lanza GUI + 3 hilos backend)
├── gui.py                     # Interfaz Gráfica Tkinter (grilla 5x5 + consola)
├── requirements.txt
├── config/
│   └── pc3_config.json        # Puertos, IP de PC2, failover
├── servicios/
│   ├── receptor.py            # Hilo PULL :5561 ← eventos de PC2
│   ├── almacenador.py         # Hilo escritura SQLite (batch 50 eventos)
│   └── monitor_comandos.py    # Monitoreo + failover DB + REQ → PC2
└── db/
    └── trafico_principal.db   # BD principal (creada en runtime)
```

---

## Configuración de IPs

Modifica **un solo archivo por nodo** antes de ejecutar:

### PC1 — `pc1/config/ciudad.json`
> PC1 no necesita IPs de otros nodos (solo hace `bind`).

```json
{
  "broker": {
    "xsub_port": 5559,
    "xpub_port": 5560
  },
  "heartbeat": {
    "rep_port": 5565
  }
}
```

### PC2 — `pc2/config/pc2_config.json`
```json
{
  "pc1": { "xpub_host": "10.43.99.192", "xpub_port": 5560 },
  "pc3": { "host": "10.43.99.78", "push_port": 5561 },
  "servidor_comandos":  { "rep_port": 5563 },
  "servidor_consulta":  { "rep_port": 5564 },
  "heartbeat": {
    "intervalo_seg": 10,
    "timeout_ms": 10000,
    "pc1_host": "10.43.99.192", "pc1_hb_port": 5565,
    "pc3_host": "10.43.99.78",  "pc3_hb_port": 5566
  }
}
```

### PC3 — `pc3/config/pc3_config.json`
```json
{
  "receptor":  { "pull_port": 5561 },
  "pc2":       { "host": "10.43.100.91", "rep_port": 5563 },
  "servidor_consulta_pc2": { "port": 5564 },
  "heartbeat": { "rep_port": 5566 }
}
```

---

## Instalación y Ejecución

### Requisitos
- Python 3.10+
- `pyzmq >= 25.0`
- `sqlite3` — incluido en Python
- `tkinter` — incluido en Python (para la GUI de PC3)

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
# PC1: broker + heartbeat
sudo ufw allow 5559/tcp
sudo ufw allow 5560/tcp
sudo ufw allow 5565/tcp

# PC2: comandos + consulta réplica
sudo ufw allow 5563/tcp
sudo ufw allow 5564/tcp

# PC3: recepción + heartbeat
sudo ufw allow 5561/tcp
sudo ufw allow 5566/tcp
```

---

## Interfaz Gráfica (PC3)

Al ejecutar `servidor_db.py`, se abre automáticamente la ventana de monitoreo. La consola interactiva anterior ha sido reemplazada por la GUI.

- **Grilla 5×5**: Cada celda de intersección muestra dos indicadores de color (NS y EO) actualizados cada 500 ms.
- **Consola del Operador** (panel lateral derecho): Selecciona intersección, eje, y envía comandos `OLA_VERDE` o `NORMAL` directamente a PC2.
- **Tab Decisiones**: Log de cada cambio de estado detectado, con posición, motivo, sensor disparador y timestamp.
- **Tab Heartbeat**: LED de estado (verde = conectado, rojo = failover activo), con log de alertas de caídas y reconexiones automáticas.

---

## Pruebas Locales (todo en una sola máquina)

Cambia `pc2_config.json`:
```json
"pc1": { "xpub_host": "localhost" },
"pc3": { "host": "localhost" },
"heartbeat": {
  "pc1_host": "localhost",
  "pc3_host": "localhost"
}
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

# Terminal 3 (abre GUI automáticamente)
python3 pc3/servidor_db.py
```

### Scripts de verificación

```bash
# Verificar PC1 (captura mensajes del broker)
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
[ON]  [CAM_A1_NS] activo en INT_A1_NS - cada 30s
[ON]  [ESP_A1_NS] activo en INT_A1_NS - cada 30s
[ON]  [GPS_A1_NS] activo en INT_A1_NS - cada 30s
[HEARTBEAT] 22:41:00 ✔ PC3 disponible de nuevo — db_ok=True | eventos=450
```

### PC2
```
[ANALITICA] 22:41:35 | INT_B2_NS
  Estado : NORMAL --> CONGESTION
  Motivo : Tendencia predictiva: Incremento continuo del 32.5%
  Sensor : CAM_B2_NS (camara)
[SEMAFORO] 22:41:35 | INT_B2_NS | VERDE=30s (extendido) / ROJO=5s | Estado: CONGESTION
[SEMAFORO] 22:41:35 | INT_B2_EO | VERDE=0s / ROJO=30s              | Estado: ROJO_FORZADO

[PERSISTENCIA] PC3 disponible de nuevo. Pasando a modo 'Sincronizando'...
[PERSISTENCIA] ✔ Batch Flush completado. 87 eventos sincronizados correctamente.
```

### PC3
```
[ALMACENADOR] +50 eventos | Total: 150
[GUI] Actualizando grilla — 25 intersecciones, 50 ejes monitoreados
```

---

## Resiliencia ante Fallos

| Fallo | Comportamiento |
|-------|---------------|
| PC3 caído | PC2 guarda en réplica local con `sincronizado = 0`; el heartbeat lo detecta en ≤10 s |
| PC3 regresa | PC2 lanza Batch Flush automático; sincroniza pendientes en orden cronológico |
| BD de PC3 caída (PC3 vivo) | Monitor de PC3 redirige consultas a réplica de PC2 vía puerto 5564 (failover transparente) |
| PC1 caído | PC2 mantiene últimos estados conocidos en memoria; el heartbeat notifica la caída |
| PC2 caído | PC1 sigue generando (el broker descarta si no hay suscriptor); PC3 muestra indicadores en Gris |
| Sensor caído | El hilo de ese sensor se detiene; los otros 5 de la misma intersección siguen activos |
| Red intermitente | El socket REQ de heartbeat se recrea automáticamente tras cada timeout |