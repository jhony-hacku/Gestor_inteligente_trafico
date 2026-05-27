import tkinter as tk
from tkinter import ttk
from tkinter import scrolledtext
import threading
import zmq
from datetime import datetime

class MonitorGUI:
    def __init__(self, root, monitor, stop_event):
        self.root = root
        self.monitor = monitor
        self.stop_event = stop_event
        self.root.title("PC3 - Monitor de Tráfico (GUI)")
        self.root.geometry("900x750")

        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

        # Configuración de estilos
        style = ttk.Style()
        style.theme_use('clam')
        style.configure("TFrame", background="#f0f0f0")
        style.configure("TLabel", background="#f0f0f0")
        style.configure("TLabelframe", background="#f0f0f0")
        style.configure("TLabelframe.Label", background="#f0f0f0", font=('Arial', 10, 'bold'))
        
        self.main_frame = ttk.Frame(self.root, padding=10)
        self.main_frame.pack(fill=tk.BOTH, expand=True)

        # === REDISEÑO DEL LAYOUT ===
        # Panel superior para grid y controles
        self.top_frame = ttk.Frame(self.main_frame)
        self.top_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        # Panel izquierdo para la grilla (5x5)
        self.grid_frame = ttk.Frame(self.top_frame)
        self.grid_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Panel derecho para los controles
        self.control_frame = ttk.LabelFrame(self.top_frame, text="Consola del Operador", padding=15)
        self.control_frame.pack(side=tk.RIGHT, fill=tk.Y, padx=10)

        # Panel inferior expandible
        self.log_frame = ttk.Frame(self.main_frame)
        self.log_frame.pack(side=tk.BOTTOM, fill=tk.X, pady=10)

        # Componente de pestañas nativo
        self.notebook = ttk.Notebook(self.log_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        # --- Pestaña 1: Decisiones y Semáforos ---
        self.tab_decisiones = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_decisiones, text=" Decisiones y Semáforos ")
        
        self.txt_decisiones = scrolledtext.ScrolledText(
            self.tab_decisiones, height=10, bg="#1e1e1e", fg="#00ff00", font=("Consolas", 10)
        )
        self.txt_decisiones.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.txt_decisiones.configure(state='disabled')

        # --- Pestaña 2: Heartbeat y Failover ---
        self.tab_resiliencia = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_resiliencia, text=" Heartbeat y Failover ")
        
        self.status_header_frame = ttk.Frame(self.tab_resiliencia)
        self.status_header_frame.pack(fill=tk.X, padx=5, pady=5)
        
        self.led_canvas = tk.Canvas(self.status_header_frame, width=20, height=20, bg="#f0f0f0", highlightthickness=0)
        self.led_canvas.pack(side=tk.LEFT, padx=(0, 5))
        self.led_circle = self.led_canvas.create_oval(2, 2, 18, 18, fill="gray")
        
        self.lbl_network_status = ttk.Label(self.status_header_frame, text="Iniciando estado de red...", font=('Arial', 10, 'bold'))
        self.lbl_network_status.pack(side=tk.LEFT)

        self.txt_alertas = scrolledtext.ScrolledText(
            self.tab_resiliencia, height=8, bg="#1e1e1e", fg="#ffffff", font=("Consolas", 10)
        )
        self.txt_alertas.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.txt_alertas.configure(state='disabled')

        # Variables de estado para no duplicar logs
        self.last_states = {}
        self.last_db_ok = None

        self.cells = {}
        self.create_grid()
        self.create_controls()

        # Iniciar el bucle de actualización
        self.update_gui()

    def _log_decision(self, texto):
        self.txt_decisiones.configure(state='normal')
        self.txt_decisiones.insert(tk.END, texto + "\n")
        
        # Búfer automático de 50 líneas máximo
        lineas = int(self.txt_decisiones.index('end-1c').split('.')[0])
        if lineas > 50:
            self.txt_decisiones.delete('1.0', f'{lineas - 50 + 1}.0')
            
        self.txt_decisiones.see(tk.END)
        self.txt_decisiones.configure(state='disabled')

    def _log_alert(self, texto):
        self.txt_alertas.configure(state='normal')
        ts = datetime.now().strftime('%H:%M:%S')
        self.txt_alertas.insert(tk.END, f"[{ts}] {texto}\n")
        self.txt_alertas.see(tk.END)
        self.txt_alertas.configure(state='disabled')

    def create_grid(self):
        rows = ['A', 'B', 'C', 'D', 'E']
        cols = ['1', '2', '3', '4', '5']

        for i, r in enumerate(rows):
            for j, c in enumerate(cols):
                cruce = f"INT_{r}{c}"
                
                # Contenedor para cada intersección
                cell = ttk.LabelFrame(self.grid_frame, text=cruce)
                cell.grid(row=i, column=j, padx=5, pady=5, sticky="nsew")

                # Indicador NS
                ttk.Label(cell, text="NS:", font=('Arial', 9)).grid(row=0, column=0, padx=5, pady=5, sticky="e")
                ns_canvas = tk.Canvas(cell, width=25, height=25, bg="gray", highlightthickness=1, highlightbackground="black")
                ns_canvas.grid(row=0, column=1, padx=5, pady=5)
                
                # Indicador EO
                ttk.Label(cell, text="EO:", font=('Arial', 9)).grid(row=1, column=0, padx=5, pady=5, sticky="e")
                eo_canvas = tk.Canvas(cell, width=25, height=25, bg="gray", highlightthickness=1, highlightbackground="black")
                eo_canvas.grid(row=1, column=1, padx=5, pady=5)

                self.cells[cruce] = {
                    "NS": ns_canvas,
                    "EO": eo_canvas
                }
                
        # Hacer que las filas y columnas se expandan de manera uniforme
        for i in range(5):
            self.grid_frame.rowconfigure(i, weight=1)
            self.grid_frame.columnconfigure(i, weight=1)

    def create_controls(self):
        ttk.Label(self.control_frame, text="Intersección:", font=('Arial', 10)).pack(pady=(10, 2), anchor="w")
        self.combo_int = ttk.Combobox(self.control_frame, state="readonly", font=('Arial', 10))
        
        intersections = [f"INT_{r}{c}" for r in ['A', 'B', 'C', 'D', 'E'] for c in ['1', '2', '3', '4', '5']]
        self.combo_int['values'] = intersections
        if intersections:
            self.combo_int.current(0)
        self.combo_int.pack(pady=2, fill=tk.X)

        ttk.Label(self.control_frame, text="Eje:", font=('Arial', 10)).pack(pady=(15, 2), anchor="w")
        self.combo_eje = ttk.Combobox(self.control_frame, state="readonly", values=["_NS", "_EO"], font=('Arial', 10))
        self.combo_eje.current(0)
        self.combo_eje.pack(pady=2, fill=tk.X)

        btn_ola = ttk.Button(self.control_frame, text="Enviar OLA_VERDE", command=self.send_ola_verde)
        btn_ola.pack(pady=(30, 5), fill=tk.X)
        
        btn_normal = ttk.Button(self.control_frame, text="Forzar NORMAL", command=self.send_normal)
        btn_normal.pack(pady=5, fill=tk.X)

        self.status_label = ttk.Label(self.control_frame, text="Sistema listo.", wraplength=200, foreground="blue", font=('Arial', 10))
        self.status_label.pack(pady=30)

    def send_ola_verde(self):
        self._dispatch_command("OLA_VERDE")

    def send_normal(self):
        self._dispatch_command("NORMAL")

    def _dispatch_command(self, cmd):
        cruce = self.combo_int.get()
        eje = self.combo_eje.get()
        if cruce and eje:
            posicion = f"{cruce}{eje}"
            self.status_label.config(text=f"Enviando {cmd} a {posicion}...", foreground="blue")
            # Enviar el comando de forma asincrónica para no bloquear la GUI
            threading.Thread(target=self._send_cmd_thread, args=(cmd, posicion), daemon=True).start()

    def _trigger_network_error(self):
        self.led_canvas.itemconfig(self.led_circle, fill="red")
        self.lbl_network_status.config(text="Failover Activo / Error ZMQ", foreground="red")
        self._log_alert("🚨 FALLO DE RED DETECTADO: PC3 fuera de línea. Failover activo en SQLite local")
        self.last_db_ok = False

    def _send_cmd_thread(self, cmd, posicion):
        try:
            # Crear un socket ZMQ independiente para este hilo y evitar errores de concurrencia
            ctx = zmq.Context.instance()
            sock = ctx.socket(zmq.REQ)
            sock.setsockopt(zmq.RCVTIMEO, 3000)
            sock.setsockopt(zmq.SNDTIMEO, 3000)
            sock.setsockopt(zmq.LINGER, 0)
            
            host = self.monitor.config["pc2"]["host"]
            port = self.monitor.config["pc2"]["rep_port"]
            sock.connect(f"tcp://{host}:{port}")
            
            sock.send_json({
                "comando": cmd.upper(),
                "posicion": posicion.upper(),
                "duracion_seg": 60,
            })
            resp_json = sock.recv_json()
            resp = resp_json.get("mensaje", str(resp_json))
            sock.close()
        except Exception as exc:
            resp = f"Error ZMQ: {exc}"
            # Capturar timeout zmq.Again para disparar alerta de red
            if "Again" in str(type(exc)) or "Timeout" in str(exc) or "Again" in str(exc):
                self.root.after(0, self._trigger_network_error)
            
        # Actualizar la interfaz en el hilo principal
        self.root.after(0, lambda: self.status_label.config(text=f"Respuesta: {resp}", foreground="black"))

    def update_gui(self):
        if self.stop_event.is_set():
            self.root.quit()
            return
            
        # Consultar la base de datos local directamente de forma thread-safe
        rows = self.monitor._leer_estados_local()
        
        # Reiniciar todos los colores a neutro (gris) por defecto (SIN DATOS)
        for cruce in self.cells.values():
            cruce["NS"].config(bg="gray")
            cruce["EO"].config(bg="gray")

        # Actualizar colores con los datos reales
        for r in rows:
            posicion, estado, motivo, sensor, ts = r
            
            # --- TAB 1: LOG DE DECISIONES Y SEMAFOROS ---
            state_key = posicion
            current_state = f"{estado}|{motivo}"
            
            # Solo loggeamos cuando el estado o motivo de una intersección cambia
            if state_key not in self.last_states or self.last_states[state_key] != current_state:
                log_msg = f"[{posicion}] Estado: {estado} | Motivo: {motivo} | Sensor: {sensor} | TS: {ts}"
                self._log_decision(log_msg)
                self.last_states[state_key] = current_state
            # --------------------------------------------

            if posicion.endswith("_NS") or posicion.endswith("_EO"):
                base = posicion[:-3]
                direccion = posicion[-2:] # "NS" o "EO"
            else:
                base = posicion
                direccion = "NS" # Fallback
                
            if base in self.cells and direccion in ["NS", "EO"]:
                estado_upper = str(estado).upper()
                
                # Lógica de Colores por Estado
                if "CONGESTION" in estado_upper or "OLA_VERDE" in estado_upper:
                    color = "green"
                elif "ROJO" in estado_upper:
                    color = "red"
                else:
                    color = "yellow"
                    
                self.cells[base][direccion].config(bg=color)

        # --- TAB 2: HEARTBEAT, RESILIENCIA Y FAILOVER ---
        db_ok = getattr(self.monitor, '_db_ok', True)
        
        if self.last_db_ok is None or self.last_db_ok != db_ok:
            if db_ok:
                self.led_canvas.itemconfig(self.led_circle, fill="#00ff00") # Verde
                self.lbl_network_status.config(text="Conectado / Heartbeat OK", foreground="green")
                if self.last_db_ok is False:
                    self._log_alert("Conexión restablecida. Sincronización automática de datos en lote completada.")
            else:
                self.led_canvas.itemconfig(self.led_circle, fill="#ff0000") # Rojo brillante
                self.lbl_network_status.config(text="Failover Activo", foreground="red")
                self._log_alert("🚨 FALLO DE RED DETECTADO: PC3 fuera de línea. Failover activo en SQLite local")
            
            self.last_db_ok = db_ok
        # ------------------------------------------------

        # Reprogramar la actualización en 500 ms (tasa de refresco constante)
        self.root.after(500, self.update_gui)

    def on_closing(self):
        print("\n[GUI] Cerrando ventana, deteniendo servicios...")
        self.stop_event.set()
        self.root.destroy()
