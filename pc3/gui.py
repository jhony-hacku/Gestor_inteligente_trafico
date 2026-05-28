import tkinter as tk
from tkinter import ttk
import threading
from pathlib import Path
import zmq
from datetime import datetime, timezone
import json
import sqlite3

# Directorio de claves CurveZMQ de PC3 (relativo a gui.py)
_KEYS_DIR = Path(__file__).parent / "keys"

# Importar utilidades de criptografía (modo estricto: falla si faltan claves)
from servicios.cripto import aplicar_curve_cliente

class MonitorGUI:
    def __init__(self, root, monitor, stop_event):
        self.root = root
        self.monitor = monitor
        self.stop_event = stop_event
        self.root.title("PC3 - Monitor de Tráfico (GUI)")
        self.root.geometry("1100x620")  # Aumentado para mejor distribución

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
        # Panel superior para grid y controles (ocupa toda la pantalla)
        self.top_frame = ttk.Frame(self.main_frame)
        self.top_frame.pack(fill=tk.BOTH, expand=True)

        # Panel izquierdo para la grilla (5x5)
        self.grid_frame = ttk.Frame(self.top_frame)
        self.grid_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Panel derecho para los controles (Consola)
        self.control_frame = ttk.LabelFrame(self.top_frame, text="Consola del Operador", padding=15)
        self.control_frame.pack(side=tk.RIGHT, fill=tk.Y, padx=10)

        # Variables de estado
        self.last_states = {}
        self.last_db_ok = None
        self.fig_canvas = None

        self.cells = {}
        self.create_grid()
        self.create_controls()

        # Iniciar el bucle de actualización
        self.update_gui()

    def _log_decision(self, texto):
        print(f"[DECISION] {texto}")

    def _log_alert(self, texto):
        print(f"[ALERTA] {texto}")

    def create_grid(self):
        rows = ['A', 'B', 'C', 'D', 'E']
        cols = ['1', '2', '3', '4', '5']

        for i, r in enumerate(rows):
            for j, c in enumerate(cols):
                cruce = f"INT_{r}{c}"
                
                # Contenedor para cada intersección (Diseño plano, relieve flat, bd=1, fondo gris neutro)
                cell = tk.LabelFrame(
                    self.grid_frame, text=cruce, relief="flat", bd=1, 
                    bg="#e8e8e8", font=('Arial', 9, 'bold'), fg="#333333"
                )
                cell.grid(row=i, column=j, padx=8, pady=6, sticky="nsew")

                # Indicador NS (Círculo en Canvas)
                tk.Label(cell, text="NS:", font=('Arial', 9), bg="#e8e8e8", fg="#333333").grid(row=0, column=0, padx=(10, 2), pady=4, sticky="e")
                ns_canvas = tk.Canvas(cell, width=20, height=20, bg="#e8e8e8", highlightthickness=0)
                ns_circle = ns_canvas.create_oval(2, 2, 18, 18, fill="gray", outline="#b0b0b0", width=1)
                ns_canvas.grid(row=0, column=1, padx=5, pady=4)
                
                # Indicador EO (Círculo en Canvas)
                tk.Label(cell, text="EO:", font=('Arial', 9), bg="#e8e8e8", fg="#333333").grid(row=1, column=0, padx=(10, 2), pady=4, sticky="e")
                eo_canvas = tk.Canvas(cell, width=20, height=20, bg="#e8e8e8", highlightthickness=0)
                eo_circle = eo_canvas.create_oval(2, 2, 18, 18, fill="gray", outline="#b0b0b0", width=1)
                eo_canvas.grid(row=1, column=1, padx=5, pady=4)

                self.cells[cruce] = {
                    "NS": {"canvas": ns_canvas, "circle": ns_circle},
                    "EO": {"canvas": eo_canvas, "circle": eo_circle}
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
        btn_ola.pack(pady=(20, 5), fill=tk.X)
        
        btn_normal = ttk.Button(self.control_frame, text="Forzar NORMAL", command=self.send_normal)
        btn_normal.pack(pady=5, fill=tk.X)

        # Botón estilizado para Generar Reporte Visual
        btn_reporte = ttk.Button(self.control_frame, text="Generar Reporte Visual", command=self.generar_reporte_visual)
        btn_reporte.pack(pady=(20, 5), fill=tk.X)

        self.status_label = ttk.Label(self.control_frame, text="Sistema listo.", wraplength=200, foreground="blue", font=('Arial', 10))
        self.status_label.pack(pady=15)

        # Separador y sección de Estado de Red / Failover integrada al final de la consola
        ttk.Separator(self.control_frame, orient='horizontal').pack(fill=tk.X, pady=(20, 15))
        
        self.status_header_frame = ttk.Frame(self.control_frame)
        self.status_header_frame.pack(fill=tk.X, anchor="w")
        
        self.led_canvas = tk.Canvas(self.status_header_frame, width=20, height=20, bg="#f0f0f0", highlightthickness=0)
        self.led_canvas.pack(side=tk.LEFT, padx=(0, 5))
        self.led_circle = self.led_canvas.create_oval(2, 2, 18, 18, fill="gray", outline="#7f7f7f")
        
        self.lbl_network_status = ttk.Label(self.status_header_frame, text="Iniciando red...", font=('Arial', 9, 'bold'))
        self.lbl_network_status.pack(side=tk.LEFT)

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
        self.lbl_network_status.config(text="Failover Activo", foreground="red")
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

            # Aplicar CurveZMQ ANTES de connect() — falla si faltan claves
            aplicar_curve_cliente(sock, _KEYS_DIR)

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
            cruce["NS"]["canvas"].itemconfig(cruce["NS"]["circle"], fill="gray")
            cruce["EO"]["canvas"].itemconfig(cruce["EO"]["circle"], fill="gray")

        # Actualizar colores con los datos reales
        for r in rows:
            posicion, estado, motivo, sensor, ts = r
            
            state_key = posicion
            current_state = f"{estado}|{motivo}"
            
            # Registrar decisiones en consola (stdout)
            if state_key not in self.last_states or self.last_states[state_key] != current_state:
                log_msg = f"[{posicion}] Estado: {estado} | Motivo: {motivo} | Sensor: {sensor} | TS: {ts}"
                self._log_decision(log_msg)
                self.last_states[state_key] = current_state

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
                    
                self.cells[base][direccion]["canvas"].itemconfig(self.cells[base][direccion]["circle"], fill=color)

        # Actualizar LED de estado de red
        db_ok = getattr(self.monitor, '_db_ok', True)
        
        if self.last_db_ok is None or self.last_db_ok != db_ok:
            if db_ok:
                self.led_canvas.itemconfig(self.led_circle, fill="#00ff00") # Verde
                self.lbl_network_status.config(text="Heartbeat OK", foreground="green")
                if self.last_db_ok is False:
                    self._log_alert("Conexión restablecida. Sincronización completada.")
            else:
                self.led_canvas.itemconfig(self.led_circle, fill="#ff0000") # Rojo
                self.lbl_network_status.config(text="Failover Activo", foreground="red")
                self._log_alert("🚨 FALLO DE RED DETECTADO: Failover activo en SQLite local")
            
            self.last_db_ok = db_ok

        # Reprogramar la actualización en 500 ms (tasa de refresco constante)
        self.root.after(500, self.update_gui)

    # ==============================================================================
    # INTEGRACIÓN DE MATPLOTLIB (ANALÍTICA Y REPORTES)
    # ==============================================================================
    # Colores del tema oscuro para la ventana de reportes
    _BG_DARK    = "#0d0d1f"
    _BG_PANEL   = "#13132b"
    _FG_TITLE   = "#e8e8ff"
    _FG_MUTED   = "#8888aa"
    _ACCENT     = "#00d4ff"

    def generar_reporte_visual(self):
        """Abre la ventana de Analítica premium y lanza la consulta asíncrona."""
        BD = self._BG_DARK
        BP = self._BG_PANEL

        if not hasattr(self, "report_win") or not self.report_win.winfo_exists():
            self.report_win = tk.Toplevel(self.root)
            self.report_win.title("Analítica y Reportes — Gestor Inteligente de Tráfico")
            self.report_win.geometry("1000x780")
            self.report_win.configure(bg=BD)

            # --- Header premium ---
            header = tk.Frame(self.report_win, bg=BP, pady=0)
            header.pack(fill=tk.X)

            tk.Label(
                header,
                text="📊  Analítica Histórica y Métricas de Desempeño",
                font=("Arial", 15, "bold"), bg=BP, fg=self._FG_TITLE, pady=14, padx=20
            ).pack(side=tk.LEFT)

            tk.Label(
                header, text="Gestor Inteligente de Tráfico · PC3",
                font=("Arial", 9), bg=BP, fg=self._FG_MUTED, padx=20
            ).pack(side=tk.RIGHT, pady=18)

            # Separador
            sep = tk.Frame(self.report_win, bg=self._ACCENT, height=2)
            sep.pack(fill=tk.X)

            # --- Panel de estadísticas (se llena tras consulta) ---
            self.stats_frame = tk.Frame(self.report_win, bg=BD, pady=6)
            self.stats_frame.pack(fill=tk.X, padx=20, pady=(8, 0))

            # --- Área de gráficas scrollable ---
            canvas_scroll = tk.Canvas(self.report_win, bg=BD, highlightthickness=0)
            scrollbar = tk.Scrollbar(
                self.report_win, orient="vertical", command=canvas_scroll.yview,
                bg=BP, troughcolor=BD
            )
            canvas_scroll.configure(yscrollcommand=scrollbar.set)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            canvas_scroll.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

            self.report_frame = tk.Frame(canvas_scroll, bg=BD)
            self._scroll_win_id = canvas_scroll.create_window(
                (0, 0), window=self.report_frame, anchor="nw"
            )

            def _on_frame_configure(event):
                canvas_scroll.configure(scrollregion=canvas_scroll.bbox("all"))
            def _on_canvas_configure(event):
                canvas_scroll.itemconfig(self._scroll_win_id, width=event.width)

            self.report_frame.bind("<Configure>", _on_frame_configure)
            canvas_scroll.bind("<Configure>", _on_canvas_configure)
            self._report_canvas_scroll = canvas_scroll

            self.lbl_loading = tk.Label(
                self.report_frame,
                text="⏳  Consultando base de datos...",
                font=("Arial", 13, "italic"), bg=BD, fg=self._FG_MUTED
            )
            self.lbl_loading.pack(expand=True, pady=60)

            self.fig_canvas = None
        else:
            self.report_win.lift()
            for widget in self.report_frame.winfo_children():
                try:
                    widget.destroy()
                except Exception:
                    pass
            for widget in self.stats_frame.winfo_children():
                try:
                    widget.destroy()
                except Exception:
                    pass
            self.fig_canvas = None
            self.lbl_loading = tk.Label(
                self.report_frame,
                text="⏳  Consultando base de datos...",
                font=("Arial", 13, "italic"), bg=self._BG_DARK, fg=self._FG_MUTED
            )
            self.lbl_loading.pack(expand=True, pady=60)

        cruce = self.combo_int.get()
        eje   = self.combo_eje.get()
        posicion = f"{cruce}{eje}"

        self.status_label.config(text="Generando reporte visual...", foreground="#00aaff")
        print(f"[REPORTE] Iniciando consulta para posicion='{posicion}'")

        threading.Thread(target=self._query_data_thread, args=(posicion,), daemon=True).start()

    def _query_data_thread(self, posicion):
        """Hilo de consulta SQLite y procesamiento de datos."""
        try:
            db_path = self.monitor._ruta_db()
            conn = sqlite3.connect(str(db_path))
            
            # Auxiliar para parsear fechas
            def parse_ts(ts_str):
                if not ts_str:
                    return None
                try:
                    return datetime.fromisoformat(ts_str)
                except Exception:
                    try:
                        return datetime.strptime(ts_str.split('.')[0], "%Y-%m-%dT%H:%M:%S")
                    except Exception:
                        return None

            # 1. Consulta para Gráfico 1 (Histórico de Congestión en Intersección seleccionada)
            # Limitar a los últimos 200 eventos de este eje para legibilidad
            rows_congestion = conn.execute("""
                SELECT timestamp, tipo, datos_json, estado_trafico 
                FROM eventos 
                WHERE posicion = ? 
                ORDER BY timestamp DESC 
                LIMIT 200
            """, (posicion,)).fetchall()
            rows_congestion = list(reversed(rows_congestion))

            # 2. Consulta para Gráfico 2 (Volumen y Latencia en bloque de tiempo de la simulación)
            # Analizar últimos 1000 eventos globales
            rows_performance = conn.execute("""
                SELECT timestamp, timestamp_ingreso 
                FROM eventos 
                ORDER BY timestamp DESC 
                LIMIT 1000
            """).fetchall()
            rows_performance = list(reversed(rows_performance))

            conn.close()

            # --- AGREGACIÓN GRÁFICO 1 ---
            x_congestion = []
            y_cola = []
            y_conteo = []

            if rows_congestion:
                min_time = parse_ts(rows_congestion[0][0])
                if min_time:
                    # Agrupar en cubetas de 30 segundos
                    buckets = {}
                    for row in rows_congestion:
                        ts, tipo, datos_json, estado = row
                        dt = parse_ts(ts)
                        if not dt:
                            continue
                        
                        try:
                            data = json.loads(datos_json)
                        except Exception:
                            data = {}

                        offset = (dt - min_time).total_seconds()
                        bucket_idx = int(offset // 30)

                        if bucket_idx not in buckets:
                            buckets[bucket_idx] = {
                                "times": [],
                                "colas": [],
                                "conteos": []
                            }
                        buckets[bucket_idx]["times"].append(dt)
                        if tipo == "camara":
                            buckets[bucket_idx]["colas"].append(data.get("longitud_cola", 0))
                        elif tipo == "espira":
                            buckets[bucket_idx]["conteos"].append(data.get("conteo_vehicular", 0))

                    for b_idx in sorted(buckets.keys()):
                        b = buckets[b_idx]
                        avg_dt = b["times"][0] + (b["times"][-1] - b["times"][0]) / 2 if len(b["times"]) > 1 else b["times"][0]
                        x_congestion.append(avg_dt.strftime("%H:%M:%S"))
                        y_cola.append(sum(b["colas"]) / len(b["colas"]) if b["colas"] else 0.0)
                        y_conteo.append(sum(b["conteos"]) if b["conteos"] else 0.0)

            # --- AGREGACIÓN GRÁFICO 2 ---
            x_perf = []
            y_volume = []
            y_latency = []

            if rows_performance:
                min_time_all = parse_ts(rows_performance[0][0])
                if min_time_all:
                    perf_buckets = {}
                    for row in rows_performance:
                        ts_orig, ts_ing = row
                        dt_orig = parse_ts(ts_orig)
                        dt_ing = parse_ts(ts_ing)
                        if not dt_orig or not dt_ing:
                            continue
                        
                        # Latencia en segundos (mínimo 0.001 para robustez)
                        latency = max(0.001, (dt_ing - dt_orig).total_seconds())

                        offset = (dt_orig - min_time_all).total_seconds()
                        b_idx = int(offset // 30)  # Bloques de 30 segundos

                        if b_idx not in perf_buckets:
                            perf_buckets[b_idx] = {
                                "times": [],
                                "latencies": [],
                                "count": 0
                            }
                        perf_buckets[b_idx]["times"].append(dt_orig)
                        perf_buckets[b_idx]["latencies"].append(latency)
                        perf_buckets[b_idx]["count"] += 1

                    for b_idx in sorted(perf_buckets.keys()):
                        b = perf_buckets[b_idx]
                        avg_dt = b["times"][0] + (b["times"][-1] - b["times"][0]) / 2 if len(b["times"]) > 1 else b["times"][0]
                        x_perf.append(avg_dt.strftime("%H:%M:%S"))
                        y_volume.append(b["count"])
                        
                        # Latencia en milisegundos
                        avg_lat_ms = (sum(b["latencies"]) / len(b["latencies"])) * 1000
                        y_latency.append(avg_lat_ms)

            # Enviar datos calculados a la UI principal
            self.root.after(0, lambda: self._update_charts_ui(
                posicion, x_congestion, y_cola, y_conteo, x_perf, y_volume, y_latency
            ))

        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"[REPORTE] Error procesando reporte: {e}")
            # Desvanecer la pantalla de carga y pintar estados vacíos
            self.root.after(0, lambda err=e: self.status_label.config(
                text=f"Error consulta: {err}", foreground="red"
            ))
            self.root.after(0, lambda: self._update_charts_ui(posicion, [], [], [], [], [], []))

    def _update_charts_ui(self, posicion, x_congestion, y_cola, y_conteo, x_perf, y_volume, y_latency):
        """
        Renderiza las gráficas con estilo oscuro profesional.
        Fallback a tablas de texto si matplotlib no está disponible.
        """
        BD = self._BG_DARK
        print(f"[REPORTE] _update_charts_ui llamado. congestion_pts={len(x_congestion)}, perf_pts={len(x_perf)}")

        # Limpiar widgets anteriores
        if self.fig_canvas:
            try:
                self.fig_canvas.get_tk_widget().destroy()
            except Exception:
                pass
            self.fig_canvas = None

        for widget in self.report_frame.winfo_children():
            try:
                widget.destroy()
            except Exception:
                pass

        try:
            from matplotlib.figure import Figure
            from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
            from matplotlib.ticker import MaxNLocator
            print("[REPORTE] matplotlib importado OK")

            # ----------------------------------------------------------------
            # Paleta y estilos (tema oscuro profesional)
            # ----------------------------------------------------------------
            BG_FIG  = "#0d0d1f"   # Fondo figura
            BG_AX   = "#13132b"   # Fondo ejes
            CLR_GRID= "#1e1e3a"   # Línea de cuadrícula
            CLR_TXT = "#c8c8e8"   # Texto general
            CLR_LBL = "#8888aa"   # Etiquetas de ejes

            # Gráfico 1
            C_COLA   = "#00d4ff"  # Cian eléctrico  — Cola (cámara)
            C_COLA_F = "#00d4ff22"  # Fill transparente
            C_FLUJO  = "#ff8c42"  # Coral — Flujo (espira)
            C_FLUJO_F= "#ff8c4222"

            # Gráfico 2
            C_VOL    = "#00e676"  # Verde menta — Solicitudes
            C_VOL_F  = "#00e67644"
            C_LAT    = "#ff4569"  # Rosa-rojo — Latencia

            # ----------------------------------------------------------------
            # Panel de estadísticas rápidas
            # ----------------------------------------------------------------
            stats = [
                ("Eje vial",      posicion,                               "#00d4ff"),
                ("Eventos analizados", str(len(x_congestion) + len(x_perf)), "#e8e8ff"),
                ("Cola máx (veh)", f"{max(y_cola):.0f}" if y_cola else "N/A", "#ff8c42"),
                ("Flujo máx",     f"{max(y_conteo):.0f}" if y_conteo else "N/A", "#ff8c42"),
                ("Latencia prom",  f"{sum(y_latency)/len(y_latency):.1f} ms" if y_latency else "N/A", "#ff4569"),
            ]
            for lbl, val, col in stats:
                card = tk.Frame(self.stats_frame, bg="#13132b", padx=14, pady=6, relief="flat")
                card.pack(side=tk.LEFT, padx=6, pady=2)
                tk.Label(card, text=lbl, font=("Arial", 8), bg="#13132b", fg="#8888aa").pack()
                tk.Label(card, text=val, font=("Arial", 12, "bold"), bg="#13132b", fg=col).pack()

            # ----------------------------------------------------------------
            # Figura Matplotlib
            # ----------------------------------------------------------------
            fig = Figure(figsize=(9.6, 7.2), dpi=100)
            fig.patch.set_facecolor(BG_FIG)
            fig.subplots_adjust(left=0.08, right=0.92, top=0.93, bottom=0.10, hspace=0.42)

            # Función auxiliar para estilizar ejes
            def _style_ax(ax, title):
                ax.set_facecolor(BG_AX)
                ax.set_title(title, color=CLR_TXT, fontsize=11, fontweight="bold", pad=10)
                ax.tick_params(colors=CLR_LBL, labelsize=8)
                ax.xaxis.label.set_color(CLR_LBL)
                ax.yaxis.label.set_color(CLR_LBL)
                for spine in ax.spines.values():
                    spine.set_edgecolor("#1e1e3a")
                ax.grid(True, linestyle="--", linewidth=0.5, color=CLR_GRID, alpha=0.8)

            # ============================================================
            # GRÁFICO 1: Histórico de Congestión
            # ============================================================
            ax1 = fig.add_subplot(211)
            _style_ax(ax1, f"Evolución de Tráfico — {posicion}  (intervalos 30 s)")

            if x_congestion:
                xs = list(range(len(x_congestion)))  # índices numéricos para evitar solapamiento

                # --- Cola (cámara) — línea cian con área rellena ---
                ax1.fill_between(xs, y_cola, color=C_COLA_F)
                ax1.plot(xs, y_cola, color=C_COLA, linewidth=2.0,
                         marker="o", markersize=4, markerfacecolor=C_COLA,
                         markeredgecolor=BG_FIG, label="Cola prom. (cámara)")
                ax1.set_ylabel("Cola  (vehículos)", color=C_COLA, fontsize=9)
                ax1.tick_params(axis='y', labelcolor=C_COLA, labelsize=8)

                # --- Flujo (espira) — línea coral con área rellena — eje derecho ---
                ax1r = ax1.twinx()
                ax1r.set_facecolor(BG_AX)
                ax1r.fill_between(xs, y_conteo, color=C_FLUJO_F)
                ax1r.plot(xs, y_conteo, color=C_FLUJO, linewidth=1.8,
                          linestyle="--", marker="s", markersize=3,
                          markerfacecolor=C_FLUJO, markeredgecolor=BG_FIG,
                          label="Flujo (espira)")
                ax1r.set_ylabel("Vehículos / min", color=C_FLUJO, fontsize=9)
                ax1r.tick_params(axis='y', labelcolor=C_FLUJO, labelsize=8)
                for spine in ax1r.spines.values():
                    spine.set_edgecolor("#1e1e3a")

                # Eje X: mostrar máximo 10 etiquetas de tiempo
                step = max(1, len(x_congestion) // 10)
                tick_pos = list(range(0, len(xs), step))
                ax1.set_xticks(tick_pos)
                ax1.set_xticklabels(
                    [x_congestion[i] for i in tick_pos],
                    rotation=30, ha="right", fontsize=8, color=CLR_LBL
                )

                # Leyenda combinada
                h1, l1 = ax1.get_legend_handles_labels()
                h2, l2 = ax1r.get_legend_handles_labels()
                ax1.legend(h1 + h2, l1 + l2, loc="upper left", fontsize=8,
                           facecolor="#1e1e3a", edgecolor="#333355",
                           labelcolor=CLR_TXT, framealpha=0.85)
            else:
                ax1.text(0.5, 0.5,
                         "Sin datos para este eje vial.\nEjecuta la simulación y vuelve a generar el reporte.",
                         ha="center", va="center", fontsize=11, color="#8888aa",
                         transform=ax1.transAxes)

            # ============================================================
            # GRÁFICO 2: Volumen y Latencia
            # ============================================================
            ax2 = fig.add_subplot(212)
            _style_ax(ax2, "Volumen de Solicitudes  y  Latencia de Procesamiento  (bloques 30 s)")

            if x_perf:
                xs2 = list(range(len(x_perf)))
                bar_w = 0.6

                # --- Barras de volumen — eje izquierdo ---
                bars = ax2.bar(xs2, y_volume, width=bar_w, color=C_VOL,
                               alpha=0.75, label="Solicitudes", zorder=3)
                ax2.set_ylabel("Nú m. solicitudes", color=C_VOL, fontsize=9)
                ax2.tick_params(axis='y', labelcolor=C_VOL, labelsize=8)

                # Valor encima de cada barra
                for bar in bars:
                    h = bar.get_height()
                    if h > 0:
                        ax2.text(bar.get_x() + bar.get_width() / 2, h + max(y_volume) * 0.01,
                                 f"{int(h)}", ha="center", va="bottom",
                                 fontsize=7, color=C_VOL, fontweight="bold")

                # --- Línea de latencia — eje derecho ---
                ax2r = ax2.twinx()
                ax2r.set_facecolor(BG_AX)
                ax2r.fill_between(xs2, y_latency, color="#ff456920")
                ax2r.plot(xs2, y_latency, color=C_LAT, linewidth=2.2,
                          marker="D", markersize=5,
                          markerfacecolor=C_LAT, markeredgecolor=BG_FIG,
                          label="Latencia prom.", zorder=4)
                ax2r.set_ylabel("Latencia  (ms)", color=C_LAT, fontsize=9)
                ax2r.tick_params(axis='y', labelcolor=C_LAT, labelsize=8)
                for spine in ax2r.spines.values():
                    spine.set_edgecolor("#1e1e3a")

                # Eje X: máximo 10 etiquetas
                step2 = max(1, len(x_perf) // 10)
                tick_pos2 = list(range(0, len(xs2), step2))
                ax2.set_xticks(tick_pos2)
                ax2.set_xticklabels(
                    [x_perf[i] for i in tick_pos2],
                    rotation=30, ha="right", fontsize=8, color=CLR_LBL
                )

                # Leyenda combinada
                h1, l1 = ax2.get_legend_handles_labels()
                h2, l2 = ax2r.get_legend_handles_labels()
                ax2.legend(h1 + h2, l1 + l2, loc="upper left", fontsize=8,
                           facecolor="#1e1e3a", edgecolor="#333355",
                           labelcolor=CLR_TXT, framealpha=0.85)
            else:
                ax2.text(0.5, 0.5,
                         "Base de datos vacía. No hay métricas disponibles.",
                         ha="center", va="center", fontsize=11, color="#8888aa",
                         transform=ax2.transAxes)

            # Incrustar en Tkinter
            self.fig_canvas = FigureCanvasTkAgg(fig, master=self.report_frame)
            self.fig_canvas.draw()
            self.fig_canvas.get_tk_widget().configure(bg=BD)
            self.fig_canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

            self.status_label.config(text="✓  Reporte visual generado.", foreground="#00cc66")

        except ImportError as ie:
            print(f"[REPORTE] matplotlib no disponible: {ie}")
            # Fallback: mostrar datos en tabla de texto Tkinter si matplotlib no está disponible
            self._mostrar_reporte_texto(posicion, x_congestion, y_cola, y_conteo, x_perf, y_volume, y_latency)

        except Exception as exc:
            import traceback
            traceback.print_exc()
            print(f"[REPORTE] Error al renderizar gráfico: {exc}")
            self.status_label.config(text=f"Error render: {exc}", foreground="red")
            # Intentar fallback de texto igualmente
            self._mostrar_reporte_texto(posicion, x_congestion, y_cola, y_conteo, x_perf, y_volume, y_latency)

    def _mostrar_reporte_texto(self, posicion, x_congestion, y_cola, y_conteo, x_perf, y_volume, y_latency):
        """
        Fallback: renderiza los datos de analítica como tablas de texto Tkinter nativo
        cuando matplotlib no está disponible en el sistema.
        """
        from tkinter import scrolledtext
        print("[REPORTE] Renderizando modo texto (fallback).")

        aviso = tk.Label(
            self.report_frame,
            text="⚠️  Sin gráficas — mostrando datos en modo texto",
            bg="#fff3cd", fg="#7d4e00", font=("Arial", 11, "bold"),
            relief="solid", bd=1, pady=8
        )
        aviso.pack(fill=tk.X, padx=10, pady=(8, 4))

        # --- Tabla 1: Histórico de Congestión ---
        tk.Label(self.report_frame,
                 text=f"Histórico de Tráfico — {posicion} (Intervalos de 30s)",
                 font=("Arial", 11, "bold"), bg="#f5f5f7", fg="#333333"
                 ).pack(pady=(12, 2))

        txt1 = scrolledtext.ScrolledText(
            self.report_frame, height=8, bg="#ffffff",
            fg="#222222", font=("Consolas", 10), relief="solid", bd=1
        )
        txt1.pack(fill=tk.X, padx=15, pady=(0, 8))

        if x_congestion:
            txt1.insert(tk.END, f"{'Intervalo':<12}  {'Cola Prom (veh)':<18}  {'Flujo Espira (veh)'}\n")
            txt1.insert(tk.END, "-" * 52 + "\n")
            for i, t in enumerate(x_congestion):
                c = y_cola[i] if i < len(y_cola) else 0
                e = y_conteo[i] if i < len(y_conteo) else 0
                txt1.insert(tk.END, f"{t:<12}  {c:<18.1f}  {e:.0f}\n")
        else:
            txt1.insert(tk.END, "Sin datos para este eje. Ejecuta la simulación primero.\n")
        txt1.configure(state="disabled")

        # --- Tabla 2: Métricas de Desempeño ---
        tk.Label(self.report_frame,
                 text="Volumen y Latencia de Procesamiento (Bloques de 30s)",
                 font=("Arial", 11, "bold"), bg="#f5f5f7", fg="#333333"
                 ).pack(pady=(4, 2))

        txt2 = scrolledtext.ScrolledText(
            self.report_frame, height=8, bg="#ffffff",
            fg="#222222", font=("Consolas", 10), relief="solid", bd=1
        )
        txt2.pack(fill=tk.X, padx=15, pady=(0, 8))

        if x_perf:
            txt2.insert(tk.END, f"{'Intervalo':<12}  {'Solicitudes':<14}  {'Latencia Prom (ms)'}\n")
            txt2.insert(tk.END, "-" * 50 + "\n")
            for i, t in enumerate(x_perf):
                v = y_volume[i] if i < len(y_volume) else 0
                l = y_latency[i] if i < len(y_latency) else 0
                txt2.insert(tk.END, f"{t:<12}  {v:<14}  {l:.2f} ms\n")
        else:
            txt2.insert(tk.END, "Sin métricas disponibles. La base de datos está vacía.\n")
        txt2.configure(state="disabled")

        self.status_label.config(text="Reporte en modo texto generado.", foreground="#856404")

    def on_closing(self):
        print("\n[GUI] Cerrando ventana, deteniendo servicios...")
        self.stop_event.set()
        self.root.destroy()
