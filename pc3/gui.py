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

    def generar_reporte_visual(self):
        """Abre la ventana de Analítica y lanza la consulta asíncrona."""
        if not hasattr(self, "report_win") or not self.report_win.winfo_exists():
            self.report_win = tk.Toplevel(self.root)
            self.report_win.title("Analítica y Reportes - Gestor Inteligente de Tráfico")
            self.report_win.geometry("900x700")
            self.report_win.configure(bg="#f5f5f7")

            self.lbl_report_title = tk.Label(
                self.report_win, text="Analítica Histórica y Métricas de Desempeño",
                font=("Arial", 14, "bold"), bg="#f5f5f7", fg="#1a1a2e"
            )
            self.lbl_report_title.pack(pady=10)

            # Contenedor scrollable para que el fallback de texto sea siempre visible
            canvas_scroll = tk.Canvas(self.report_win, bg="#f5f5f7", highlightthickness=0)
            scrollbar = tk.Scrollbar(self.report_win, orient="vertical", command=canvas_scroll.yview)
            canvas_scroll.configure(yscrollcommand=scrollbar.set)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            canvas_scroll.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

            self.report_frame = tk.Frame(canvas_scroll, bg="#f5f5f7")
            self._scroll_win_id = canvas_scroll.create_window((0, 0), window=self.report_frame, anchor="nw")

            def _on_frame_configure(event):
                canvas_scroll.configure(scrollregion=canvas_scroll.bbox("all"))
            def _on_canvas_configure(event):
                canvas_scroll.itemconfig(self._scroll_win_id, width=event.width)

            self.report_frame.bind("<Configure>", _on_frame_configure)
            canvas_scroll.bind("<Configure>", _on_canvas_configure)
            self._report_canvas_scroll = canvas_scroll

            self.lbl_loading = tk.Label(
                self.report_frame, text="⏳  Consultando base de datos...",
                font=("Arial", 12, "italic"), bg="#f5f5f7", fg="#555555"
            )
            self.lbl_loading.pack(expand=True, pady=40)

            self.fig_canvas = None
        else:
            self.report_win.lift()
            # Limpiar frame para nueva consulta
            for widget in self.report_frame.winfo_children():
                try:
                    widget.destroy()
                except Exception:
                    pass
            self.fig_canvas = None
            self.lbl_loading = tk.Label(
                self.report_frame, text="⏳  Consultando base de datos...",
                font=("Arial", 12, "italic"), bg="#f5f5f7", fg="#555555"
            )
            self.lbl_loading.pack(expand=True, pady=40)

        cruce = self.combo_int.get()
        eje = self.combo_eje.get()
        posicion = f"{cruce}{eje}"

        self.status_label.config(text="Generando reporte visual...", foreground="blue")
        print(f"[REPORTE] Iniciando consulta para posicion='{posicion}'")

        # Ejecutar en hilo separado para no congelar la GUI ni ZMQ
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
        Construye y renderiza las figuras en la ventana secundaria.
        - No llama a matplotlib.use() para evitar ValueError en Linux.
        - Si matplotlib falla, muestra tablas de texto Tkinter nativas.
        - El label de carga se elimina SIEMPRE antes de renderizar.
        """
        print(f"[REPORTE] _update_charts_ui llamado. congestion_pts={len(x_congestion)}, perf_pts={len(x_perf)}")

        # Limpiar TODOS los widgets del report_frame (carga, canvas previo, texto previo)
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
            # NO llamar matplotlib.use() — causa ValueError si ya se cargó el backend
            from matplotlib.figure import Figure
            from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
            print("[REPORTE] matplotlib importado OK")

            # Crear figura Matplotlib (dos filas, una columna)
            fig = Figure(figsize=(8, 6), dpi=100)
            fig.patch.set_facecolor("#f5f5f7")

            # --- GRÁFICO 1: Histórico de Congestión ---
            ax1 = fig.add_subplot(211)
            ax1.set_facecolor("#ffffff")
            ax1.grid(True, linestyle="--", alpha=0.5, color="#cccccc")

            if x_congestion:
                line1 = ax1.plot(x_congestion, y_cola, color="#007bff", linewidth=2, marker="o", label="Cola Promedio (Cámara)")
                ax1.set_ylabel("Cola (Vehículos)", fontdict={"fontsize": 9}, color="#007bff")
                ax1.tick_params(axis='y', labelcolor="#007bff", labelsize=8)

                ax1_sec = ax1.twinx()
                line2 = ax1_sec.plot(x_congestion, y_conteo, color="#fd7e14", linewidth=1.5, linestyle="--", marker="s", label="Flujo (Espira)")
                ax1_sec.set_ylabel("Vehículos/Min", fontdict={"fontsize": 9}, color="#fd7e14")
                ax1_sec.tick_params(axis='y', labelcolor="#fd7e14", labelsize=8)

                lines = line1 + line2
                labels = [l.get_label() for l in lines]
                ax1.legend(lines, labels, loc="upper left", fontsize=8)
                ax1.set_title(f"Evolución de Tráfico en {posicion} (Intervalos de 30s)", fontdict={"fontsize": 11, "weight": "bold"}, color="#333333")
                ax1.tick_params(axis='x', rotation=15, labelsize=8)
            else:
                ax1.text(0.5, 0.5, "Sin datos de eventos para este eje.\nEjecuta la simulación y vuelve a generar el reporte.", ha="center", va="center", fontsize=10, color="#666666")
                ax1.set_title(f"Histórico de Tráfico en {posicion}", fontdict={"fontsize": 11, "weight": "bold"})

            # --- GRÁFICO 2: Métricas de Desempeño ---
            ax2 = fig.add_subplot(212)
            ax2.set_facecolor("#ffffff")
            ax2.grid(True, linestyle="--", alpha=0.5, color="#cccccc")

            if x_perf:
                bars = ax2.bar(x_perf, y_volume, color="#28a745", alpha=0.6, width=0.4, label="Solicitudes")
                ax2.set_ylabel("Volumen Solicitudes", fontdict={"fontsize": 9}, color="#28a745")
                ax2.tick_params(axis='y', labelcolor="#28a745", labelsize=8)

                ax2_sec = ax2.twinx()
                line_lat = ax2_sec.plot(x_perf, y_latency, color="#dc3545", linewidth=2, marker="d", label="Latencia")
                ax2_sec.set_ylabel("Latencia (ms)", fontdict={"fontsize": 9}, color="#dc3545")
                ax2_sec.tick_params(axis='y', labelcolor="#dc3545", labelsize=8)

                lines2 = [bars] + line_lat
                labels2 = [l.get_label() for l in lines2]
                ax2.legend(lines2, labels2, loc="upper left", fontsize=8)
                ax2.set_title("Volumen y Latencia Promedio de Procesamiento (Bloques 30s)", fontdict={"fontsize": 11, "weight": "bold"}, color="#333333")
                ax2.tick_params(axis='x', rotation=15, labelsize=8)
            else:
                ax2.text(0.5, 0.5, "Base de datos vacía. No hay métricas disponibles.", ha="center", va="center", fontsize=10, color="#666666")
                ax2.set_title("Rendimiento del Sistema", fontdict={"fontsize": 11, "weight": "bold"})

            fig.tight_layout()

            self.fig_canvas = FigureCanvasTkAgg(fig, master=self.report_frame)
            self.fig_canvas.draw()
            self.fig_canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

            self.status_label.config(text="Reporte visual generado.", foreground="green")

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
