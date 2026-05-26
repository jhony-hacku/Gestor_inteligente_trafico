import tkinter as tk
from tkinter import ttk
import threading

class MonitorGUI:
    def __init__(self, root, monitor, stop_event):
        self.root = root
        self.monitor = monitor
        self.stop_event = stop_event
        self.root.title("PC3 - Monitor de Tráfico (GUI)")
        self.root.geometry("900x600")

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

        # Panel izquierdo para la grilla (5x5)
        self.grid_frame = ttk.Frame(self.main_frame)
        self.grid_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Panel derecho para los controles
        self.control_frame = ttk.LabelFrame(self.main_frame, text="Consola del Operador", padding=15)
        self.control_frame.pack(side=tk.RIGHT, fill=tk.Y, padx=10)

        self.cells = {}
        self.create_grid()
        self.create_controls()

        # Iniciar el bucle de actualización
        self.update_gui()

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

    def _send_cmd_thread(self, cmd, posicion):
        resp = self.monitor._enviar_comando_pc2(cmd, posicion)
        # Actualizar la interfaz en el hilo principal
        self.root.after(0, lambda: self.status_label.config(text=f"Respuesta: {resp}", foreground="black"))

    def update_gui(self):
        if self.stop_event.is_set():
            self.root.quit()
            return
            
        # Consultar la base de datos local o estado en memoria a traves del monitor
        rows = self.monitor._leer_estados()
        
        # Reiniciar todos los colores a neutro (gris) por defecto (SIN DATOS)
        for cruce in self.cells.values():
            cruce["NS"].config(bg="gray")
            cruce["EO"].config(bg="gray")

        # Actualizar colores con los datos reales
        for r in rows:
            posicion, estado, motivo, sensor, ts = r
            
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
                    # Si el estado es normal o no está explícitamente definido como rojo/verde
                    color = "yellow"
                    
                self.cells[base][direccion].config(bg=color)

        # Reprogramar la actualización en 500 ms (tasa de refresco constante)
        self.root.after(500, self.update_gui)

    def on_closing(self):
        print("\n[GUI] Cerrando ventana, deteniendo servicios...")
        self.stop_event.set()
        self.root.destroy()
