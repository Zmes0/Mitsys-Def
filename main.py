"""
Aplicación principal de Mitsy's POS
"""
import tkinter as tk
from tkinter import messagebox
from config import COLORS, FONTS, WINDOW_CONFIG, DENOMINACIONES
from database import db
from utils import get_current_date

class MitsysPOS:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Mitsy's POS")
        self.root.geometry("600x700")
        self.root.configure(bg=COLORS['bg_primary'])
        
        # NO OCULTAR LA VENTANA - Dejarla visible pero vacía
        # La llenaremos después del splash
        
        # Centrar ventana principal
        self.center_window(self.root, 600, 700)
        
        # Mostrar splash screen
        self.show_splash()
    
    def center_window(self, window, width, height):
        """Centra una ventana en la pantalla"""
        window.update_idletasks()
        x = (window.winfo_screenwidth() // 2) - (width // 2)
        y = (window.winfo_screenheight() // 2) - (height // 2)
        window.geometry(f"{width}x{height}+{x}+{y}")
    
    def show_splash(self):
        """Muestra la pantalla de bienvenida"""
        # Ocultar el root temporalmente
        self.root.withdraw()
        
        self.splash = tk.Toplevel(self.root)
        self.splash.title("")
        
        # Sin bordes y siempre al frente
        self.splash.overrideredirect(True)
        self.splash.attributes('-topmost', True)
        
        # Configurar
        self.splash.configure(bg=COLORS['bg_primary'])
        self.center_window(self.splash, 600, 400)
        
        # Contenido
        frame = tk.Frame(self.splash, bg=COLORS['bg_primary'])
        frame.pack(expand=True)
        
        tk.Label(frame, text="Welcome to", font=('Segoe UI', 20),
                bg=COLORS['bg_primary'], fg=COLORS['text_primary']).pack(pady=(0, 10))
        
        tk.Label(frame, text="Mitsy's Point of Sale", font=('Segoe UI', 32, 'bold'),
                bg=COLORS['bg_primary'], fg=COLORS['accent']).pack(pady=(0, 20))
        
        tk.Label(frame, text="By Sebas and Paola", font=('Segoe UI', 16),
                bg=COLORS['bg_primary'], fg=COLORS['text_secondary']).pack()
        
        # Programar cierre del splash
        self.splash.after(WINDOW_CONFIG['splash_duration'], self.close_splash)
    
    def close_splash(self):
        """Cierra el splash y continúa con el flujo"""
        try:
            self.splash.destroy()
        except:
            pass
        
        # Mostrar root de nuevo
        self.root.deiconify()
        
        # Verificar dinero en caja
        self.check_dinero_caja()
    
    def check_dinero_caja(self):
        """Verifica si se debe ingresar dinero en caja"""
        if not db.check_dinero_ingresado_hoy():
            self.show_dinero_caja_window()
        else:
            self.show_main_menu()
    
    def show_dinero_caja_window(self):
        """Muestra la ventana para ingresar dinero en caja"""
        DineroCajaWindow(self.root, callback=self.show_main_menu)
    
    def show_main_menu(self):
        """Muestra el menú principal"""
        # Limpiar ventana principal
        for widget in self.root.winfo_children():
            widget.destroy()
        
        # Asegurar que el root está visible
        self.root.deiconify()
        
        # Configurar ventana
        self.root.title("Mitsy's POS - Menú Principal")
        self.center_window(self.root, 600, 700)
        
        # Forzar al frente
        self.root.lift()
        self.root.attributes('-topmost', True)
        self.root.after(100, lambda: self.root.attributes('-topmost', False))
        self.root.focus_force()
        
        # Frame principal
        main_frame = tk.Frame(self.root, bg=COLORS['bg_primary'])
        main_frame.pack(expand=True, fill=tk.BOTH)
        
        # Contenedor centrado
        center_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        center_frame.place(relx=0.5, rely=0.5, anchor='center')
        
        # Título
        tk.Label(center_frame, text="Sistema POS", font=FONTS['title'],
                bg=COLORS['bg_primary'], fg=COLORS['text_primary']).pack(pady=(0, 40))
        
        # Botones del menú
        menu_options = [
            ("Punto de Venta", self.open_punto_venta),
            ("Productos", self.open_productos),
            ("Materia Prima", self.open_ingredientes),
            ("Recetas", self.open_recetas),
            ("Stock", self.open_stock),
            ("Historial de Ventas", self.open_historial),
            ("Cortes", self.open_cortes),
            ("Salir", self.salir)
        ]
        
        for text, command in menu_options:
            # Color especial para el botón Salir
            bg_color = COLORS['danger'] if text == "Salir" else COLORS['button_bg']
            fg_color = 'white' if text == "Salir" else COLORS['text_primary']
            
            btn = tk.Button(center_frame, text=text, command=command,
                          font=FONTS['button'], bg=bg_color, fg=fg_color,
                          relief=tk.RAISED, borderwidth=2, width=25, pady=15,
                          cursor='hand2')
            btn.pack(pady=10)
            
            # Efecto hover (solo para botones que no sean "Salir")
            if text != "Salir":
                btn.bind('<Enter>', lambda e, b=btn: b.config(bg=COLORS['button_hover']))
                btn.bind('<Leave>', lambda e, b=btn: b.config(bg=COLORS['button_bg']))
    
    def open_punto_venta(self):
        """Abre el módulo de punto de venta"""
        self.root.withdraw()
        from punto_venta import PuntoVentaWindow
        PuntoVentaWindow(self.root, on_close=self.on_module_close)
    
    def open_productos(self):
        """Abre el módulo de productos"""
        self.root.withdraw()  # Ocultar menú
        from productos import ProductosWindow
        ProductosWindow(self.root, on_close=self.on_module_close)
    
    def open_ingredientes(self):
        """Abre el módulo de ingredientes"""
        self.root.withdraw()
        from ingredientes import IngredientesWindow
        IngredientesWindow(self.root, on_close=self.on_module_close)
    
    def open_recetas(self):
        """Abre el módulo de recetas"""
        self.root.withdraw()
        from recetas import RecetasWindow
        RecetasWindow(self.root, on_close=self.on_module_close)
    
    def open_stock(self):
        """Abre el módulo de stock"""
        self.root.withdraw()
        from stock import StockWindow
        StockWindow(self.root, on_close=self.on_module_close)
    
    def open_historial(self):
        """Abre el módulo de historial de ventas"""
        messagebox.showinfo("Próximamente", "Módulo de Historial de Ventas en PARTE 3")
    
    def open_cortes(self):
        """Abre el módulo de cortes"""
        messagebox.showinfo("Próximamente", "Módulo de Cortes en PARTE 3")
    
    def on_module_close(self):
        """Callback cuando se cierra un módulo - vuelve a mostrar el menú"""
        self.show_main_menu()
    
    def salir(self):
        """Cierra el programa"""
        if messagebox.askyesno("Salir", "¿Estás seguro de que deseas salir del sistema?"):
            self.root.quit()
            self.root.destroy()
    
    def run(self):
        """Ejecuta la aplicación"""
        self.root.mainloop()


class DineroCajaWindow:
    def __init__(self, parent, callback=None):
        self.callback = callback
        self.denominaciones_cantidad = {}
        
        self.window = tk.Toplevel(parent)
        self.window.title("Ingresa el dinero en caja")
        self.window.configure(bg=COLORS['bg_primary'])
        self.window.transient(parent)
        self.window.grab_set()
        
        # Forzar al frente
        self.window.lift()
        self.window.attributes('-topmost', True)
        self.window.after(100, lambda: self.window.attributes('-topmost', False))
        
        # Centrar ventana (MÁS ANCHA)
        self.setup_ui()
        self.window.update_idletasks()
        width = 600  # Aumentado de 500 a 600
        height = 650
        x = (self.window.winfo_screenwidth() // 2) - (width // 2)
        y = (self.window.winfo_screenheight() // 2) - (height // 2)
        self.window.geometry(f"{width}x{height}+{x}+{y}")
    
    def setup_ui(self):
        """Configura la interfaz"""
        main_frame = tk.Frame(self.window, bg=COLORS['bg_primary'])
        main_frame.pack(fill=tk.BOTH, expand=True, padx=30, pady=30)
        
        # Título
        tk.Label(main_frame, text="Ingresa el dinero en caja", 
                font=FONTS['title'], bg=COLORS['bg_primary'],
                fg=COLORS['text_primary']).pack(pady=(0, 30))
        
        # Frame scrollable
        canvas = tk.Canvas(main_frame, bg=COLORS['bg_primary'], 
                          highlightthickness=0, height=400)
        scrollbar = tk.Scrollbar(main_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = tk.Frame(canvas, bg=COLORS['bg_primary'])
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # Frame para billetes
        billetes_frame = tk.LabelFrame(scrollable_frame, text="Billetes", 
                                       font=FONTS['heading'],
                                       bg=COLORS['bg_secondary'],
                                       fg=COLORS['text_primary'],
                                       relief=tk.RAISED, borderwidth=2)
        billetes_frame.pack(fill=tk.X, pady=(0, 20), padx=10)
        
        for denominacion in DENOMINACIONES['billetes']:
            self.create_denominacion_row(billetes_frame, denominacion, 'billete')
        
        # Frame para monedas
        monedas_frame = tk.LabelFrame(scrollable_frame, text="Monedas", 
                                      font=FONTS['heading'],
                                      bg=COLORS['bg_secondary'],
                                      fg=COLORS['text_primary'],
                                      relief=tk.RAISED, borderwidth=2)
        monedas_frame.pack(fill=tk.X, pady=(0, 20), padx=10)
        
        for denominacion in DENOMINACIONES['monedas']:
            self.create_denominacion_row(monedas_frame, denominacion, 'moneda')
        
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Total calculado
        self.total_var = tk.StringVar(value="$0.00")
        total_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        total_frame.pack(fill=tk.X, pady=(10, 20))
        
        tk.Label(total_frame, text="Dinero en caja:", font=FONTS['heading'],
                bg=COLORS['bg_primary']).pack(side=tk.LEFT, padx=(0, 10))
        
        tk.Label(total_frame, textvariable=self.total_var, font=FONTS['heading'],
                bg=COLORS['bg_primary'], fg=COLORS['accent']).pack(side=tk.LEFT)
        
        # Botón aceptar
        tk.Button(main_frame, text="Aceptar", command=self.accept,
                 font=FONTS['button'], bg=COLORS['success'], fg='white',
                 relief=tk.RAISED, borderwidth=2, padx=40, pady=15,
                 cursor='hand2').pack(pady=20)
    
    def create_denominacion_row(self, parent, denominacion, tipo):
        """Crea una fila para ingresar cantidad de una denominación"""
        row_frame = tk.Frame(parent, bg=COLORS['bg_secondary'])
        row_frame.pack(fill=tk.X, padx=15, pady=5)
        
        # Etiqueta de denominación
        from utils import format_currency
        tk.Label(row_frame, text=format_currency(denominacion), 
                font=FONTS['normal'], bg=COLORS['bg_secondary'],
                width=15, anchor='w').pack(side=tk.LEFT, padx=5)
        
        # Entry para cantidad
        cantidad_var = tk.StringVar(value="0")
        cantidad_var.trace('w', lambda *args: self.calculate_total())
        
        entry = tk.Entry(row_frame, textvariable=cantidad_var, 
                        font=FONTS['normal'], width=10, justify='center')
        entry.pack(side=tk.LEFT, padx=5)
        
        # Guardar referencia
        key = f"{tipo}_{denominacion}"
        self.denominaciones_cantidad[key] = {
            'var': cantidad_var,
            'denominacion': denominacion,
            'tipo': tipo
        }
    
    def calculate_total(self):
        """Calcula el total de dinero ingresado"""
        total = 0
        
        for key, data in self.denominaciones_cantidad.items():
            try:
                cantidad = int(data['var'].get())
                if cantidad > 0:
                    total += cantidad * data['denominacion']
            except ValueError:
                pass
        
        from utils import format_currency
        self.total_var.set(format_currency(total))
    
    def accept(self):
        """Acepta y guarda el dinero en caja"""
        total = 0
        
        # Calcular total
        for key, data in self.denominaciones_cantidad.items():
            try:
                cantidad = int(data['var'].get())
                if cantidad >= 0:
                    total += cantidad * data['denominacion']
                else:
                    messagebox.showerror("Error", 
                                       "Las cantidades no pueden ser negativas")
                    return
            except ValueError:
                messagebox.showerror("Error", 
                                   "Todas las cantidades deben ser números enteros válidos")
                return
        
        if total == 0:
            if not messagebox.askyesno("Confirmar", 
                                      "El total es $0.00. ¿Deseas continuar?"):
                return
        
        try:
            # Guardar en base de datos
            fecha = get_current_date()
            
            for key, data in self.denominaciones_cantidad.items():
                cantidad = int(data['var'].get())
                if cantidad > 0:
                    db.cursor.execute('''
                        INSERT INTO dinero_caja 
                        (fecha, tipo, denominacion, cantidad, total, tipo_registro)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (fecha, data['tipo'], data['denominacion'], cantidad,
                          cantidad * data['denominacion'], 'apertura'))
            
            db.conn.commit()
            
            # Marcar como ingresado hoy
            db.mark_dinero_ingresado()
            
            # Guardar el total como configuración
            db.set_config('dinero_inicial_dia', str(total))
            
            from utils import format_currency
            messagebox.showinfo("Éxito", 
                              f"Dinero en caja registrado: {format_currency(total)}")
            
            self.window.destroy()
            
            if self.callback:
                self.callback()
            
        except Exception as e:
            messagebox.showerror("Error", f"Error al guardar: {e}")


if __name__ == "__main__":
    app = MitsysPOS()
    app.run()