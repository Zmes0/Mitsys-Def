"""
Módulo de gestión de stock
"""
import tkinter as tk
from tkinter import ttk, messagebox
from config import COLORS, FONTS
from utils import format_currency
from database import db

class StockWindow:
    def __init__(self, parent, on_close=None):
        self.on_close_callback = on_close
        
        self.window = tk.Toplevel(parent)
        self.window.title("Gestión de Stock - Mitsy's POS")
        self.window.geometry("1300x700")
        self.window.configure(bg=COLORS['bg_primary'])
        
        # Centrar ventana
        self.center_window()
        
        # Forzar al frente
        self.window.lift()
        self.window.attributes('-topmost', True)
        self.window.after(100, lambda: self.window.attributes('-topmost', False))
        
        # Protocolo de cierre
        self.window.protocol("WM_DELETE_WINDOW", self.close_window)
        
        self.setup_ui()
        self.load_stock()
    
    def center_window(self):
        """Centra la ventana en la pantalla"""
        self.window.update_idletasks()
        width = 1300
        height = 700
        x = (self.window.winfo_screenwidth() // 2) - (width // 2)
        y = (self.window.winfo_screenheight() // 2) - (height // 2)
        self.window.geometry(f"{width}x{height}+{x}+{y}")
    
    def setup_ui(self):
        """Configura la interfaz de usuario"""
        main_frame = tk.Frame(self.window, bg=COLORS['bg_primary'])
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Frame superior con título y switch maestro
        top_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        top_frame.pack(fill=tk.X, pady=(0, 20))
        
        # Título
        title_label = tk.Label(top_frame, text="Gestión de Stock", 
                              font=FONTS['title'], bg=COLORS['bg_primary'],
                              fg=COLORS['text_primary'])
        title_label.pack(side=tk.LEFT)
        
        # Switch maestro de gestión de stock
        switch_frame = tk.Frame(top_frame, bg=COLORS['bg_secondary'],
                               relief=tk.RAISED, borderwidth=2)
        switch_frame.pack(side=tk.RIGHT, padx=20)
        
        tk.Label(switch_frame, text="Gestión Stock Global:", 
                font=FONTS['heading'], bg=COLORS['bg_secondary']).pack(side=tk.LEFT, padx=10, pady=10)
        
        self.gestion_global_var = tk.BooleanVar(value=db.is_gestion_stock_active())
        self.gestion_global_var.trace('w', self.toggle_gestion_global)
        
        tk.Radiobutton(switch_frame, text="Sí", variable=self.gestion_global_var,
                      value=True, font=FONTS['normal'], bg=COLORS['bg_secondary']).pack(side=tk.LEFT, padx=5)
        tk.Radiobutton(switch_frame, text="No", variable=self.gestion_global_var,
                      value=False, font=FONTS['normal'], bg=COLORS['bg_secondary']).pack(side=tk.LEFT, padx=5)
        
        # Frame de búsqueda
        search_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        search_frame.pack(fill=tk.X, pady=(0, 20))
        
        tk.Label(search_frame, text="Buscar:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(side=tk.LEFT, padx=(0, 10))
        
        self.search_var = tk.StringVar()
        self.search_var.trace('w', lambda *args: self.search_stock())
        search_entry = tk.Entry(search_frame, textvariable=self.search_var,
                               font=FONTS['normal'], width=40)
        search_entry.pack(side=tk.LEFT, padx=(0, 20))
        
        tk.Button(search_frame, text="Limpiar Filtro", command=self.clear_filter,
                 font=FONTS['button'], bg=COLORS['button_bg'],
                 relief=tk.RAISED, borderwidth=2, padx=15, pady=5).pack(side=tk.LEFT)
        
        # Frame con scrollbar para la tabla
        table_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        table_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 20))
        
        scrollbar = ttk.Scrollbar(table_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Treeview (tabla)
        columns = ('ID', 'Producto', 'Precio', 'Costo', 'Ganancia', 
                   'U. Medida', 'Stock Estimado', 'Stock Mínimo', 'Gestión Stock')
        
        self.tree = ttk.Treeview(table_frame, columns=columns, show='headings',
                                yscrollcommand=scrollbar.set, selectmode='extended')
        
        # Configurar columnas
        self.tree.heading('ID', text='ID')
        self.tree.heading('Producto', text='Producto')
        self.tree.heading('Precio', text='Precio Unitario')
        self.tree.heading('Costo', text='Costo')
        self.tree.heading('Ganancia', text='Ganancia')
        self.tree.heading('U. Medida', text='U. Medida')
        self.tree.heading('Stock Estimado', text='Stock Estimado')
        self.tree.heading('Stock Mínimo', text='Stock Mínimo')
        self.tree.heading('Gestión Stock', text='Gestión Stock')
        
        self.tree.column('ID', width=60, anchor='center')
        self.tree.column('Producto', width=180)
        self.tree.column('Precio', width=120, anchor='e')
        self.tree.column('Costo', width=120, anchor='e')
        self.tree.column('Ganancia', width=120, anchor='e')
        self.tree.column('U. Medida', width=100, anchor='center')
        self.tree.column('Stock Estimado', width=130, anchor='e')
        self.tree.column('Stock Mínimo', width=130, anchor='e')
        self.tree.column('Gestión Stock', width=130, anchor='center')
        
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.tree.yview)
        
        # Colores alternados y alerta de stock bajo
        self.tree.tag_configure('evenrow', background=COLORS['table_row_even'])
        self.tree.tag_configure('oddrow', background=COLORS['table_row_odd'])
        self.tree.tag_configure('low_stock', background='#FFCCCC')
        
        # Frame de botones (SIN Importar/Exportar Excel)
        button_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        button_frame.pack(fill=tk.X)
        
        buttons = [
            ("Regresar", self.close_window),
            ("Modificar", self.modificar_stock),
            ("Borrar", self.borrar_producto),
            ("Registrar Compra", self.registrar_compra),
            ("Añadir", self.add_producto)
        ]
        
        for text, command in buttons:
            btn = tk.Button(button_frame, text=text, command=command,
                          font=FONTS['button'], bg=COLORS['button_bg'],
                          fg=COLORS['text_primary'], relief=tk.RAISED,
                          borderwidth=2, padx=20, pady=10)
            btn.pack(side=tk.LEFT, padx=5)
    
    def toggle_gestion_global(self, *args):
        """Activa/desactiva la gestión de stock global"""
        activo = self.gestion_global_var.get()
        db.toggle_gestion_stock(activo)
        
        if activo:
            db.actualizar_todos_stocks_estimados()
            messagebox.showinfo("Gestión de Stock", 
                              "Gestión de stock activada. Los stocks estimados se calculan automáticamente.")
        else:
            messagebox.showinfo("Gestión de Stock", 
                              "Gestión de stock desactivada. El sistema no gestionará inventarios.")
        
        self.load_stock()
    
    def load_stock(self):
        """Carga el stock en la tabla"""
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        productos = db.get_productos()
        gestion_activa = db.is_gestion_stock_active()
        
        for idx, p in enumerate(productos):
            # Determinar el tag
            if gestion_activa and p['gestion_stock'] and p['stock_estimado'] < p['stock_minimo']:
                tag = 'low_stock'
            else:
                tag = 'evenrow' if idx % 2 == 0 else 'oddrow'
            
            values = (
                p['id'],
                p['nombre'],
                format_currency(p['precio_unitario']),
                format_currency(p['costo']),
                format_currency(p['ganancia']),
                p['unidad_medida'],
                f"{p['stock_estimado']:.2f}" if gestion_activa else "N/A",
                f"{p['stock_minimo']:.2f}",
                'Sí' if p['gestion_stock'] else 'No'
            )
            
            self.tree.insert('', tk.END, values=values, tags=(tag,))
    
    def search_stock(self):
        """Busca productos según el texto ingresado"""
        from utils import normalize_text
        query = normalize_text(self.search_var.get())
        
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        if not query:
            self.load_stock()
            return
        
        productos = db.search_productos(query)
        gestion_activa = db.is_gestion_stock_active()
        
        for idx, p in enumerate(productos):
            if gestion_activa and p['gestion_stock'] and p['stock_estimado'] < p['stock_minimo']:
                tag = 'low_stock'
            else:
                tag = 'evenrow' if idx % 2 == 0 else 'oddrow'
            
            values = (
                p['id'],
                p['nombre'],
                format_currency(p['precio_unitario']),
                format_currency(p['costo']),
                format_currency(p['ganancia']),
                p['unidad_medida'],
                f"{p['stock_estimado']:.2f}" if gestion_activa else "N/A",
                f"{p['stock_minimo']:.2f}",
                'Sí' if p['gestion_stock'] else 'No'
            )
            
            self.tree.insert('', tk.END, values=values, tags=(tag,))
    
    def clear_filter(self):
        """Limpia los filtros de búsqueda"""
        self.search_var.set("")
        self.load_stock()
    
    def modificar_stock(self):
        """Abre diálogo para modificar stock"""
        selection = self.tree.selection()
        
        if not selection:
            messagebox.showwarning("Advertencia", 
                                  "Por favor selecciona un producto para modificar")
            return
        
        if len(selection) > 1:
            messagebox.showwarning("Advertencia", 
                                  "Por favor selecciona solo un producto para modificar")
            return
        
        item = self.tree.item(selection[0])
        producto_id = item['values'][0]
        
        StockDialog(self.window, producto_id=producto_id, callback=self.load_stock)
    
    def borrar_producto(self):
        """Elimina productos seleccionados"""
        selection = self.tree.selection()
        
        if not selection:
            messagebox.showwarning("Advertencia", 
                                  "Por favor selecciona al menos un producto para borrar")
            return
        
        if not messagebox.askyesno("Confirmar", 
                                   f"¿Estás seguro de borrar {len(selection)} producto(s)?"):
            return
        
        for item in selection:
            producto_id = self.tree.item(item)['values'][0]
            db.delete_producto(producto_id)
        
        messagebox.showinfo("Éxito", "Producto(s) eliminado(s) correctamente")
        self.load_stock()
    
    def registrar_compra(self):
        """Registra una compra de producto (suma al stock estimado)"""
        selection = self.tree.selection()
        
        if not selection:
            messagebox.showwarning("Advertencia", 
                                  "Por favor selecciona un producto para registrar compra")
            return
        
        if len(selection) > 1:
            messagebox.showwarning("Advertencia", 
                                  "Por favor selecciona solo un producto")
            return
        
        item = self.tree.item(selection[0])
        producto_id = item['values'][0]
        producto_nombre = item['values'][1]
        
        RegistrarCompraProductoDialog(self.window, producto_id, producto_nombre,
                                     callback=self.load_stock)
    
    def add_producto(self):
        """Abre diálogo para añadir producto desde stock"""
        from productos import ProductoDialog
        ProductoDialog(self.window, callback=self.load_stock)
    
    def close_window(self):
        """Cierra la ventana y vuelve al menú"""
        self.window.destroy()
        if self.on_close_callback:
            self.on_close_callback()


class StockDialog:
    def __init__(self, parent, producto_id, callback=None):
        self.producto_id = producto_id
        self.callback = callback
        
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("Modificar Stock")
        self.dialog.geometry("450x400")
        self.dialog.configure(bg=COLORS['bg_primary'])
        self.dialog.transient(parent)
        self.dialog.grab_set()
        
        # Forzar al frente
        self.dialog.lift()
        self.dialog.attributes('-topmost', True)
        self.dialog.after(100, lambda: self.dialog.attributes('-topmost', False))
        
        # Centrar ventana
        self.center_dialog()
        
        self.setup_ui()
        self.load_producto_data()
    
    def center_dialog(self):
        """Centra el diálogo en la pantalla"""
        self.dialog.update_idletasks()
        width = 450
        height = 400
        x = (self.dialog.winfo_screenwidth() // 2) - (width // 2)
        y = (self.dialog.winfo_screenheight() // 2) - (height // 2)
        self.dialog.geometry(f"{width}x{height}+{x}+{y}")
    
    def setup_ui(self):
        """Configura la interfaz del diálogo"""
        main_frame = tk.Frame(self.dialog, bg=COLORS['bg_primary'])
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # ID
        tk.Label(main_frame, text=f"ID: {self.producto_id}", 
                font=FONTS['normal'], bg=COLORS['bg_primary']).pack(anchor='w', pady=5)
        
        # Nombre (solo lectura)
        tk.Label(main_frame, text="Producto:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=(10, 5))
        self.nombre_label = tk.Label(main_frame, text="", font=FONTS['heading'],
                                     bg=COLORS['bg_secondary'], relief=tk.SUNKEN,
                                     anchor='w', padx=10, pady=5)
        self.nombre_label.pack(fill=tk.X, pady=(0, 10))
        
        # Stock Estimado (solo lectura si gestión activa)
        tk.Label(main_frame, text="Stock Estimado:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=5)
        self.stock_estimado_var = tk.StringVar()
        self.stock_estimado_entry = tk.Entry(main_frame, 
                                            textvariable=self.stock_estimado_var,
                                            font=FONTS['normal'])
        self.stock_estimado_entry.pack(fill=tk.X, pady=(0, 10))
        
        # Stock Mínimo
        tk.Label(main_frame, text="Stock Mínimo:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=5)
        self.stock_minimo_var = tk.StringVar()
        tk.Entry(main_frame, textvariable=self.stock_minimo_var, 
                font=FONTS['normal']).pack(fill=tk.X, pady=(0, 10))
        
        # Gestión de Stock
        tk.Label(main_frame, text="Gestión de Stock:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=5)
        
        gestion_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        gestion_frame.pack(anchor='w', pady=(0, 20))
        
        self.gestion_var = tk.BooleanVar()
        
        tk.Radiobutton(gestion_frame, text="Sí", variable=self.gestion_var,
                      value=True, font=FONTS['normal'],
                      bg=COLORS['bg_primary']).pack(side=tk.LEFT, padx=10)
        tk.Radiobutton(gestion_frame, text="No", variable=self.gestion_var,
                      value=False, font=FONTS['normal'],
                      bg=COLORS['bg_primary']).pack(side=tk.LEFT, padx=10)
        
        # Botones
        button_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        button_frame.pack(pady=20)
        
        tk.Button(button_frame, text="Aceptar", command=self.save_stock,
                 font=FONTS['button'], bg=COLORS['success'], fg='white',
                 relief=tk.RAISED, borderwidth=2, padx=30, pady=10).pack(side=tk.LEFT, padx=10)
        
        tk.Button(button_frame, text="Cancelar", command=self.dialog.destroy,
                 font=FONTS['button'], bg=COLORS['danger'], fg='white',
                 relief=tk.RAISED, borderwidth=2, padx=30, pady=10).pack(side=tk.LEFT, padx=10)
    
    def load_producto_data(self):
        """Carga los datos del producto"""
        producto = db.get_producto(self.producto_id)
        
        if not producto:
            messagebox.showerror("Error", "Producto no encontrado")
            self.dialog.destroy()
            return
        
        self.nombre_label.config(text=producto['nombre'])
        self.stock_estimado_var.set(str(producto['stock_estimado']))
        self.stock_minimo_var.set(str(producto['stock_minimo']))
        self.gestion_var.set(bool(producto['gestion_stock']))
        
        # Si tiene recetas (gestión por ingredientes), el stock estimado es solo lectura
        if producto['gestion_stock']:
            recetas = db.get_recetas_producto(self.producto_id)
            if recetas:
                self.stock_estimado_entry.config(state='readonly')
    
    def save_stock(self):
        """Guarda los cambios de stock"""
        try:
            stock_estimado = float(self.stock_estimado_var.get())
            stock_minimo = float(self.stock_minimo_var.get())
        except ValueError:
            messagebox.showerror("Error", "Los valores de stock deben ser números válidos")
            return
        
        try:
            db.update_producto(self.producto_id, self.producto_id,
                             stock_estimado=stock_estimado,
                             stock_minimo=stock_minimo,
                             gestion_stock=1 if self.gestion_var.get() else 0)
            
            messagebox.showinfo("Éxito", "Stock actualizado correctamente")
            
            if self.callback:
                self.callback()
            
            self.dialog.destroy()
            
        except Exception as e:
            messagebox.showerror("Error", f"Error al actualizar stock: {str(e)}")


class RegistrarCompraProductoDialog:
    def __init__(self, parent, producto_id, producto_nombre, callback=None):
        self.producto_id = producto_id
        self.producto_nombre = producto_nombre
        self.callback = callback
        
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("Registrar Compra")
        self.dialog.geometry("400x250")
        self.dialog.configure(bg=COLORS['bg_primary'])
        self.dialog.transient(parent)
        self.dialog.grab_set()
        
        # Forzar al frente
        self.dialog.lift()
        self.dialog.attributes('-topmost', True)
        self.dialog.after(100, lambda: self.dialog.attributes('-topmost', False))
        
        # Centrar ventana
        self.center_dialog()
        
        self.setup_ui()
    
    def center_dialog(self):
        """Centra el diálogo en la pantalla"""
        self.dialog.update_idletasks()
        width = 400
        height = 250
        x = (self.dialog.winfo_screenwidth() // 2) - (width // 2)
        y = (self.dialog.winfo_screenheight() // 2) - (height // 2)
        self.dialog.geometry(f"{width}x{height}+{x}+{y}")
    
    def setup_ui(self):
        """Configura la interfaz del diálogo"""
        main_frame = tk.Frame(self.dialog, bg=COLORS['bg_primary'])
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Título
        tk.Label(main_frame, text="Registrar Compra", font=FONTS['heading'],
                bg=COLORS['bg_primary']).pack(pady=(0, 20))
        
        # Producto
        tk.Label(main_frame, text=f"Producto: {self.producto_nombre}", 
                font=FONTS['normal'], bg=COLORS['bg_primary']).pack(pady=10)
        
        # Cantidad comprada
        tk.Label(main_frame, text="Cantidad comprada:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=5)
        
        self.cantidad_var = tk.StringVar()
        tk.Entry(main_frame, textvariable=self.cantidad_var, font=FONTS['normal'],
                width=30).pack(fill=tk.X, pady=(0, 20))
        
        # Botones
        button_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        button_frame.pack()
        
        tk.Button(button_frame, text="Aceptar", command=self.registrar,
                 font=FONTS['button'], bg=COLORS['success'], fg='white',
                 relief=tk.RAISED, borderwidth=2, padx=30, pady=10).pack(side=tk.LEFT, padx=10)
        
        tk.Button(button_frame, text="Cancelar", command=self.dialog.destroy,
                 font=FONTS['button'], bg=COLORS['danger'], fg='white',
                 relief=tk.RAISED, borderwidth=2, padx=30, pady=10).pack(side=tk.LEFT, padx=10)
    
    def registrar(self):
        """Registra la compra"""
        try:
            cantidad = float(self.cantidad_var.get())
            
            if cantidad <= 0:
                messagebox.showerror("Error", "La cantidad debe ser mayor a 0")
                return
            
            # Sumar al stock estimado
            producto = db.get_producto(self.producto_id)
            nuevo_stock = producto['stock_estimado'] + cantidad
            
            db.update_producto(self.producto_id, self.producto_id, stock_estimado=nuevo_stock)
            
            messagebox.showinfo("Éxito", f"Se registró la compra de {cantidad} unidades")
            
            if self.callback:
                self.callback()
            
            self.dialog.destroy()
            
        except ValueError:
            messagebox.showerror("Error", "La cantidad debe ser un número válido")
        except Exception as e:
            messagebox.showerror("Error", f"Error al registrar compra: {str(e)}")