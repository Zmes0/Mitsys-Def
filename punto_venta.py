"""
Módulo de Punto de Venta para Mitsy's POS
"""
import tkinter as tk
from tkinter import ttk, messagebox
from PIL import Image, ImageTk
import os
from config import COLORS, FONTS, MESAS
from utils import format_currency, parse_currency
from database import db
from tickets import ticket_generator

class PuntoVentaWindow:
    def __init__(self, parent, on_close=None):
        self.on_close_callback = on_close
        
        self.window = tk.Toplevel(parent)
        self.window.title("Punto de Venta - Mitsy's POS")
        self.window.geometry("800x600")
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
    
    def center_window(self):
        """Centra la ventana en la pantalla"""
        self.window.update_idletasks()
        width = 800
        height = 600
        x = (self.window.winfo_screenwidth() // 2) - (width // 2)
        y = (self.window.winfo_screenheight() // 2) - (height // 2)
        self.window.geometry(f"{width}x{height}+{x}+{y}")
    
    def setup_ui(self):
        """Configura la interfaz de usuario"""
        main_frame = tk.Frame(self.window, bg=COLORS['bg_primary'])
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Título
        title_label = tk.Label(main_frame, text="Punto de Venta", 
                              font=FONTS['title'], bg=COLORS['bg_primary'],
                              fg=COLORS['text_primary'])
        title_label.pack(pady=(0, 30))
        
        # Frame para mesas (grid 3x3)
        mesas_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        mesas_frame.pack(expand=True)
        
        # Obtener mesas con ventas pendientes
        mesas_pendientes = db.get_mesas_con_ventas_pendientes()
        
        # Crear botones de mesas (3x3)
        row = 0
        col = 0
        for idx, mesa in enumerate(MESAS):
            # Determinar color según si tiene venta pendiente
            if mesa in mesas_pendientes:
                bg_color = COLORS['warning']  # Naranja para ventas pendientes
                fg_color = 'white'
            else:
                bg_color = COLORS['button_bg']
                fg_color = COLORS['text_primary']
            
            btn = tk.Button(mesas_frame, text=mesa, 
                          command=lambda m=mesa: self.open_mesa(m),
                          font=FONTS['button'], bg=bg_color, fg=fg_color,
                          relief=tk.RAISED, borderwidth=3,
                          width=15, height=3, cursor='hand2')
            btn.grid(row=row, column=col, padx=15, pady=15)
            
            col += 1
            if col > 2:  # 3 columnas
                col = 0
                row += 1
        
        # Frame de botones inferiores
        bottom_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        bottom_frame.pack(side=tk.BOTTOM, pady=(20, 0))
        
        buttons = [
            ("Finalizar Día", self.finalizar_dia),
            ("Volver", self.close_window)
        ]
        
        for text, command in buttons:
            bg_color = COLORS['accent'] if text == "Finalizar Día" else COLORS['button_bg']
            fg_color = 'white' if text == "Finalizar Día" else COLORS['text_primary']
            
            btn = tk.Button(bottom_frame, text=text, command=command,
                          font=FONTS['button'], bg=bg_color, fg=fg_color,
                          relief=tk.RAISED, borderwidth=2, padx=30, pady=10)
            btn.pack(side=tk.LEFT, padx=10)
    
    def open_mesa(self, mesa):
        """Abre la ventana de venta para una mesa"""
        VentaMesaWindow(self.window, mesa, callback=self.refresh_mesas)
    
    def refresh_mesas(self):
        """Refresca la ventana para actualizar indicadores de mesas pendientes"""
        # Limpiar ventana
        for widget in self.window.winfo_children():
            widget.destroy()
        
        # Recrear UI
        self.setup_ui()
    
    def finalizar_dia(self):
        """Abre ventana para finalizar el día (corte de caja)"""
        FinalizarDiaWindow(self.window, callback=self.close_window)
    
    def close_window(self):
        """Cierra la ventana y vuelve al menú"""
        self.window.destroy()
        if self.on_close_callback:
            self.on_close_callback()


class VentaMesaWindow:
    def __init__(self, parent, mesa, callback=None):
        self.mesa = mesa
        self.callback = callback
        self.productos_venta = []  # Lista de productos en la venta actual
        
        self.window = tk.Toplevel(parent)
        self.window.title(f"{mesa} - Mitsy's POS")
        self.window.geometry("1000x700")
        self.window.configure(bg=COLORS['bg_primary'])
        
        # Centrar ventana
        self.center_window()
        
        # Forzar al frente
        self.window.lift()
        self.window.attributes('-topmost', True)
        self.window.after(100, lambda: self.window.attributes('-topmost', False))
        
        # Protocolo de cierre
        self.window.protocol("WM_DELETE_WINDOW", self.minimizar_ventana)
        
        # Cargar venta pendiente si existe
        self.load_venta_pendiente()
        
        self.setup_ui()
        self.update_table()
    
    def center_window(self):
        """Centra la ventana en la pantalla"""
        self.window.update_idletasks()
        width = 1000
        height = 700
        x = (self.window.winfo_screenwidth() // 2) - (width // 2)
        y = (self.window.winfo_screenheight() // 2) - (height // 2)
        self.window.geometry(f"{width}x{height}+{x}+{y}")
    
    def load_venta_pendiente(self):
        """Carga una venta pendiente si existe"""
        venta_pendiente = db.get_venta_pendiente(self.mesa)
        if venta_pendiente:
            self.productos_venta = venta_pendiente['productos']
    
    def setup_ui(self):
        """Configura la interfaz de usuario"""
        main_frame = tk.Frame(self.window, bg=COLORS['bg_primary'])
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Título
        title_label = tk.Label(main_frame, text=self.mesa, 
                              font=FONTS['title'], bg=COLORS['bg_primary'],
                              fg=COLORS['text_primary'])
        title_label.pack(pady=(0, 20))
        
        # Frame con scrollbar para la tabla
        table_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        table_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 15))
        
        scrollbar = ttk.Scrollbar(table_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Treeview (tabla)
        columns = ('No.', 'Producto', 'Cantidad', 'Precio Unit.', 'Total', 'Stock Est.')
        
        self.tree = ttk.Treeview(table_frame, columns=columns, show='headings',
                                yscrollcommand=scrollbar.set, selectmode='extended')
        
        # Configurar columnas
        self.tree.heading('No.', text='No. Venta')
        self.tree.heading('Producto', text='Producto')
        self.tree.heading('Cantidad', text='Cantidad')
        self.tree.heading('Precio Unit.', text='Precio Unitario')
        self.tree.heading('Total', text='Total')
        self.tree.heading('Stock Est.', text='Stock Estimado')
        
        self.tree.column('No.', width=80, anchor='center')
        self.tree.column('Producto', width=200)
        self.tree.column('Cantidad', width=100, anchor='center')
        self.tree.column('Precio Unit.', width=120, anchor='e')
        self.tree.column('Total', width=120, anchor='e')
        self.tree.column('Stock Est.', width=120, anchor='center')
        
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.tree.yview)
        
        # Configurar colores y tags
        self.tree.tag_configure('evenrow', background=COLORS['table_row_even'])
        self.tree.tag_configure('oddrow', background=COLORS['table_row_odd'])
        self.tree.tag_configure('low_stock', background='#FFCCCC')
        
        # Permitir edición al hacer doble clic
        self.tree.bind('<Double-1>', self.edit_item)
        
        # Total de la venta
        total_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        total_frame.pack(fill=tk.X, pady=(0, 15))
        
        tk.Label(total_frame, text="Total de la Venta:", font=FONTS['heading'],
                bg=COLORS['bg_primary']).pack(side=tk.LEFT, padx=(0, 10))
        
        self.total_var = tk.StringVar(value="$0.00")
        tk.Label(total_frame, textvariable=self.total_var, font=FONTS['heading'],
                bg=COLORS['bg_primary'], fg=COLORS['accent']).pack(side=tk.LEFT)
        
        # Frame de botones
        button_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        button_frame.pack(fill=tk.X)
        
        buttons = [
            ("Agregar Productos", self.agregar_productos, COLORS['success']),
            ("Borrar Producto", self.borrar_producto, COLORS['danger']),
            ("Limpiar Venta", self.limpiar_venta, COLORS['warning']),
            ("Cobrar Venta", self.cobrar_venta, COLORS['accent']),
            ("Minimizar Ventana", self.minimizar_ventana, COLORS['button_bg'])
        ]
        
        for text, command, color in buttons:
            fg = 'white' if color != COLORS['button_bg'] else COLORS['text_primary']
            
            btn = tk.Button(button_frame, text=text, command=command,
                          font=FONTS['button'], bg=color, fg=fg,
                          relief=tk.RAISED, borderwidth=2, padx=15, pady=8)
            btn.pack(side=tk.LEFT, padx=5)
    
    def update_table(self):
        """Actualiza la tabla de productos"""
        # Limpiar tabla
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # Cargar productos
        total_venta = 0
        gestion_activa = db.is_gestion_stock_active()
        
        for idx, prod in enumerate(self.productos_venta):
            producto_db = db.get_producto(prod['id'])
            
            # Stock estimado
            if gestion_activa and producto_db and producto_db['gestion_stock']:
                stock_est = producto_db['stock_estimado']
                stock_min = producto_db.get('stock_minimo', 0)
                stock_text = f"{stock_est:.0f}"
                
                # Determinar tag por stock bajo
                if stock_est < stock_min:
                    tag = 'low_stock'
                else:
                    tag = 'evenrow' if idx % 2 == 0 else 'oddrow'
            else:
                stock_text = "N/A"
                tag = 'evenrow' if idx % 2 == 0 else 'oddrow'
            
            values = (
                idx + 1,
                prod['nombre'],
                f"{prod['cantidad']:.1f}",
                format_currency(prod['precio']),
                format_currency(prod['total']),
                stock_text
            )
            
            self.tree.insert('', tk.END, values=values, tags=(tag,))
            total_venta += prod['total']
        
        # Actualizar total
        self.total_var.set(format_currency(total_venta))
    
    def agregar_productos(self):
        """Abre ventana para agregar productos"""
        AgregarProductosWindow(self.window, callback=self.add_producto_to_venta)
    
    def add_producto_to_venta(self, producto_data):
        """Añade un producto a la venta"""
        # producto_data = {'id': 1, 'nombre': 'Tacos', 'precio': 15.00, 'cantidad': 2}
        
        # Verificar si el producto ya está en la venta
        producto_existente = None
        for prod in self.productos_venta:
            if prod['id'] == producto_data['id']:
                producto_existente = prod
                break
        
        if producto_existente:
            # Sumar cantidad
            producto_existente['cantidad'] += producto_data['cantidad']
            producto_existente['total'] = producto_existente['cantidad'] * producto_existente['precio']
        else:
            # Añadir nuevo
            total = producto_data['cantidad'] * producto_data['precio']
            self.productos_venta.append({
                'id': producto_data['id'],
                'nombre': producto_data['nombre'],
                'cantidad': producto_data['cantidad'],
                'precio': producto_data['precio'],
                'total': total
            })
        
        self.update_table()
    
    def edit_item(self, event):
        """Permite editar un item al hacer doble clic"""
        region = self.tree.identify_region(event.x, event.y)
        if region != "cell":
            return
        
        column = self.tree.identify_column(event.x)
        row_id = self.tree.identify_row(event.y)
        
        if not row_id:
            return
        
        # Solo permitir editar cantidad y precio unitario
        col_index = int(column.replace('#', '')) - 1
        
        if col_index not in [2, 3]:  # Columnas Cantidad y Precio Unit.
            return
        
        # Obtener índice del producto
        values = self.tree.item(row_id)['values']
        producto_idx = int(values[0]) - 1
        
        if producto_idx >= len(self.productos_venta):
            return
        
        producto = self.productos_venta[producto_idx]
        
        # Crear diálogo de edición
        if col_index == 2:  # Cantidad
            EditarCantidadDialog(self.window, producto, self.update_table)
        elif col_index == 3:  # Precio
            EditarPrecioDialog(self.window, producto, self.update_table)
    
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
        
        # Obtener índices a eliminar (de mayor a menor para no alterar índices)
        indices = []
        for item in selection:
            values = self.tree.item(item)['values']
            indices.append(int(values[0]) - 1)
        
        indices.sort(reverse=True)
        
        for idx in indices:
            if idx < len(self.productos_venta):
                del self.productos_venta[idx]
        
        self.update_table()
    
    def limpiar_venta(self):
        """Limpia toda la venta"""
        if not self.productos_venta:
            messagebox.showinfo("Información", "No hay productos en la venta")
            return
        
        if not messagebox.askyesno("Confirmar", 
                                   "¿Estás seguro de limpiar toda la venta?"):
            return
        
        self.productos_venta = []
        self.update_table()
    
    def cobrar_venta(self):
        """Abre ventana para cobrar la venta"""
        if not self.productos_venta:
            messagebox.showwarning("Advertencia", "No hay productos en la venta")
            return
        
        total = sum(p['total'] for p in self.productos_venta)
        
        CobrarVentaWindow(self.window, self.productos_venta, total, 
                         self.mesa, callback=self.on_venta_cobrada)
    
    def on_venta_cobrada(self):
        """Callback cuando se cobra la venta exitosamente"""
        # Limpiar venta
        self.productos_venta = []
        self.update_table()
        
        # Eliminar venta pendiente
        db.delete_venta_pendiente(self.mesa)
        
        # Cerrar ventana
        self.window.destroy()
        
        if self.callback:
            self.callback()
    
    def minimizar_ventana(self):
        """Minimiza la ventana y guarda la venta como pendiente"""
        if self.productos_venta:
            # Guardar venta pendiente
            total = sum(p['total'] for p in self.productos_venta)
            db.save_venta_pendiente(self.mesa, self.productos_venta, total)
        else:
            # Si no hay productos, eliminar venta pendiente
            db.delete_venta_pendiente(self.mesa)
        
        self.window.destroy()
        
        if self.callback:
            self.callback()
            
class AgregarProductosWindow:
    def __init__(self, parent, callback=None):
        self.callback = callback
        
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("Agregar Productos")
        self.dialog.geometry("1100x700")  # MÁS ANCHO para 4-5 productos por fila
        self.dialog.configure(bg=COLORS['bg_primary'])
        self.dialog.transient(parent)
        self.dialog.grab_set()
        
        # Forzar al frente
        self.dialog.lift()
        self.dialog.attributes('-topmost', True)
        self.dialog.after(100, lambda: self.dialog.attributes('-topmost', False))
        
        # Protocolo de cierre para limpiar eventos
        self.dialog.protocol("WM_DELETE_WINDOW", self.close_dialog)
        
        # Centrar ventana
        self.center_dialog()
        
        self.setup_ui()
        self.load_productos()
    
    def center_dialog(self):
        """Centra el diálogo en la pantalla"""
        self.dialog.update_idletasks()
        width = 1100  # AUMENTADO
        height = 700
        x = (self.dialog.winfo_screenwidth() // 2) - (width // 2)
        y = (self.dialog.winfo_screenheight() // 2) - (height // 2)
        self.dialog.geometry(f"{width}x{height}+{x}+{y}")
    
    def setup_ui(self):
        """Configura la interfaz de usuario"""
        main_frame = tk.Frame(self.dialog, bg=COLORS['bg_primary'])
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Barra de búsqueda
        search_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        search_frame.pack(fill=tk.X, pady=(0, 20))
        
        tk.Label(search_frame, text="Buscar:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(side=tk.LEFT, padx=(0, 10))
        
        self.search_var = tk.StringVar()
        self.search_var.trace('w', lambda *args: self.search_productos())
        search_entry = tk.Entry(search_frame, textvariable=self.search_var,
                               font=FONTS['normal'], width=40)
        search_entry.pack(side=tk.LEFT)
        
        # Frame con scrollbar para la galería
        canvas_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        canvas_frame.pack(fill=tk.BOTH, expand=True)
        
        # Canvas y scrollbar
        self.canvas = tk.Canvas(canvas_frame, bg=COLORS['bg_primary'], 
                               highlightthickness=0)
        scrollbar = tk.Scrollbar(canvas_frame, orient="vertical", 
                                command=self.canvas.yview)
        
        self.scrollable_frame = tk.Frame(self.canvas, bg=COLORS['bg_primary'])
        
        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        )
        
        self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=scrollbar.set)
        
        self.canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Bind scroll con mouse wheel solo a este canvas
        self.canvas.bind("<Enter>", self._bind_mousewheel)
        self.canvas.bind("<Leave>", self._unbind_mousewheel)
        
        # Botones inferiores
        button_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        button_frame.pack(fill=tk.X, pady=(20, 0))
        
        tk.Button(button_frame, text="Regresar", command=self.close_dialog,
                 font=FONTS['button'], bg=COLORS['button_bg'],
                 fg=COLORS['text_primary'], relief=tk.RAISED,
                 borderwidth=2, padx=30, pady=10).pack(side=tk.LEFT, padx=5)
        
        tk.Button(button_frame, text="Aceptar", command=self.close_dialog,
                 font=FONTS['button'], bg=COLORS['success'], fg='white',
                 relief=tk.RAISED, borderwidth=2, padx=30, pady=10).pack(side=tk.LEFT, padx=5)
    
    def _bind_mousewheel(self, event):
        """Vincula el scroll del mouse cuando entra al canvas"""
        self.canvas.bind_all("<MouseWheel>", self._on_mousewheel)
    
    def _unbind_mousewheel(self, event):
        """Desvincula el scroll del mouse cuando sale del canvas"""
        self.canvas.unbind_all("<MouseWheel>")
    
    def _on_mousewheel(self, event):
        """Maneja el scroll con la rueda del mouse"""
        try:
            self.canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        except:
            pass  # Si el canvas ya no existe, ignorar
    
    def load_productos(self):
        """Carga los productos en la galería"""
        # Limpiar frame
        for widget in self.scrollable_frame.winfo_children():
            widget.destroy()
        
        productos = db.get_productos()
        
        # Crear grid de productos (4 columnas para pantallas normales)
        row = 0
        col = 0
        
        for producto in productos:
            self.create_producto_card(producto, row, col)
            
            col += 1
            if col > 6:  # 4 COLUMNAS (0, 1, 2, 3)
                col = 0
                row += 1
    
    def create_producto_card(self, producto, row, col):
        """Crea una tarjeta de producto"""
        card = tk.Frame(self.scrollable_frame, bg=COLORS['bg_secondary'],
                       relief=tk.RAISED, borderwidth=2)
        card.grid(row=row, column=col, padx=12, pady=12, sticky='nsew')  # PADDING REDUCIDO
        
        # Imagen (MÁS PEQUEÑA)
        img_frame = tk.Frame(card, bg=COLORS['bg_secondary'], 
                            width=120, height=120)  # REDUCIDO de 150 a 120
        img_frame.pack(pady=8)
        img_frame.pack_propagate(False)
        
        try:
            if producto['imagen'] and os.path.exists(producto['imagen']):
                # Cargar imagen del producto
                img = Image.open(producto['imagen'])
                img = img.resize((110, 110), Image.Resampling.LANCZOS)
                photo = ImageTk.PhotoImage(img)
            else:
                # Crear placeholder
                photo = self.create_placeholder_image()
            
            img_label = tk.Label(img_frame, image=photo, bg=COLORS['bg_secondary'])
            img_label.image = photo  # Mantener referencia
            img_label.pack(expand=True)
        except:
            # Si falla, usar placeholder
            photo = self.create_placeholder_image()
            img_label = tk.Label(img_frame, image=photo, bg=COLORS['bg_secondary'])
            img_label.image = photo
            img_label.pack(expand=True)
        
        # Nombre
        nombre = producto['nombre']
        if len(nombre) > 18:
            nombre = nombre[:18] + "..."
        
        tk.Label(card, text=nombre, font=FONTS['normal'],
                bg=COLORS['bg_secondary'], wraplength=130).pack(pady=(0, 5))
        
        # Precio
        tk.Label(card, text=format_currency(producto['precio_unitario']),
                font=FONTS['normal'], bg=COLORS['bg_secondary'],
                fg=COLORS['accent']).pack(pady=(0, 8))
        
        # Botón seleccionar
        btn = tk.Button(card, text="Seleccionar", 
                       command=lambda p=producto: self.select_producto(p),
                       font=FONTS['normal'], bg=COLORS['accent'], fg='white',
                       relief=tk.RAISED, borderwidth=2, cursor='hand2')
        btn.pack(pady=(0, 8), padx=8, fill=tk.X)
        
        # Hacer toda la tarjeta clickeable
        card.bind('<Button-1>', lambda e, p=producto: self.select_producto(p))
        for child in card.winfo_children():
            if not isinstance(child, tk.Button):
                child.bind('<Button-1>', lambda e, p=producto: self.select_producto(p))
    
    def create_placeholder_image(self):
        """Crea una imagen placeholder"""
        img = Image.new('RGB', (110, 110), color=COLORS['table_header'])  # REDUCIDO
        
        # Dibujar un icono simple
        from PIL import ImageDraw
        draw = ImageDraw.Draw(img)
        
        # Dibujar rectángulo y texto
        draw.rectangle([15, 15, 95, 95], outline='gray', width=2)
        draw.text((35, 45), "Sin", fill='gray')
        draw.text((25, 60), "Imagen", fill='gray')
        
        return ImageTk.PhotoImage(img)
    
    def search_productos(self):
        """Busca productos según el texto ingresado"""
        query = self.search_var.get()
        
        # Limpiar frame
        for widget in self.scrollable_frame.winfo_children():
            widget.destroy()
        
        if not query:
            self.load_productos()
            return
        
        productos = db.search_productos(query)
        
        # Crear grid
        row = 0
        col = 0
        
        for producto in productos:
            self.create_producto_card(producto, row, col)
            
            col += 1
            if col > 3:  # 4 COLUMNAS
                col = 0
                row += 1
    
    def select_producto(self, producto):
        """Selecciona un producto y abre diálogo de cantidad"""
        CantidadProductoDialog(self.dialog, producto, callback=self.on_cantidad_confirmed)
    
    def on_cantidad_confirmed(self, producto_data):
        """Callback cuando se confirma la cantidad"""
        if self.callback:
            self.callback(producto_data)
    
    def close_dialog(self):
        """Cierra el diálogo y limpia eventos"""
        try:
            self.canvas.unbind_all("<MouseWheel>")
        except:
            pass
        self.dialog.destroy()

class CantidadProductoDialog:
    def __init__(self, parent, producto, callback=None):
        self.producto = producto
        self.callback = callback
        
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("Ingresa la cantidad")
        self.dialog.geometry("400x300")
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
        height = 300
        x = (self.dialog.winfo_screenwidth() // 2) - (width // 2)
        y = (self.dialog.winfo_screenheight() // 2) - (height // 2)
        self.dialog.geometry(f"{width}x{height}+{x}+{y}")
    
    def setup_ui(self):
        """Configura la interfaz del diálogo"""
        main_frame = tk.Frame(self.dialog, bg=COLORS['bg_primary'])
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Título
        tk.Label(main_frame, text="Ingresa la cantidad del producto", 
                font=FONTS['heading'], bg=COLORS['bg_primary']).pack(pady=(0, 20))
        
        # Producto
        tk.Label(main_frame, text=f"Producto: {self.producto['nombre']}", 
                font=FONTS['normal'], bg=COLORS['bg_primary']).pack(pady=10)
        
        tk.Label(main_frame, text=f"Precio: {format_currency(self.producto['precio_unitario'])}", 
                font=FONTS['normal'], bg=COLORS['bg_primary'],
                fg=COLORS['accent']).pack(pady=(0, 20))
        
        # Cantidad
        tk.Label(main_frame, text="Cantidad:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=5)
        
        self.cantidad_var = tk.StringVar(value="1")
        cantidad_entry = tk.Entry(main_frame, textvariable=self.cantidad_var, 
                                 font=FONTS['normal'], justify='center')
        cantidad_entry.pack(fill=tk.X, pady=(0, 20))
        cantidad_entry.focus()
        cantidad_entry.select_range(0, tk.END)
        
        # Bind Enter para aceptar
        cantidad_entry.bind('<Return>', lambda e: self.accept())
        
        # Botones
        button_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        button_frame.pack()
        
        tk.Button(button_frame, text="Aceptar", command=self.accept,
                 font=FONTS['button'], bg=COLORS['success'], fg='white',
                 relief=tk.RAISED, borderwidth=2, padx=30, pady=10).pack(side=tk.LEFT, padx=10)
        
        tk.Button(button_frame, text="Regresar", command=self.dialog.destroy,
                 font=FONTS['button'], bg=COLORS['danger'], fg='white',
                 relief=tk.RAISED, borderwidth=2, padx=30, pady=10).pack(side=tk.LEFT, padx=10)
    
    def accept(self):
        """Acepta y retorna la cantidad"""
        try:
            cantidad = float(self.cantidad_var.get())
            
            if cantidad <= 0:
                messagebox.showerror("Error", "La cantidad debe ser mayor a 0")
                return
            
            producto_data = {
                'id': self.producto['id'],
                'nombre': self.producto['nombre'],
                'precio': self.producto['precio_unitario'],
                'cantidad': cantidad
            }
            
            if self.callback:
                self.callback(producto_data)
            
            self.dialog.destroy()
            
        except ValueError:
            messagebox.showerror("Error", "La cantidad debe ser un número válido")


class EditarCantidadDialog:
    def __init__(self, parent, producto, callback=None):
        self.producto = producto
        self.callback = callback
        
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("Editar Cantidad")
        self.dialog.geometry("350x200")
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
        width = 350
        height = 200
        x = (self.dialog.winfo_screenwidth() // 2) - (width // 2)
        y = (self.dialog.winfo_screenheight() // 2) - (height // 2)
        self.dialog.geometry(f"{width}x{height}+{x}+{y}")
    
    def setup_ui(self):
        """Configura la interfaz del diálogo"""
        main_frame = tk.Frame(self.dialog, bg=COLORS['bg_primary'])
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        tk.Label(main_frame, text=f"Producto: {self.producto['nombre']}", 
                font=FONTS['normal'], bg=COLORS['bg_primary']).pack(pady=10)
        
        tk.Label(main_frame, text="Nueva Cantidad:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=5)
        
        self.cantidad_var = tk.StringVar(value=str(self.producto['cantidad']))
        cantidad_entry = tk.Entry(main_frame, textvariable=self.cantidad_var, 
                                 font=FONTS['normal'])
        cantidad_entry.pack(fill=tk.X, pady=(0, 20))
        cantidad_entry.focus()
        cantidad_entry.select_range(0, tk.END)
        
        cantidad_entry.bind('<Return>', lambda e: self.accept())
        
        button_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        button_frame.pack()
        
        tk.Button(button_frame, text="Aceptar", command=self.accept,
                 font=FONTS['button'], bg=COLORS['success'], fg='white',
                 relief=tk.RAISED, borderwidth=2, padx=20, pady=8).pack(side=tk.LEFT, padx=5)
        
        tk.Button(button_frame, text="Cancelar", command=self.dialog.destroy,
                 font=FONTS['button'], bg=COLORS['danger'], fg='white',
                 relief=tk.RAISED, borderwidth=2, padx=20, pady=8).pack(side=tk.LEFT, padx=5)
    
    def accept(self):
        """Acepta y actualiza la cantidad"""
        try:
            cantidad = float(self.cantidad_var.get())
            
            if cantidad <= 0:
                messagebox.showerror("Error", "La cantidad debe ser mayor a 0")
                return
            
            self.producto['cantidad'] = cantidad
            self.producto['total'] = cantidad * self.producto['precio']
            
            if self.callback:
                self.callback()
            
            self.dialog.destroy()
            
        except ValueError:
            messagebox.showerror("Error", "La cantidad debe ser un número válido")


class EditarPrecioDialog:
    def __init__(self, parent, producto, callback=None):
        self.producto = producto
        self.callback = callback
        
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("Editar Precio")
        self.dialog.geometry("350x200")
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
        width = 350
        height = 200
        x = (self.dialog.winfo_screenwidth() // 2) - (width // 2)
        y = (self.dialog.winfo_screenheight() // 2) - (height // 2)
        self.dialog.geometry(f"{width}x{height}+{x}+{y}")
    
    def setup_ui(self):
        """Configura la interfaz del diálogo"""
        main_frame = tk.Frame(self.dialog, bg=COLORS['bg_primary'])
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        tk.Label(main_frame, text=f"Producto: {self.producto['nombre']}", 
                font=FONTS['normal'], bg=COLORS['bg_primary']).pack(pady=10)
        
        tk.Label(main_frame, text="Nuevo Precio Unitario:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=5)
        
        self.precio_var = tk.StringVar(value=str(self.producto['precio']))
        precio_entry = tk.Entry(main_frame, textvariable=self.precio_var, 
                               font=FONTS['normal'])
        precio_entry.pack(fill=tk.X, pady=(0, 20))
        precio_entry.focus()
        precio_entry.select_range(0, tk.END)
        
        precio_entry.bind('<Return>', lambda e: self.accept())
        
        button_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        button_frame.pack()
        
        tk.Button(button_frame, text="Aceptar", command=self.accept,
                 font=FONTS['button'], bg=COLORS['success'], fg='white',
                 relief=tk.RAISED, borderwidth=2, padx=20, pady=8).pack(side=tk.LEFT, padx=5)
        
        tk.Button(button_frame, text="Cancelar", command=self.dialog.destroy,
                 font=FONTS['button'], bg=COLORS['danger'], fg='white',
                 relief=tk.RAISED, borderwidth=2, padx=20, pady=8).pack(side=tk.LEFT, padx=5)
    
    def accept(self):
        """Acepta y actualiza el precio"""
        try:
            precio = float(self.precio_var.get())
            
            if precio <= 0:
                messagebox.showerror("Error", "El precio debe ser mayor a 0")
                return
            
            self.producto['precio'] = precio
            self.producto['total'] = self.producto['cantidad'] * precio
            
            if self.callback:
                self.callback()
            
            self.dialog.destroy()
            
        except ValueError:
            messagebox.showerror("Error", "El precio debe ser un número válido")
            
class CobrarVentaWindow:
    def __init__(self, parent, productos, total, mesa, callback=None):
        self.productos = productos
        self.subtotal = total
        self.mesa = mesa
        self.callback = callback
        
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("Cobrar Venta")
        self.dialog.geometry("500x600")
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
        width = 500
        height = 600
        x = (self.dialog.winfo_screenwidth() // 2) - (width // 2)
        y = (self.dialog.winfo_screenheight() // 2) - (height // 2)
        self.dialog.geometry(f"{width}x{height}+{x}+{y}")
    
    def setup_ui(self):
        """Configura la interfaz del diálogo"""
        main_frame = tk.Frame(self.dialog, bg=COLORS['bg_primary'])
        main_frame.pack(fill=tk.BOTH, expand=True, padx=30, pady=30)
        
        # Título
        tk.Label(main_frame, text="Cobrar Venta", font=FONTS['title'],
                bg=COLORS['bg_primary'], fg=COLORS['text_primary']).pack(pady=(0, 20))
        
        # Subtotal
        subtotal_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        subtotal_frame.pack(fill=tk.X, pady=10)
        
        tk.Label(subtotal_frame, text="Subtotal:", font=FONTS['heading'],
                bg=COLORS['bg_primary']).pack(side=tk.LEFT)
        
        tk.Label(subtotal_frame, text=format_currency(self.subtotal), 
                font=FONTS['heading'], bg=COLORS['bg_primary'],
                fg=COLORS['text_primary']).pack(side=tk.RIGHT)
        
        # Propina
        propina_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        propina_frame.pack(fill=tk.X, pady=10)
        
        tk.Label(propina_frame, text="Propina:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(side=tk.LEFT)
        
        self.propina_var = tk.StringVar(value="0")
        self.propina_var.trace('w', lambda *args: self.calculate_total())
        propina_entry = tk.Entry(propina_frame, textvariable=self.propina_var,
                                font=FONTS['normal'], width=15, justify='right')
        propina_entry.pack(side=tk.RIGHT)
        
        # Total a pagar
        total_frame = tk.Frame(main_frame, bg=COLORS['bg_secondary'],
                              relief=tk.RAISED, borderwidth=2)
        total_frame.pack(fill=tk.X, pady=20, padx=10)
        
        tk.Label(total_frame, text="Total a pagar:", font=FONTS['heading'],
                bg=COLORS['bg_secondary']).pack(side=tk.LEFT, padx=10, pady=15)
        
        self.total_var = tk.StringVar(value=format_currency(self.subtotal))
        tk.Label(total_frame, textvariable=self.total_var, 
                font=('Segoe UI', 20, 'bold'), bg=COLORS['bg_secondary'],
                fg=COLORS['accent']).pack(side=tk.RIGHT, padx=10, pady=15)
        
        # Dinero recibido
        recibido_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        recibido_frame.pack(fill=tk.X, pady=10)
        
        tk.Label(recibido_frame, text="Dinero recibido:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(side=tk.LEFT)
        
        self.recibido_var = tk.StringVar(value="0")
        self.recibido_var.trace('w', lambda *args: self.calculate_cambio())
        recibido_entry = tk.Entry(recibido_frame, textvariable=self.recibido_var,
                                 font=FONTS['normal'], width=15, justify='right')
        recibido_entry.pack(side=tk.RIGHT)
        recibido_entry.focus()
        
        # Cambio
        cambio_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        cambio_frame.pack(fill=tk.X, pady=10)
        
        tk.Label(cambio_frame, text="Cambio:", font=FONTS['heading'],
                bg=COLORS['bg_primary']).pack(side=tk.LEFT)
        
        self.cambio_var = tk.StringVar(value="$0.00")
        tk.Label(cambio_frame, textvariable=self.cambio_var, 
                font=FONTS['heading'], bg=COLORS['bg_primary'],
                fg=COLORS['success']).pack(side=tk.RIGHT)
        
        # Separador
        tk.Frame(main_frame, bg=COLORS['border'], height=2).pack(fill=tk.X, pady=20)
        
        # Método de pago
        tk.Label(main_frame, text="Método de pago:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=10)
        
        metodo_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        metodo_frame.pack(anchor='w')
        
        self.metodo_var = tk.StringVar(value='Efectivo')
        
        tk.Radiobutton(metodo_frame, text="Efectivo", variable=self.metodo_var,
                      value='Efectivo', font=FONTS['normal'],
                      bg=COLORS['bg_primary']).pack(side=tk.LEFT, padx=10)
        tk.Radiobutton(metodo_frame, text="Transferencia", variable=self.metodo_var,
                      value='Transferencia', font=FONTS['normal'],
                      bg=COLORS['bg_primary']).pack(side=tk.LEFT, padx=10)
        
        # Botones
        button_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        button_frame.pack(pady=30)
        
        tk.Button(button_frame, text="Finalizar Venta", command=self.finalizar_venta,
                 font=FONTS['button'], bg=COLORS['success'], fg='white',
                 relief=tk.RAISED, borderwidth=2, padx=30, pady=12).pack(side=tk.LEFT, padx=10)
        
        tk.Button(button_frame, text="Cancelar", command=self.dialog.destroy,
                 font=FONTS['button'], bg=COLORS['danger'], fg='white',
                 relief=tk.RAISED, borderwidth=2, padx=30, pady=12).pack(side=tk.LEFT, padx=10)
    
    def calculate_total(self):
        """Calcula el total con propina"""
        try:
            propina = float(self.propina_var.get()) if self.propina_var.get() else 0
            total = self.subtotal + propina
            self.total_var.set(format_currency(total))
            self.calculate_cambio()
        except ValueError:
            pass
    
    def calculate_cambio(self):
        """Calcula el cambio"""
        try:
            propina = float(self.propina_var.get()) if self.propina_var.get() else 0
            total = self.subtotal + propina
            recibido = float(self.recibido_var.get()) if self.recibido_var.get() else 0
            cambio = recibido - total
            
            if cambio < 0:
                self.cambio_var.set(format_currency(0))
            else:
                self.cambio_var.set(format_currency(cambio))
        except ValueError:
            self.cambio_var.set("$0.00")
    
    def finalizar_venta(self):
        """Finaliza la venta"""
        try:
            propina = float(self.propina_var.get()) if self.propina_var.get() else 0
            total = self.subtotal + propina
            recibido = float(self.recibido_var.get()) if self.recibido_var.get() else 0
            
            # Validar que el dinero recibido sea suficiente
            if recibido < total:
                messagebox.showerror("Error", 
                                   f"El dinero recibido ({format_currency(recibido)}) es menor al total ({format_currency(total)})")
                return
            
            cambio = recibido - total
            metodo_pago = self.metodo_var.get()
            
            # Guardar venta en base de datos
            numero_venta = db.finalizar_venta(self.productos, metodo_pago, 
                                             self.mesa, propina)
            
            # Generar ticket
            venta_data = {
                'numero_venta': numero_venta,
                'fecha': db.get_current_datetime(),
                'productos': self.productos,
                'subtotal': self.subtotal,
                'propina': propina,
                'total': total,
                'recibido': recibido,
                'cambio': cambio,
                'metodo_pago': metodo_pago,
                'mesa': self.mesa
            }
            
            try:
                # Generar PDF
                ticket_path = ticket_generator.generate_ticket_pdf(venta_data)
                
                # Preguntar si desea imprimir
                if messagebox.askyesno("Ticket Generado", 
                                      f"Ticket guardado en:\n{ticket_path}\n\n¿Desea imprimir el ticket?"):
                    ticket_generator.print_ticket(ticket_path)
                
            except Exception as e:
                messagebox.showerror("Error", f"Error al generar ticket: {str(e)}")
            
            # Mostrar resumen
            messagebox.showinfo("Venta Completada", 
                              f"Venta #{numero_venta} completada exitosamente\n\n"
                              f"Total: {format_currency(total)}\n"
                              f"Recibido: {format_currency(recibido)}\n"
                              f"Cambio: {format_currency(cambio)}")
            
            self.dialog.destroy()
            
            if self.callback:
                self.callback()
            
        except ValueError:
            messagebox.showerror("Error", "Valores inválidos. Verifica propina y dinero recibido.")
        except Exception as e:
            messagebox.showerror("Error", f"Error al finalizar venta: {str(e)}")


class FinalizarDiaWindow:
    def __init__(self, parent, callback=None):
        self.callback = callback
        
        # Verificar si hay ventas pendientes
        mesas_pendientes = db.get_mesas_con_ventas_pendientes()
        if mesas_pendientes:
            if not messagebox.askyesno("Ventas Pendientes", 
                                      f"Hay {len(mesas_pendientes)} mesa(s) con ventas pendientes:\n"
                                      f"{', '.join(mesas_pendientes)}\n\n"
                                      f"¿Deseas continuar con el corte de caja de todos modos?"):
                return
        
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("Finalizar Día - Corte de Caja")
        self.dialog.geometry("600x700")
        self.dialog.configure(bg=COLORS['bg_primary'])
        self.dialog.transient(parent)
        self.dialog.grab_set()
        
        # Forzar al frente
        self.dialog.lift()
        self.dialog.attributes('-topmost', True)
        self.dialog.after(100, lambda: self.dialog.attributes('-topmost', False))
        
        # Centrar ventana
        self.center_dialog()
        
        self.denominaciones_cantidad = {}
        
        self.setup_ui()
    
    def center_dialog(self):
        """Centra el diálogo en la pantalla"""
        self.dialog.update_idletasks()
        width = 600
        height = 700
        x = (self.dialog.winfo_screenwidth() // 2) - (width // 2)
        y = (self.dialog.winfo_screenheight() // 2) - (height // 2)
        self.dialog.geometry(f"{width}x{height}+{x}+{y}")
    
    def setup_ui(self):
        """Configura la interfaz del diálogo"""
        main_frame = tk.Frame(self.dialog, bg=COLORS['bg_primary'])
        main_frame.pack(fill=tk.BOTH, expand=True, padx=30, pady=30)
        
        # Título
        tk.Label(main_frame, text="Finalizar Día - Corte de Caja", 
                font=FONTS['title'], bg=COLORS['bg_primary'],
                fg=COLORS['text_primary']).pack(pady=(0, 20))
        
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
        from config import DENOMINACIONES
        
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
        
        # Egresos/Retiros
        egresos_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        egresos_frame.pack(fill=tk.X, pady=10)
        
        tk.Label(egresos_frame, text="Egresos/Retiros del día:", 
                font=FONTS['normal'], bg=COLORS['bg_primary']).pack(side=tk.LEFT, padx=(0, 10))
        
        self.egresos_var = tk.StringVar(value="0")
        tk.Entry(egresos_frame, textvariable=self.egresos_var, 
                font=FONTS['normal'], width=15, justify='right').pack(side=tk.LEFT)
        
        # Total contado
        total_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        total_frame.pack(fill=tk.X, pady=10)
        
        tk.Label(total_frame, text="Corte Final (contado):", font=FONTS['heading'],
                bg=COLORS['bg_primary']).pack(side=tk.LEFT, padx=(0, 10))
        
        self.total_var = tk.StringVar(value="$0.00")
        tk.Label(total_frame, textvariable=self.total_var, font=FONTS['heading'],
                bg=COLORS['bg_primary'], fg=COLORS['accent']).pack(side=tk.LEFT)
        
        # Botones
        button_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        button_frame.pack(pady=20)
        
        tk.Button(button_frame, text="Finalizar Día", command=self.finalizar_dia,
                 font=FONTS['button'], bg=COLORS['accent'], fg='white',
                 relief=tk.RAISED, borderwidth=2, padx=30, pady=12).pack(side=tk.LEFT, padx=10)
        
        tk.Button(button_frame, text="Cancelar", command=self.dialog.destroy,
                 font=FONTS['button'], bg=COLORS['danger'], fg='white',
                 relief=tk.RAISED, borderwidth=2, padx=30, pady=12).pack(side=tk.LEFT, padx=10)
    
    def create_denominacion_row(self, parent, denominacion, tipo):
        """Crea una fila para ingresar cantidad de una denominación"""
        row_frame = tk.Frame(parent, bg=COLORS['bg_secondary'])
        row_frame.pack(fill=tk.X, padx=15, pady=5)
        
        tk.Label(row_frame, text=format_currency(denominacion), 
                font=FONTS['normal'], bg=COLORS['bg_secondary'],
                width=15, anchor='w').pack(side=tk.LEFT, padx=5)
        
        cantidad_var = tk.StringVar(value="0")
        cantidad_var.trace('w', lambda *args: self.calculate_total())
        
        entry = tk.Entry(row_frame, textvariable=cantidad_var, 
                        font=FONTS['normal'], width=10, justify='center')
        entry.pack(side=tk.LEFT, padx=5)
        
        key = f"{tipo}_{denominacion}"
        self.denominaciones_cantidad[key] = {
            'var': cantidad_var,
            'denominacion': denominacion,
            'tipo': tipo
        }
    
    def calculate_total(self):
        """Calcula el total del corte"""
        total = 0
        
        for key, data in self.denominaciones_cantidad.items():
            try:
                cantidad = int(data['var'].get())
                if cantidad > 0:
                    total += cantidad * data['denominacion']
            except ValueError:
                pass
        
        self.total_var.set(format_currency(total))
    
    def finalizar_dia(self):
        """Finaliza el día y realiza el corte"""
        try:
            # Calcular corte final
            corte_final = 0
            for key, data in self.denominaciones_cantidad.items():
                try:
                    cantidad = int(data['var'].get())
                    if cantidad >= 0:
                        corte_final += cantidad * data['denominacion']
                    else:
                        messagebox.showerror("Error", "Las cantidades no pueden ser negativas")
                        return
                except ValueError:
                    messagebox.showerror("Error", "Todas las cantidades deben ser números enteros")
                    return
            
            # Obtener egresos
            try:
                egresos = float(self.egresos_var.get())
            except ValueError:
                messagebox.showerror("Error", "Los egresos deben ser un número válido")
                return
            
            # Obtener dinero inicial
            dinero_inicial = float(db.get_config('dinero_inicial_dia') or 0)
            
            # Guardar corte en base de datos
            numero_corte = db.add_corte(dinero_inicial, corte_final, egresos)
            
            # Obtener información del corte
            from datetime import datetime
            fecha_hoy = datetime.now().strftime('%d/%m/%Y')
            
            # Calcular ventas del día (solo efectivo)
            db.cursor.execute('''
                SELECT SUM(total) as total_ventas
                FROM ventas
                WHERE fecha LIKE ? AND metodo_pago = 'Efectivo'
            ''', (f'{fecha_hoy}%',))
            
            result = db.cursor.fetchone()
            total_ventas = result['total_ventas'] if result['total_ventas'] else 0
            
            # Calcular ganancias
            db.cursor.execute('''
                SELECT SUM(v.total) - SUM(p.costo * v.cantidad) as ganancias
                FROM ventas v
                JOIN productos p ON v.id_producto = p.id
                WHERE v.fecha LIKE ?
            ''', (f'{fecha_hoy}%',))
            
            result = db.cursor.fetchone()
            ganancias = result['ganancias'] if result['ganancias'] else 0
            
            corte_esperado = dinero_inicial + total_ventas - egresos
            diferencia = corte_final - corte_esperado
            
            if abs(diferencia) < 0.01:
                estado = 'Cuadrado'
            elif diferencia > 0:
                estado = 'Sobrante'
            else:
                estado = 'Faltante'
            
            # Mostrar resumen
            resumen = f"""
CORTE DE CAJA #{numero_corte}

Dinero inicial: {format_currency(dinero_inicial)}
Ventas (efectivo): {format_currency(total_ventas)}
Egresos/Retiros: {format_currency(egresos)}

Corte esperado: {format_currency(corte_esperado)}
Corte final (contado): {format_currency(corte_final)}

Diferencia: {format_currency(abs(diferencia))}
Estado: {estado}

Ganancias del día: {format_currency(ganancias)}
            """
            
            messagebox.showinfo("Corte de Caja Completado", resumen)
            
            self.dialog.destroy()
            
            if self.callback:
                self.callback()
            
        except Exception as e:
            messagebox.showerror("Error", f"Error al finalizar día: {str(e)}")