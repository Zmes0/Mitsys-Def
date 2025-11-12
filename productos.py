"""
Módulo de gestión de productos
"""
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from PIL import Image, ImageTk
import os
from config import COLORS, FONTS
from utils import format_currency, parse_currency, validate_float
from database import db

class ProductosWindow:
    def __init__(self, parent, on_close=None):
        self.on_close_callback = on_close
        
        self.window = tk.Toplevel(parent)
        self.window.title("Productos - Mitsy's POS")
        self.window.geometry("1200x700")
        self.window.configure(bg=COLORS['bg_primary'])
        
        # Centrar ventana
        self.center_window()
        
        # Forzar al frente
        self.window.lift()
        self.window.attributes('-topmost', True)
        self.window.after(100, lambda: self.window.attributes('-topmost', False))
        
        # Protocolo de cierre
        self.window.protocol("WM_DELETE_WINDOW", self.close_window)
        
        self.selected_items = []
        
        self.setup_ui()
        self.load_productos()
    
    def center_window(self):
        """Centra la ventana en la pantalla"""
        self.window.update_idletasks()
        width = 1200
        height = 700
        x = (self.window.winfo_screenwidth() // 2) - (width // 2)
        y = (self.window.winfo_screenheight() // 2) - (height // 2)
        self.window.geometry(f"{width}x{height}+{x}+{y}")
    
    def setup_ui(self):
        """Configura la interfaz de usuario"""
        
        # Frame principal
        main_frame = tk.Frame(self.window, bg=COLORS['bg_primary'])
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Título
        title_label = tk.Label(main_frame, text="Productos", 
                              font=FONTS['title'], bg=COLORS['bg_primary'],
                              fg=COLORS['text_primary'])
        title_label.pack(pady=(0, 20))
        
        # Frame de búsqueda
        search_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        search_frame.pack(fill=tk.X, pady=(0, 20))
        
        tk.Label(search_frame, text="Buscar:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(side=tk.LEFT, padx=(0, 10))
        
        self.search_var = tk.StringVar()
        self.search_var.trace('w', lambda *args: self.search_productos())
        search_entry = tk.Entry(search_frame, textvariable=self.search_var,
                               font=FONTS['normal'], width=40)
        search_entry.pack(side=tk.LEFT)
        
        # Frame con scrollbar para la tabla
        table_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        table_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 20))
        
        # Scrollbar
        scrollbar = ttk.Scrollbar(table_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Treeview (tabla)
        columns = ('ID', 'Nombre', 'Precio', 'Costo', 'Ganancia', 'U. Medida', 
                   'Stock', 'Gestión Stock')
        
        self.tree = ttk.Treeview(table_frame, columns=columns, show='headings',
                                yscrollcommand=scrollbar.set, selectmode='extended')
        
        # Configurar columnas
        self.tree.heading('ID', text='ID')
        self.tree.heading('Nombre', text='Nombre')
        self.tree.heading('Precio', text='Precio Unitario')
        self.tree.heading('Costo', text='Costo')
        self.tree.heading('Ganancia', text='Ganancia')
        self.tree.heading('U. Medida', text='U. Medida')
        self.tree.heading('Stock', text='Stock Estimado')
        self.tree.heading('Gestión Stock', text='Gestión Stock')
        
        self.tree.column('ID', width=50, anchor='center')
        self.tree.column('Nombre', width=200)
        self.tree.column('Precio', width=120, anchor='e')
        self.tree.column('Costo', width=120, anchor='e')
        self.tree.column('Ganancia', width=120, anchor='e')
        self.tree.column('U. Medida', width=100, anchor='center')
        self.tree.column('Stock', width=120, anchor='e')
        self.tree.column('Gestión Stock', width=120, anchor='center')
        
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.tree.yview)
        
        # Configurar colores alternados en filas
        self.tree.tag_configure('evenrow', background=COLORS['table_row_even'])
        self.tree.tag_configure('oddrow', background=COLORS['table_row_odd'])
        
        # Frame de botones (SIN botón "Gestión Stock")
        button_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        button_frame.pack(fill=tk.X)
        
        buttons = [
            ("Regresar", self.close_window),
            ("Editar Producto", self.editar_producto),
            ("Borrar Producto", self.borrar_producto),
            ("Añadir Producto", self.add_producto_dialog)
        ]
        
        for text, command in buttons:
            btn = tk.Button(button_frame, text=text, command=command,
                          font=FONTS['button'], bg=COLORS['button_bg'],
                          fg=COLORS['text_primary'], relief=tk.RAISED,
                          borderwidth=2, padx=20, pady=10)
            btn.pack(side=tk.LEFT, padx=5)
    
    def load_productos(self):
        """Carga los productos en la tabla"""
        # Limpiar tabla
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # Cargar productos
        productos = db.get_productos()
        
        for idx, p in enumerate(productos):
            tag = 'evenrow' if idx % 2 == 0 else 'oddrow'
            
            values = (
                p['id'],
                p['nombre'],
                format_currency(p['precio_unitario']),
                format_currency(p['costo']),
                format_currency(p['ganancia']),
                p['unidad_medida'],
                f"{p['stock_estimado']:.2f}" if db.is_gestion_stock_active() else "N/A",
                'Sí' if p['gestion_stock'] else 'No'
            )
            
            self.tree.insert('', tk.END, values=values, tags=(tag,))
    
    def search_productos(self):
        """Busca productos según el texto ingresado"""
        query = self.search_var.get()
        
        # Limpiar tabla
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        if not query:
            self.load_productos()
            return
        
        # Buscar productos
        productos = db.search_productos(query)
        
        for idx, p in enumerate(productos):
            tag = 'evenrow' if idx % 2 == 0 else 'oddrow'
            
            values = (
                p['id'],
                p['nombre'],
                format_currency(p['precio_unitario']),
                format_currency(p['costo']),
                format_currency(p['ganancia']),
                p['unidad_medida'],
                f"{p['stock_estimado']:.2f}" if db.is_gestion_stock_active() else "N/A",
                'Sí' if p['gestion_stock'] else 'No'
            )
            
            self.tree.insert('', tk.END, values=values, tags=(tag,))
    
    def add_producto_dialog(self):
        """Abre diálogo para añadir producto"""
        ProductoDialog(self.window, callback=self.load_productos)
    
    def editar_producto(self):
        """Abre diálogo para editar producto seleccionado"""
        selection = self.tree.selection()
        
        if not selection:
            messagebox.showwarning("Advertencia", 
                                  "Por favor selecciona un producto para editar")
            return
        
        if len(selection) > 1:
            messagebox.showwarning("Advertencia", 
                                  "Por favor selecciona solo un producto para editar")
            return
        
        item = self.tree.item(selection[0])
        producto_id = item['values'][0]
        
        ProductoDialog(self.window, producto_id=producto_id, 
                      callback=self.load_productos)
    
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
        self.load_productos()
    
    def close_window(self):
        """Cierra la ventana y vuelve al menú"""
        self.window.destroy()
        if self.on_close_callback:
            self.on_close_callback()


class ProductoDialog:
    def __init__(self, parent, producto_id=None, callback=None):
        self.producto_id = producto_id
        self.callback = callback
        self.ingredientes_agregados = []
        
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("Añadir Producto" if not producto_id else "Editar Producto")
        self.dialog.configure(bg=COLORS['bg_primary'])
        self.dialog.transient(parent)
        self.dialog.grab_set()
        
        # Forzar al frente
        self.dialog.lift()
        self.dialog.attributes('-topmost', True)
        self.dialog.after(100, lambda: self.dialog.attributes('-topmost', False))
        
        self.setup_ui()
        
        # Centrar después de crear UI
        self.center_dialog()
        
        if producto_id:
            self.load_producto_data()
    
    def center_dialog(self):
        """Centra el diálogo en la pantalla"""
        self.dialog.update_idletasks()
        width = self.dialog.winfo_width()
        height = self.dialog.winfo_height()
        x = (self.dialog.winfo_screenwidth() // 2) - (width // 2)
        y = (self.dialog.winfo_screenheight() // 2) - (height // 2)
        self.dialog.geometry(f"+{x}+{y}")
    
    def setup_ui(self):
        """Configura la interfaz del diálogo"""
        # Frame principal con scrollbar
        main_canvas = tk.Canvas(self.dialog, bg=COLORS['bg_primary'], 
                               highlightthickness=0, width=500)
        scrollbar = tk.Scrollbar(self.dialog, orient="vertical", 
                                command=main_canvas.yview)
        
        self.scrollable_frame = tk.Frame(main_canvas, bg=COLORS['bg_primary'])
        
        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: main_canvas.configure(scrollregion=main_canvas.bbox("all"))
        )
        
        main_canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        main_canvas.configure(yscrollcommand=scrollbar.set)
        
        main_frame = tk.Frame(self.scrollable_frame, bg=COLORS['bg_primary'])
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # ID (editable)
        tk.Label(main_frame, text="ID:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=(10, 5))
        self.id_var = tk.StringVar()
        if not self.producto_id:
            self.id_var.set(str(db.get_next_producto_id()))
        else:
            self.id_var.set(str(self.producto_id))
        
        tk.Entry(main_frame, textvariable=self.id_var, font=FONTS['normal'],
                width=40).pack(fill=tk.X, pady=(0, 10))
        
        # Nombre
        tk.Label(main_frame, text="Nombre:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=(10, 5))
        self.nombre_var = tk.StringVar()
        tk.Entry(main_frame, textvariable=self.nombre_var, font=FONTS['normal'],
                width=40).pack(fill=tk.X, pady=(0, 10))
        
        # Precio Unitario
        tk.Label(main_frame, text="Precio Unitario:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=5)
        self.precio_var = tk.StringVar()
        tk.Entry(main_frame, textvariable=self.precio_var, font=FONTS['normal'],
                width=40).pack(fill=tk.X, pady=(0, 10))
        
        # Costo
        tk.Label(main_frame, text="Costo:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=5)
        self.costo_var = tk.StringVar()
        costo_entry = tk.Entry(main_frame, textvariable=self.costo_var, 
                              font=FONTS['normal'], width=40)
        costo_entry.pack(fill=tk.X, pady=(0, 5))
        
        tk.Label(main_frame, text="(Se calcula automáticamente si tiene receta)", 
                font=FONTS['small'], bg=COLORS['bg_primary'],
                fg=COLORS['text_secondary']).pack(anchor='w', pady=(0, 10))
        
        # Unidad de Medida
        tk.Label(main_frame, text="Unidad de Medida:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=5)
        
        unidad_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        unidad_frame.pack(anchor='w', pady=(0, 10))
        
        self.unidad_var = tk.StringVar(value='Pza')
        for unidad in ['Pza', 'Kg', 'L']:
            tk.Radiobutton(unidad_frame, text=unidad, variable=self.unidad_var,
                          value=unidad, font=FONTS['normal'],
                          bg=COLORS['bg_primary']).pack(side=tk.LEFT, padx=10)
        
        # Gestionar inventario
        tk.Label(main_frame, text="Gestionar inventario de este producto:", 
                font=FONTS['normal'], bg=COLORS['bg_primary']).pack(anchor='w', pady=5)
        
        gestion_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        gestion_frame.pack(anchor='w', pady=(0, 10))
        
        self.gestion_var = tk.BooleanVar(value=False)
        self.gestion_var.trace('w', self.toggle_ingredientes)
        
        tk.Radiobutton(gestion_frame, text="Sí", variable=self.gestion_var,
                      value=True, font=FONTS['normal'],
                      bg=COLORS['bg_primary']).pack(side=tk.LEFT, padx=10)
        tk.Radiobutton(gestion_frame, text="No", variable=self.gestion_var,
                      value=False, font=FONTS['normal'],
                      bg=COLORS['bg_primary']).pack(side=tk.LEFT, padx=10)
        
        # Frame de ingredientes (inicialmente oculto)
        self.ingredientes_frame = tk.Frame(main_frame, bg=COLORS['bg_secondary'],
                                          relief=tk.SUNKEN, borderwidth=2)
        
        tk.Label(self.ingredientes_frame, text="Ingredientes de la receta:", 
                font=FONTS['heading'], bg=COLORS['bg_secondary']).pack(pady=10)
        
        # Lista de ingredientes
        self.ingredientes_listbox = tk.Listbox(self.ingredientes_frame, 
                                              font=FONTS['normal'], height=6)
        self.ingredientes_listbox.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # Imagen del producto
        tk.Label(main_frame, text="Imagen del producto:", 
                font=FONTS['normal'], bg=COLORS['bg_primary']).pack(anchor='w', pady=5)
        
        imagen_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        imagen_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.imagen_var = tk.StringVar()
        imagen_entry = tk.Entry(imagen_frame, textvariable=self.imagen_var, 
                               font=FONTS['normal'], state='readonly')
        imagen_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))
        
        tk.Button(imagen_frame, text="Examinar", command=self.browse_image,
                 font=FONTS['button'], bg=COLORS['button_bg'],
                 relief=tk.RAISED, borderwidth=2, padx=15, pady=5).pack(side=tk.LEFT)
        
        # Botón añadir ingrediente
        self.btn_add_ingrediente = tk.Button(self.ingredientes_frame, 
                                            text="Añadir Ingrediente",
                                            command=self.add_ingrediente_dialog,
                                            font=FONTS['button'], 
                                            bg=COLORS['button_bg'],
                                            relief=tk.RAISED, borderwidth=2,
                                            padx=15, pady=5)
        self.btn_add_ingrediente.pack(pady=10)
        
        # Cantidad en stock (opcional)
        tk.Label(main_frame, text="Cantidad en Stock (opcional):", 
                font=FONTS['normal'], bg=COLORS['bg_primary']).pack(anchor='w', pady=5)
        self.stock_var = tk.StringVar(value="0")
        tk.Entry(main_frame, textvariable=self.stock_var, font=FONTS['normal'],
                width=40).pack(fill=tk.X, pady=(0, 10))
        
        # Botones
        button_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        button_frame.pack(pady=20)
        
        tk.Button(button_frame, text="Aceptar", command=self.save_producto,
                 font=FONTS['button'], bg=COLORS['success'], fg='white',
                 relief=tk.RAISED, borderwidth=2, padx=30, pady=10).pack(side=tk.LEFT, padx=10)
        
        tk.Button(button_frame, text="Cancelar", command=self.dialog.destroy,
                 font=FONTS['button'], bg=COLORS['danger'], fg='white',
                 relief=tk.RAISED, borderwidth=2, padx=30, pady=10).pack(side=tk.LEFT, padx=10)
        
        # Empaquetar canvas y scrollbar
        main_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Inicializar tamaño
        self.dialog.geometry("550x700")
    
    def toggle_ingredientes(self, *args):
        """Muestra/oculta el frame de ingredientes y ajusta el tamaño de la ventana"""
        if self.gestion_var.get():
            self.ingredientes_frame.pack(fill=tk.BOTH, expand=True, pady=10)
            # Aumentar altura de la ventana
            self.dialog.geometry("550x900")
        else:
            self.ingredientes_frame.pack_forget()
            # Restaurar altura original
            self.dialog.geometry("550x700")
        
        # Recentrar después de cambiar tamaño
        self.dialog.after(10, self.center_dialog)
    
    def load_producto_data(self):
        """Carga los datos del producto a editar"""
        producto = db.get_producto(self.producto_id)
        
        if not producto:
            messagebox.showerror("Error", "Producto no encontrado")
            self.dialog.destroy()
            return
        
        self.id_var.set(str(producto['id']))
        self.nombre_var.set(producto['nombre'])
        self.precio_var.set(str(producto['precio_unitario']))
        self.costo_var.set(str(producto['costo']))
        self.unidad_var.set(producto['unidad_medida'])
        self.gestion_var.set(bool(producto['gestion_stock']))
        self.stock_var.set(str(producto['stock_estimado']))
        
        # Cargar ingredientes si tiene
        if producto['gestion_stock']:
            recetas = db.get_recetas_producto(self.producto_id)
            for receta in recetas:
                self.ingredientes_agregados.append({
                    'id': receta['id_ingrediente'],
                    'nombre': receta['ingrediente_nombre'],
                    'cantidad': receta['cantidad_requerida'],
                    'unidad': receta['unidad_porcionamiento']
                })
            
            self.update_ingredientes_list()
            
        if producto.get('imagen'):
            self.imagen_var.set(producto['imagen'])    
    
    def add_ingrediente_dialog(self):
        """Abre diálogo para añadir ingrediente"""
        IngredienteRecetaDialog(self.dialog, callback=self.add_ingrediente_to_list)
    
    def add_ingrediente_to_list(self, ingrediente_data):
        """Añade un ingrediente a la lista"""
        self.ingredientes_agregados.append(ingrediente_data)
        self.update_ingredientes_list()
    
    def update_ingredientes_list(self):
        """Actualiza la lista de ingredientes"""
        self.ingredientes_listbox.delete(0, tk.END)
        
        for ing in self.ingredientes_agregados:
            text = f"{ing['nombre']} - {ing['cantidad']} {ing['unidad']}"
            self.ingredientes_listbox.insert(tk.END, text)
    
    def save_producto(self):
        """Guarda el producto"""
        # Validar ID
        try:
            new_id = int(self.id_var.get())
            if new_id <= 0:
                raise ValueError()
        except ValueError:
            messagebox.showerror("Error", "El ID debe ser un número entero positivo")
            return
        
        # Validaciones
        nombre = self.nombre_var.get().strip()
        if not nombre:
            messagebox.showerror("Error", "El nombre es obligatorio")
            return
        
        try:
            precio = float(self.precio_var.get())
            costo = float(self.costo_var.get()) if self.costo_var.get() else 0
            stock = float(self.stock_var.get()) if self.stock_var.get() else 0
        except ValueError:
            messagebox.showerror("Error", "Precio, costo y stock deben ser números válidos")
            return
        
        gestion = self.gestion_var.get()
        
        # Si gestiona inventario, debe tener ingredientes
        if gestion and not self.ingredientes_agregados:
            messagebox.showwarning("Advertencia", 
                                  "Si gestiona inventario, debe añadir al menos un ingrediente")
            return
        
        try:
            if self.producto_id:
                # Actualizar producto
                db.update_producto(self.producto_id, new_id,
                                 nombre=nombre,
                                 precio_unitario=precio,
                                 costo=costo,
                                 unidad_medida=self.unidad_var.get(),
                                 gestion_stock=1 if gestion else 0,
                                 stock_estimado=stock,
                                 imagen=self.imagen_var.get() or None)  # AÑADIR
                
                # Eliminar recetas anteriores
                recetas_anteriores = db.get_recetas_producto(new_id)
                for receta in recetas_anteriores:
                    db.delete_receta(receta['id'])
                
                producto_id = new_id
            else:
                # Verificar si el ID ya existe
                if db.id_exists('productos', new_id):
                    messagebox.showerror("Error", f"El ID {new_id} ya existe")
                    return
                
                # Crear nuevo producto
                producto_id = db.add_producto(new_id, nombre, precio, costo,
                                            self.unidad_var.get(), gestion,
                                            stock, imagen=self.imagen_var.get() or None)
            
            # Añadir ingredientes (recetas)
            if gestion:
                for ing in self.ingredientes_agregados:
                    receta_id = db.get_next_receta_id()
                    db.add_receta(receta_id, producto_id, ing['id'], 
                                ing['cantidad'], ing['unidad'])
            
            messagebox.showinfo("Éxito", "Producto guardado correctamente")
            
            if self.callback:
                self.callback()
            
            self.dialog.destroy()
            
        except ValueError as e:
            messagebox.showerror("Error", str(e))
        except Exception as e:
            messagebox.showerror("Error", f"Error al guardar producto: {str(e)}")
            
    def browse_image(self):
        """Abre diálogo para seleccionar imagen"""
        from tkinter import filedialog
        
        filename = filedialog.askopenfilename(
            title="Seleccionar imagen del producto",
            filetypes=[
                ("Imágenes", "*.png *.jpg *.jpeg *.gif *.bmp"),
                ("Todos los archivos", "*.*")
            ]
        )
        
        if filename:
            # Copiar imagen a la carpeta images/productos/
            import shutil
            os.makedirs('images/productos', exist_ok=True)
            
            # Generar nombre único
            from datetime import datetime
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            extension = os.path.splitext(filename)[1]
            nuevo_nombre = f"producto_{timestamp}{extension}"
            destino = os.path.join('images/productos', nuevo_nombre)
            
            try:
                shutil.copy2(filename, destino)
                self.imagen_var.set(destino)
                messagebox.showinfo("Éxito", "Imagen cargada correctamente")
            except Exception as e:
                messagebox.showerror("Error", f"Error al copiar imagen: {str(e)}")

class IngredienteRecetaDialog:
    def __init__(self, parent, callback=None):
        self.callback = callback
        
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("Añadir Ingrediente a Receta")
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
        """Configura la interfaz"""
        main_frame = tk.Frame(self.dialog, bg=COLORS['bg_primary'])
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Ingrediente
        tk.Label(main_frame, text="Ingrediente:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=5)
        
        self.ingrediente_var = tk.StringVar()
        self.ingrediente_combo = ttk.Combobox(main_frame, 
                                             textvariable=self.ingrediente_var,
                                             font=FONTS['normal'], state='readonly')
        self.ingrediente_combo.pack(fill=tk.X, pady=(0, 10))
        
        # Cargar ingredientes
        ingredientes = db.get_ingredientes()
        self.ingredientes_dict = {f"{i['nombre']} (ID: {i['id']})": i 
                                 for i in ingredientes}
        self.ingrediente_combo['values'] = list(self.ingredientes_dict.keys())
        
        # Cantidad requerida
        tk.Label(main_frame, text="Cantidad Requerida:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=5)
        self.cantidad_var = tk.StringVar()
        tk.Entry(main_frame, textvariable=self.cantidad_var, 
                font=FONTS['normal']).pack(fill=tk.X, pady=(0, 10))
        
        # Unidad de porcionamiento
        tk.Label(main_frame, text="Unidad de Porcionamiento:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=5)
        
        unidad_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        unidad_frame.pack(anchor='w', pady=(0, 20))
        
        self.unidad_var = tk.StringVar(value='Kg')
        for unidad in ['Pza', 'Kg', 'L']:
            tk.Radiobutton(unidad_frame, text=unidad, variable=self.unidad_var,
                          value=unidad, font=FONTS['normal'],
                          bg=COLORS['bg_primary']).pack(side=tk.LEFT, padx=10)
        
        # Botones
        button_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        button_frame.pack()
        
        tk.Button(button_frame, text="Aceptar", command=self.accept,
                 font=FONTS['button'], bg=COLORS['success'], fg='white',
                 relief=tk.RAISED, borderwidth=2, padx=30, pady=10).pack(side=tk.LEFT, padx=10)
        
        tk.Button(button_frame, text="Cancelar", command=self.dialog.destroy,
                 font=FONTS['button'], bg=COLORS['danger'], fg='white',
                 relief=tk.RAISED, borderwidth=2, padx=30, pady=10).pack(side=tk.LEFT, padx=10)
    
    def accept(self):
        """Acepta y retorna los datos"""
        if not self.ingrediente_var.get():
            messagebox.showerror("Error", "Debe seleccionar un ingrediente")
            return
        
        try:
            cantidad = float(self.cantidad_var.get())
        except ValueError:
            messagebox.showerror("Error", "La cantidad debe ser un número válido")
            return
        
        ingrediente = self.ingredientes_dict[self.ingrediente_var.get()]
        
        data = {
            'id': ingrediente['id'],
            'nombre': ingrediente['nombre'],
            'cantidad': cantidad,
            'unidad': self.unidad_var.get()
        }
        
        if self.callback:
            self.callback(data)
        
        self.dialog.destroy()
        