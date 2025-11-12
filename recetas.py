"""
Módulo de gestión de recetas
"""
import tkinter as tk
from tkinter import ttk, messagebox
from config import COLORS, FONTS
from database import db

class RecetasWindow:
    def __init__(self, parent, on_close=None):
        self.on_close_callback = on_close
        
        self.window = tk.Toplevel(parent)
        self.window.title("Recetas - Mitsy's POS")
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
        
        self.setup_ui()
        self.load_recetas()
    
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
        main_frame = tk.Frame(self.window, bg=COLORS['bg_primary'])
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # Título
        title_label = tk.Label(main_frame, text="Recetas", 
                              font=FONTS['title'], bg=COLORS['bg_primary'],
                              fg=COLORS['text_primary'])
        title_label.pack(pady=(0, 20))
        
        # Frame de búsqueda
        search_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        search_frame.pack(fill=tk.X, pady=(0, 20))
        
        tk.Label(search_frame, text="Buscar:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(side=tk.LEFT, padx=(0, 10))
        
        self.search_var = tk.StringVar()
        self.search_var.trace('w', lambda *args: self.search_recetas())
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
        columns = ('ID Receta', 'ID Producto', 'Producto', 'ID Ingrediente', 
                   'Ingrediente', 'Cantidad Requerida', 'Unidad')
        
        self.tree = ttk.Treeview(table_frame, columns=columns, show='headings',
                                yscrollcommand=scrollbar.set, selectmode='extended')
        
        # Configurar columnas
        self.tree.heading('ID Receta', text='ID Receta')
        self.tree.heading('ID Producto', text='ID Producto')
        self.tree.heading('Producto', text='Producto')
        self.tree.heading('ID Ingrediente', text='ID Ingrediente')
        self.tree.heading('Ingrediente', text='Ingrediente')
        self.tree.heading('Cantidad Requerida', text='Cantidad Requerida')
        self.tree.heading('Unidad', text='Unidad Porcionamiento')
        
        self.tree.column('ID Receta', width=100, anchor='center')
        self.tree.column('ID Producto', width=100, anchor='center')
        self.tree.column('Producto', width=200)
        self.tree.column('ID Ingrediente', width=120, anchor='center')
        self.tree.column('Ingrediente', width=200)
        self.tree.column('Cantidad Requerida', width=150, anchor='e')
        self.tree.column('Unidad', width=150, anchor='center')
        
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.tree.yview)
        
        # Colores alternados
        self.tree.tag_configure('evenrow', background=COLORS['table_row_even'])
        self.tree.tag_configure('oddrow', background=COLORS['table_row_odd'])
        
        # Frame de botones (SIN Importar/Exportar Excel)
        button_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        button_frame.pack(fill=tk.X)
        
        buttons = [
            ("Regresar", self.close_window),
            ("Modificar Receta", self.modificar_receta),
            ("Borrar Receta", self.borrar_receta),
            ("Agregar Receta", self.add_receta_dialog)
        ]
        
        for text, command in buttons:
            btn = tk.Button(button_frame, text=text, command=command,
                          font=FONTS['button'], bg=COLORS['button_bg'],
                          fg=COLORS['text_primary'], relief=tk.RAISED,
                          borderwidth=2, padx=20, pady=10)
            btn.pack(side=tk.LEFT, padx=5)
    
    def load_recetas(self):
        """Carga las recetas en la tabla"""
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        recetas = db.get_todas_recetas()
        
        for idx, r in enumerate(recetas):
            tag = 'evenrow' if idx % 2 == 0 else 'oddrow'
            
            values = (
                r['id'],
                r['id_producto'],
                r['producto_nombre'],
                r['id_ingrediente'],
                r['ingrediente_nombre'],
                f"{r['cantidad_requerida']:.2f}",
                r['unidad_porcionamiento']
            )
            
            self.tree.insert('', tk.END, values=values, tags=(tag,))
    
    def search_recetas(self):
        """Busca recetas según el texto ingresado"""
        from utils import normalize_text
        query = normalize_text(self.search_var.get())
        
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        if not query:
            self.load_recetas()
            return
        
        recetas = db.get_todas_recetas()
        filtered = [r for r in recetas 
                   if query in normalize_text(r['producto_nombre']) or
                      query in normalize_text(r['ingrediente_nombre'])]
        
        for idx, r in enumerate(filtered):
            tag = 'evenrow' if idx % 2 == 0 else 'oddrow'
            
            values = (
                r['id'],
                r['id_producto'],
                r['producto_nombre'],
                r['id_ingrediente'],
                r['ingrediente_nombre'],
                f"{r['cantidad_requerida']:.2f}",
                r['unidad_porcionamiento']
            )
            
            self.tree.insert('', tk.END, values=values, tags=(tag,))
    
    def clear_filter(self):
        """Limpia los filtros de búsqueda"""
        self.search_var.set("")
        self.load_recetas()
    
    def add_receta_dialog(self):
        """Abre diálogo para añadir receta"""
        RecetaDialog(self.window, callback=self.load_recetas)
    
    def modificar_receta(self):
        """Abre diálogo para modificar receta"""
        selection = self.tree.selection()
        
        if not selection:
            messagebox.showwarning("Advertencia", 
                                  "Por favor selecciona una receta para modificar")
            return
        
        if len(selection) > 1:
            messagebox.showwarning("Advertencia", 
                                  "Por favor selecciona solo una receta para modificar")
            return
        
        item = self.tree.item(selection[0])
        receta_id = item['values'][0]
        
        RecetaDialog(self.window, receta_id=receta_id, callback=self.load_recetas)
    
    def borrar_receta(self):
        """Elimina recetas seleccionadas"""
        selection = self.tree.selection()
        
        if not selection:
            messagebox.showwarning("Advertencia", 
                                  "Por favor selecciona al menos una receta para borrar")
            return
        
        if not messagebox.askyesno("Confirmar", 
                                   f"¿Estás seguro de borrar {len(selection)} receta(s)?"):
            return
        
        for item in selection:
            receta_id = self.tree.item(item)['values'][0]
            db.delete_receta(receta_id)
        
        messagebox.showinfo("Éxito", "Receta(s) eliminada(s) correctamente")
        self.load_recetas()
    
    def close_window(self):
        """Cierra la ventana y vuelve al menú"""
        self.window.destroy()
        if self.on_close_callback:
            self.on_close_callback()


class RecetaDialog:
    def __init__(self, parent, receta_id=None, callback=None):
        self.receta_id = receta_id
        self.callback = callback
        
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("Añadir Receta" if not receta_id else "Modificar Receta")
        self.dialog.geometry("500x550")
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
        
        if receta_id:
            self.load_receta_data()
    
    def center_dialog(self):
        """Centra el diálogo en la pantalla"""
        self.dialog.update_idletasks()
        width = 500
        height = 550
        x = (self.dialog.winfo_screenwidth() // 2) - (width // 2)
        y = (self.dialog.winfo_screenheight() // 2) - (height // 2)
        self.dialog.geometry(f"{width}x{height}+{x}+{y}")
    
    def setup_ui(self):
        """Configura la interfaz del diálogo"""
        main_frame = tk.Frame(self.dialog, bg=COLORS['bg_primary'])
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # ID (editable)
        tk.Label(main_frame, text="ID Receta:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=(10, 5))
        self.id_var = tk.StringVar()
        if not self.receta_id:
            self.id_var.set(str(db.get_next_receta_id()))
        else:
            self.id_var.set(str(self.receta_id))
        
        tk.Entry(main_frame, textvariable=self.id_var, font=FONTS['normal'],
                width=40).pack(fill=tk.X, pady=(0, 10))
        
        # Producto
        tk.Label(main_frame, text="Producto:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=(10, 5))
        
        self.producto_var = tk.StringVar()
        self.producto_combo = ttk.Combobox(main_frame, textvariable=self.producto_var,
                                          font=FONTS['normal'], state='readonly')
        self.producto_combo.pack(fill=tk.X, pady=(0, 10))
        
        # Cargar productos
        productos = db.get_productos()
        self.productos_dict = {f"{p['nombre']} (ID: {p['id']})": p for p in productos}
        self.producto_combo['values'] = list(self.productos_dict.keys())
        
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
        
        # Cantidad Requerida
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
        button_frame.pack(pady=20)
        
        tk.Button(button_frame, text="Aceptar", command=self.save_receta,
                 font=FONTS['button'], bg=COLORS['success'], fg='white',
                 relief=tk.RAISED, borderwidth=2, padx=30, pady=10).pack(side=tk.LEFT, padx=10)
        
        tk.Button(button_frame, text="Cancelar", command=self.dialog.destroy,
                 font=FONTS['button'], bg=COLORS['danger'], fg='white',
                 relief=tk.RAISED, borderwidth=2, padx=30, pady=10).pack(side=tk.LEFT, padx=10)
    
    def load_receta_data(self):
        """Carga los datos de la receta a editar"""
        receta = db.get_receta(self.receta_id)
        
        if not receta:
            messagebox.showerror("Error", "Receta no encontrada")
            self.dialog.destroy()
            return
        
        self.id_var.set(str(receta['id']))
        
        # Establecer valores
        producto_key = f"{receta['producto_nombre']} (ID: {receta['id_producto']})"
        self.producto_var.set(producto_key)
        
        ingrediente_key = f"{receta['ingrediente_nombre']} (ID: {receta['id_ingrediente']})"
        self.ingrediente_var.set(ingrediente_key)
        
        self.cantidad_var.set(str(receta['cantidad_requerida']))
        self.unidad_var.set(receta['unidad_porcionamiento'])
    
    def save_receta(self):
        """Guarda la receta"""
        # Validar ID
        try:
            new_id = int(self.id_var.get())
            if new_id <= 0:
                raise ValueError()
        except ValueError:
            messagebox.showerror("Error", "El ID debe ser un número entero positivo")
            return
        
        # Validaciones
        if not self.producto_var.get():
            messagebox.showerror("Error", "Debe seleccionar un producto")
            return
        
        if not self.ingrediente_var.get():
            messagebox.showerror("Error", "Debe seleccionar un ingrediente")
            return
        
        try:
            cantidad = float(self.cantidad_var.get())
            if cantidad <= 0:
                raise ValueError()
        except ValueError:
            messagebox.showerror("Error", "La cantidad debe ser un número válido mayor a 0")
            return
        
        producto = self.productos_dict[self.producto_var.get()]
        ingrediente = self.ingredientes_dict[self.ingrediente_var.get()]
        
        try:
            if self.receta_id:
                # Actualizar receta
                db.update_receta(self.receta_id, new_id,
                               id_producto=producto['id'],
                               id_ingrediente=ingrediente['id'],
                               cantidad_requerida=cantidad,
                               unidad_porcionamiento=self.unidad_var.get())
            else:
                # Verificar si el ID ya existe
                if db.id_exists('recetas', new_id):
                    messagebox.showerror("Error", f"El ID {new_id} ya existe")
                    return
                
                # Crear nueva receta
                db.add_receta(new_id, producto['id'], ingrediente['id'], 
                            cantidad, self.unidad_var.get())
            
            messagebox.showinfo("Éxito", "Receta guardada correctamente")
            
            if self.callback:
                self.callback()
            
            self.dialog.destroy()
            
        except ValueError as e:
            messagebox.showerror("Error", str(e))
        except Exception as e:
            messagebox.showerror("Error", f"Error al guardar receta: {str(e)}")