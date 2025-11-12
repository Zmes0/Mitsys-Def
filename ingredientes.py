"""
Módulo de gestión de materia prima (ingredientes)
"""
import tkinter as tk
from tkinter import ttk, messagebox
from config import COLORS, FONTS
from utils import format_currency, validate_float
from database import db

class IngredientesWindow:
    def __init__(self, parent, on_close=None):
        self.on_close_callback = on_close
        
        self.window = tk.Toplevel(parent)
        self.window.title("Materia Prima - Mitsy's POS")
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
        self.load_ingredientes()
    
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
        title_label = tk.Label(main_frame, text="Materia Prima", 
                              font=FONTS['title'], bg=COLORS['bg_primary'],
                              fg=COLORS['text_primary'])
        title_label.pack(pady=(0, 20))
        
        # Frame de búsqueda y filtros
        search_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        search_frame.pack(fill=tk.X, pady=(0, 20))
        
        tk.Label(search_frame, text="Buscar:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(side=tk.LEFT, padx=(0, 10))
        
        self.search_var = tk.StringVar()
        self.search_var.trace('w', lambda *args: self.search_ingredientes())
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
        columns = ('ID', 'Ingrediente', 'U. Almacén', 'Costo Unitario', 
                   'Cantidad Stock', 'Gestión Stock')
        
        self.tree = ttk.Treeview(table_frame, columns=columns, show='headings',
                                yscrollcommand=scrollbar.set, selectmode='extended')
        
        # Configurar columnas
        self.tree.heading('ID', text='ID')
        self.tree.heading('Ingrediente', text='Ingrediente')
        self.tree.heading('U. Almacén', text='Unidad Almacén')
        self.tree.heading('Costo Unitario', text='Costo Unitario')
        self.tree.heading('Cantidad Stock', text='Cantidad en Stock')
        self.tree.heading('Gestión Stock', text='Gestión Stock')
        
        self.tree.column('ID', width=80, anchor='center')
        self.tree.column('Ingrediente', width=250)
        self.tree.column('U. Almacén', width=150, anchor='center')
        self.tree.column('Costo Unitario', width=150, anchor='e')
        self.tree.column('Cantidad Stock', width=180, anchor='e')
        self.tree.column('Gestión Stock', width=150, anchor='center')
        
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
            ("Modificar Ingrediente", self.modificar_ingrediente),
            ("Borrar Ingrediente", self.borrar_ingrediente),
            ("Registrar Compra", self.registrar_compra),
            ("Agregar Ingrediente", self.add_ingrediente_dialog)
        ]
        
        for text, command in buttons:
            btn = tk.Button(button_frame, text=text, command=command,
                          font=FONTS['button'], bg=COLORS['button_bg'],
                          fg=COLORS['text_primary'], relief=tk.RAISED,
                          borderwidth=2, padx=20, pady=10)
            btn.pack(side=tk.LEFT, padx=5)
    
    def load_ingredientes(self):
        """Carga los ingredientes en la tabla"""
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        ingredientes = db.get_ingredientes()
        
        for idx, ing in enumerate(ingredientes):
            tag = 'evenrow' if idx % 2 == 0 else 'oddrow'
            
            values = (
                ing['id'],
                ing['nombre'],
                ing['unidad_almacen'],
                format_currency(ing['costo_unitario']),
                f"{ing['cantidad_stock']:.2f}",
                'Sí' if ing['gestion_stock'] else 'No'
            )
            
            self.tree.insert('', tk.END, values=values, tags=(tag,))
    
    def search_ingredientes(self):
        """Busca ingredientes según el texto ingresado"""
        from utils import normalize_text
        query = normalize_text(self.search_var.get())
        
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        if not query:
            self.load_ingredientes()
            return
        
        ingredientes = db.get_ingredientes()
        filtered = [ing for ing in ingredientes 
                   if query in normalize_text(ing['nombre'])]
        
        for idx, ing in enumerate(filtered):
            tag = 'evenrow' if idx % 2 == 0 else 'oddrow'
            
            values = (
                ing['id'],
                ing['nombre'],
                ing['unidad_almacen'],
                format_currency(ing['costo_unitario']),
                f"{ing['cantidad_stock']:.2f}",
                'Sí' if ing['gestion_stock'] else 'No'
            )
            
            self.tree.insert('', tk.END, values=values, tags=(tag,))
    
    def clear_filter(self):
        """Limpia los filtros de búsqueda"""
        self.search_var.set("")
        self.load_ingredientes()
    
    def add_ingrediente_dialog(self):
        """Abre diálogo para añadir ingrediente"""
        IngredienteDialog(self.window, callback=self.load_ingredientes)
    
    def modificar_ingrediente(self):
        """Abre diálogo para modificar ingrediente"""
        selection = self.tree.selection()
        
        if not selection:
            messagebox.showwarning("Advertencia", 
                                  "Por favor selecciona un ingrediente para modificar")
            return
        
        if len(selection) > 1:
            messagebox.showwarning("Advertencia", 
                                  "Por favor selecciona solo un ingrediente para modificar")
            return
        
        item = self.tree.item(selection[0])
        ingrediente_id = item['values'][0]
        
        IngredienteDialog(self.window, ingrediente_id=ingrediente_id,
                         callback=self.load_ingredientes)
    
    def borrar_ingrediente(self):
        """Elimina ingredientes seleccionados"""
        selection = self.tree.selection()
        
        if not selection:
            messagebox.showwarning("Advertencia", 
                                  "Por favor selecciona al menos un ingrediente para borrar")
            return
        
        if not messagebox.askyesno("Confirmar", 
                                   f"¿Estás seguro de borrar {len(selection)} ingrediente(s)?"):
            return
        
        for item in selection:
            ingrediente_id = self.tree.item(item)['values'][0]
            db.delete_ingrediente(ingrediente_id)
        
        messagebox.showinfo("Éxito", "Ingrediente(s) eliminado(s) correctamente")
        self.load_ingredientes()
    
    def registrar_compra(self):
        """Registra una compra de ingrediente"""
        selection = self.tree.selection()
        
        if not selection:
            messagebox.showwarning("Advertencia", 
                                  "Por favor selecciona un ingrediente para registrar compra")
            return
        
        if len(selection) > 1:
            messagebox.showwarning("Advertencia", 
                                  "Por favor selecciona solo un ingrediente")
            return
        
        item = self.tree.item(selection[0])
        ingrediente_id = item['values'][0]
        ingrediente_nombre = item['values'][1]
        
        RegistrarCompraDialog(self.window, ingrediente_id, ingrediente_nombre,
                             callback=self.load_ingredientes)
    
    def close_window(self):
        """Cierra la ventana y vuelve al menú"""
        self.window.destroy()
        if self.on_close_callback:
            self.on_close_callback()


class IngredienteDialog:
    def __init__(self, parent, ingrediente_id=None, callback=None):
        self.ingrediente_id = ingrediente_id
        self.callback = callback
        
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("Añadir Ingrediente" if not ingrediente_id else "Modificar Ingrediente")
        self.dialog.geometry("450x500")
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
        
        if ingrediente_id:
            self.load_ingrediente_data()
    
    def center_dialog(self):
        """Centra el diálogo en la pantalla"""
        self.dialog.update_idletasks()
        width = 450
        height = 500
        x = (self.dialog.winfo_screenwidth() // 2) - (width // 2)
        y = (self.dialog.winfo_screenheight() // 2) - (height // 2)
        self.dialog.geometry(f"{width}x{height}+{x}+{y}")
    
    def setup_ui(self):
        """Configura la interfaz del diálogo"""
        main_frame = tk.Frame(self.dialog, bg=COLORS['bg_primary'])
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)
        
        # ID (editable)
        tk.Label(main_frame, text="ID:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=(10, 5))
        self.id_var = tk.StringVar()
        if not self.ingrediente_id:
            self.id_var.set(str(db.get_next_ingrediente_id()))
        else:
            self.id_var.set(str(self.ingrediente_id))
        
        tk.Entry(main_frame, textvariable=self.id_var, font=FONTS['normal'],
                width=40).pack(fill=tk.X, pady=(0, 10))
        
        # Nombre
        tk.Label(main_frame, text="Nombre:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=(10, 5))
        self.nombre_var = tk.StringVar()
        tk.Entry(main_frame, textvariable=self.nombre_var, font=FONTS['normal'],
                width=40).pack(fill=tk.X, pady=(0, 10))
        
        # Costo Unitario
        tk.Label(main_frame, text="Costo Unitario:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=5)
        self.costo_var = tk.StringVar()
        tk.Entry(main_frame, textvariable=self.costo_var, font=FONTS['normal'],
                width=40).pack(fill=tk.X, pady=(0, 10))
        
        # Unidad de Almacén
        tk.Label(main_frame, text="Unidad de Almacén:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=5)
        
        unidad_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        unidad_frame.pack(anchor='w', pady=(0, 10))
        
        self.unidad_var = tk.StringVar(value='Kg')
        for unidad in ['Pza', 'Kg', 'L']:
            tk.Radiobutton(unidad_frame, text=unidad, variable=self.unidad_var,
                          value=unidad, font=FONTS['normal'],
                          bg=COLORS['bg_primary']).pack(side=tk.LEFT, padx=10)
        
        # Cantidad en Stock
        tk.Label(main_frame, text="Cantidad en Stock:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=5)
        self.stock_var = tk.StringVar(value="0")
        tk.Entry(main_frame, textvariable=self.stock_var, font=FONTS['normal'],
                width=40).pack(fill=tk.X, pady=(0, 10))
        
        # Gestión de Stock
        tk.Label(main_frame, text="Gestión de Stock:", font=FONTS['normal'],
                bg=COLORS['bg_primary']).pack(anchor='w', pady=5)
        
        gestion_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        gestion_frame.pack(anchor='w', pady=(0, 20))
        
        self.gestion_var = tk.BooleanVar(value=False)
        
        tk.Radiobutton(gestion_frame, text="Sí", variable=self.gestion_var,
                      value=True, font=FONTS['normal'],
                      bg=COLORS['bg_primary']).pack(side=tk.LEFT, padx=10)
        tk.Radiobutton(gestion_frame, text="No", variable=self.gestion_var,
                      value=False, font=FONTS['normal'],
                      bg=COLORS['bg_primary']).pack(side=tk.LEFT, padx=10)
        
        # Botones
        button_frame = tk.Frame(main_frame, bg=COLORS['bg_primary'])
        button_frame.pack(pady=20)
        
        tk.Button(button_frame, text="Aceptar", command=self.save_ingrediente,
                 font=FONTS['button'], bg=COLORS['success'], fg='white',
                 relief=tk.RAISED, borderwidth=2, padx=30, pady=10).pack(side=tk.LEFT, padx=10)
        
        tk.Button(button_frame, text="Cancelar", command=self.dialog.destroy,
                 font=FONTS['button'], bg=COLORS['danger'], fg='white',
                 relief=tk.RAISED, borderwidth=2, padx=30, pady=10).pack(side=tk.LEFT, padx=10)
    
    def load_ingrediente_data(self):
        """Carga los datos del ingrediente a editar"""
        ingrediente = db.get_ingrediente(self.ingrediente_id)
        
        if not ingrediente:
            messagebox.showerror("Error", "Ingrediente no encontrado")
            self.dialog.destroy()
            return
        
        self.id_var.set(str(ingrediente['id']))
        self.nombre_var.set(ingrediente['nombre'])
        self.costo_var.set(str(ingrediente['costo_unitario']))
        self.unidad_var.set(ingrediente['unidad_almacen'])
        self.stock_var.set(str(ingrediente['cantidad_stock']))
        self.gestion_var.set(bool(ingrediente['gestion_stock']))
    
    def save_ingrediente(self):
        """Guarda el ingrediente"""
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
            costo = float(self.costo_var.get())
            stock = float(self.stock_var.get())
        except ValueError:
            messagebox.showerror("Error", "Costo y stock deben ser números válidos")
            return
        
        try:
            if self.ingrediente_id:
                # Actualizar ingrediente
                db.update_ingrediente(self.ingrediente_id, new_id,
                                    nombre=nombre,
                                    costo_unitario=costo,
                                    unidad_almacen=self.unidad_var.get(),
                                    cantidad_stock=stock,
                                    gestion_stock=1 if self.gestion_var.get() else 0)
                
                # Actualizar stocks estimados de productos que usan este ingrediente
                db.actualizar_todos_stocks_estimados()
            else:
                # Verificar si el ID ya existe
                if db.id_exists('ingredientes', new_id):
                    messagebox.showerror("Error", f"El ID {new_id} ya existe")
                    return
                
                # Crear nuevo ingrediente
                db.add_ingrediente(new_id, nombre, costo, self.unidad_var.get(),
                                 stock, self.gestion_var.get())
            
            messagebox.showinfo("Éxito", "Ingrediente guardado correctamente")
            
            if self.callback:
                self.callback()
            
            self.dialog.destroy()
            
        except ValueError as e:
            messagebox.showerror("Error", str(e))
        except Exception as e:
            messagebox.showerror("Error", f"Error al guardar ingrediente: {str(e)}")


class RegistrarCompraDialog:
    def __init__(self, parent, ingrediente_id, ingrediente_nombre, callback=None):
        self.ingrediente_id = ingrediente_id
        self.ingrediente_nombre = ingrediente_nombre
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
        
        # Ingrediente
        tk.Label(main_frame, text=f"Ingrediente: {self.ingrediente_nombre}", 
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
            
            db.registrar_compra_ingrediente(self.ingrediente_id, cantidad)
            
            # Actualizar stocks estimados de productos que usan este ingrediente
            db.actualizar_todos_stocks_estimados()
            
            messagebox.showinfo("Éxito", f"Se registró la compra de {cantidad} unidades")
            
            if self.callback:
                self.callback()
            
            self.dialog.destroy()
            
        except ValueError:
            messagebox.showerror("Error", "La cantidad debe ser un número válido")
        except Exception as e:
            messagebox.showerror("Error", f"Error al registrar compra: {str(e)}")