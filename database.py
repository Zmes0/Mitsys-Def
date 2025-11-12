"""
Gestor de base de datos SQLite para Mitsy's POS
"""
import sqlite3
from datetime import datetime
from typing import Optional, List, Dict, Any
import os

class Database:
    def __init__(self, db_path: str = "data/mitsys.db"):
        """Inicializa la conexión a la base de datos"""
        # Crear carpeta data si no existe
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.connect()
        self.create_tables()
        self.init_config()
    
    def connect(self):
        """Establece conexión con la base de datos"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
    
    def close(self):
        """Cierra la conexión"""
        if self.conn:
            self.conn.close()
    
    def create_tables(self):
        """Crea todas las tablas necesarias"""
        
        # Tabla de Configuración Global
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS configuracion (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                clave TEXT UNIQUE NOT NULL,
                valor TEXT,
                fecha_modificacion TEXT
            )
        ''')
        
        # Tabla de Productos (ID manual)
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS productos (
                id INTEGER PRIMARY KEY,
                nombre TEXT NOT NULL,
                precio_unitario REAL NOT NULL,
                costo REAL NOT NULL,
                ganancia REAL,
                unidad_medida TEXT DEFAULT 'Pza',
                stock_estimado REAL DEFAULT 0,
                stock_minimo REAL DEFAULT 0,
                gestion_stock INTEGER DEFAULT 0,
                imagen TEXT,
                activo INTEGER DEFAULT 1,
                fecha_creacion TEXT
            )
        ''')
        
        # Tabla de Ingredientes (ID manual)
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS ingredientes (
                id INTEGER PRIMARY KEY,
                nombre TEXT NOT NULL,
                unidad_almacen TEXT DEFAULT 'Kg',
                costo_unitario REAL NOT NULL,
                cantidad_stock REAL DEFAULT 0,
                gestion_stock INTEGER DEFAULT 0,
                activo INTEGER DEFAULT 1,
                fecha_creacion TEXT
            )
        ''')
        
        # Tabla de Recetas (ID manual)
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS recetas (
                id INTEGER PRIMARY KEY,
                id_producto INTEGER NOT NULL,
                id_ingrediente INTEGER NOT NULL,
                cantidad_requerida REAL NOT NULL,
                unidad_porcionamiento TEXT DEFAULT 'Kg',
                FOREIGN KEY (id_producto) REFERENCES productos(id) ON DELETE CASCADE,
                FOREIGN KEY (id_ingrediente) REFERENCES ingredientes(id) ON DELETE CASCADE
            )
        ''')
        
        # Tabla de Ventas
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS ventas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                numero_venta INTEGER NOT NULL,
                fecha TEXT NOT NULL,
                producto TEXT NOT NULL,
                id_producto INTEGER,
                cantidad REAL NOT NULL,
                precio_unitario REAL NOT NULL,
                total REAL NOT NULL,
                metodo_pago TEXT DEFAULT 'Efectivo',
                mesa TEXT,
                propina REAL DEFAULT 0,
                FOREIGN KEY (id_producto) REFERENCES productos(id)
            )
        ''')
        
        # Tabla de Cortes
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS cortes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                numero_corte INTEGER NOT NULL,
                fecha TEXT NOT NULL,
                dinero_en_caja REAL NOT NULL,
                corte_final REAL NOT NULL,
                corte_esperado REAL NOT NULL,
                retiros REAL DEFAULT 0,
                diferencia REAL NOT NULL,
                estado TEXT,
                ganancias REAL NOT NULL
            )
        ''')
        
        # Tabla de Dinero en Caja
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS dinero_caja (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha TEXT NOT NULL,
                tipo TEXT NOT NULL,
                denominacion INTEGER NOT NULL,
                cantidad INTEGER NOT NULL,
                total REAL NOT NULL,
                tipo_registro TEXT DEFAULT 'apertura'
            )
        ''')
        
        # Tabla de Ventas Pendientes
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS ventas_pendientes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mesa TEXT NOT NULL,
                productos TEXT,
                total REAL DEFAULT 0,
                fecha_creacion TEXT
            )
        ''')
        
        self.conn.commit()
    
    def init_config(self):
        """Inicializa configuraciones por defecto"""
        configs = [
            ('gestion_stock_global', '0'),
            ('dinero_ingresado_hoy', '0'),
            ('ultimo_numero_venta', '0'),
            ('ultimo_numero_corte', '0')
        ]
        
        for clave, valor in configs:
            self.cursor.execute('''
                INSERT OR IGNORE INTO configuracion (clave, valor, fecha_modificacion)
                VALUES (?, ?, ?)
            ''', (clave, valor, datetime.now().strftime('%d/%m/%Y %H:%M:%S')))
        
        self.conn.commit()
    
    # ==================== CONFIGURACIÓN ====================
    
    def get_config(self, clave: str) -> Optional[str]:
        """Obtiene un valor de configuración"""
        self.cursor.execute('SELECT valor FROM configuracion WHERE clave = ?', (clave,))
        result = self.cursor.fetchone()
        return result['valor'] if result else None
    
    def set_config(self, clave: str, valor: str):
        """Establece un valor de configuración"""
        fecha = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        self.cursor.execute('''
            INSERT OR REPLACE INTO configuracion (clave, valor, fecha_modificacion)
            VALUES (?, ?, ?)
        ''', (clave, valor, fecha))
        self.conn.commit()
    
    def is_gestion_stock_active(self) -> bool:
        """Verifica si la gestión de stock está activa globalmente"""
        valor = self.get_config('gestion_stock_global')
        return valor == '1'
    
    def toggle_gestion_stock(self, activo: bool):
        """Activa/desactiva la gestión de stock global"""
        self.set_config('gestion_stock_global', '1' if activo else '0')
    
    def check_dinero_ingresado_hoy(self) -> bool:
        """Verifica si ya se ingresó el dinero en caja hoy"""
        fecha_hoy = datetime.now().strftime('%d/%m/%Y')
        fecha_guardada = self.get_config('dinero_ingresado_hoy')
        return fecha_guardada == fecha_hoy
    
    def mark_dinero_ingresado(self):
        """Marca que se ingresó el dinero en caja hoy"""
        fecha_hoy = datetime.now().strftime('%d/%m/%Y')
        self.set_config('dinero_ingresado_hoy', fecha_hoy)
    
    # ==================== VALIDACIÓN DE IDs ====================
    
    def id_exists(self, table: str, id_value: int) -> bool:
        """Verifica si un ID ya existe en una tabla"""
        self.cursor.execute(f'SELECT id FROM {table} WHERE id = ?', (id_value,))
        return self.cursor.fetchone() is not None
    
    def reorganize_ids(self, table: str):
        """Reorganiza los IDs de una tabla para que sean continuos"""
        # Obtener todos los registros ordenados por ID
        self.cursor.execute(f'SELECT * FROM {table} WHERE activo = 1 ORDER BY id')
        registros = [dict(row) for row in self.cursor.fetchall()]
        
        # Eliminar todos los registros
        self.cursor.execute(f'DELETE FROM {table}')
        
        # Reinsertar con IDs continuos
        for idx, registro in enumerate(registros, start=1):
            old_id = registro['id']
            registro['id'] = idx
            
            # Construir query de inserción
            columns = ', '.join(registro.keys())
            placeholders = ', '.join(['?' for _ in registro])
            values = list(registro.values())
            
            self.cursor.execute(f'INSERT INTO {table} ({columns}) VALUES ({placeholders})', values)
            
            # Actualizar referencias en otras tablas si es necesario
            if table == 'productos':
                self.cursor.execute('UPDATE recetas SET id_producto = ? WHERE id_producto = ?', (idx, old_id))
                self.cursor.execute('UPDATE ventas SET id_producto = ? WHERE id_producto = ?', (idx, old_id))
            elif table == 'ingredientes':
                self.cursor.execute('UPDATE recetas SET id_ingrediente = ? WHERE id_ingrediente = ?', (idx, old_id))
        
        self.conn.commit()
    
    # ==================== PRODUCTOS ====================
    
    def add_producto(self, id_producto: int, nombre: str, precio: float, costo: float, 
                     unidad: str = 'Pza', gestion_stock: bool = False,
                     stock_estimado: float = 0, stock_minimo: float = 0,
                     imagen: str = None) -> int:
        """Añade un nuevo producto con ID específico"""
        if self.id_exists('productos', id_producto):
            raise ValueError(f"El ID {id_producto} ya existe")
        
        ganancia = precio - costo
        fecha = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        
        self.cursor.execute('''
            INSERT INTO productos (id, nombre, precio_unitario, costo, ganancia, 
                                 unidad_medida, stock_estimado, stock_minimo,
                                 gestion_stock, imagen, fecha_creacion)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (id_producto, nombre, precio, costo, ganancia, unidad, stock_estimado, 
              stock_minimo, 1 if gestion_stock else 0, imagen, fecha))
        
        self.conn.commit()
        return id_producto
    
    def get_productos(self, activos_only: bool = True) -> List[Dict]:
        """Obtiene todos los productos"""
        query = 'SELECT * FROM productos'
        if activos_only:
            query += ' WHERE activo = 1'
        query += ' ORDER BY id'
        
        self.cursor.execute(query)
        return [dict(row) for row in self.cursor.fetchall()]
    
    def get_producto(self, id_producto: int) -> Optional[Dict]:
        """Obtiene un producto por ID"""
        self.cursor.execute('SELECT * FROM productos WHERE id = ?', (id_producto,))
        result = self.cursor.fetchone()
        return dict(result) if result else None
    
    def update_producto(self, old_id: int, new_id: int = None, **kwargs):
        """Actualiza un producto (permite cambiar el ID)"""
        # Si se quiere cambiar el ID
        if new_id and new_id != old_id:
            if self.id_exists('productos', new_id):
                raise ValueError(f"El ID {new_id} ya existe")
            
            # Actualizar referencias en recetas
            self.cursor.execute('UPDATE recetas SET id_producto = ? WHERE id_producto = ?', 
                              (new_id, old_id))
            
            # Actualizar referencias en ventas
            self.cursor.execute('UPDATE ventas SET id_producto = ? WHERE id_producto = ?', 
                              (new_id, old_id))
            
            kwargs['id'] = new_id
        
        # Recalcular ganancia si se actualiza precio o costo
        if 'precio_unitario' in kwargs or 'costo' in kwargs:
            producto = self.get_producto(old_id)
            precio = kwargs.get('precio_unitario', producto['precio_unitario'])
            costo = kwargs.get('costo', producto['costo'])
            kwargs['ganancia'] = precio - costo
        
        if kwargs:
            fields = ', '.join([f"{k} = ?" for k in kwargs.keys()])
            values = list(kwargs.values()) + [old_id]
            
            self.cursor.execute(f'UPDATE productos SET {fields} WHERE id = ?', values)
            self.conn.commit()
    
    def delete_producto(self, id_producto: int):
        """Elimina un producto y reorganiza los IDs"""
        self.cursor.execute('UPDATE productos SET activo = 0 WHERE id = ?', (id_producto,))
        self.conn.commit()
        
        # Reorganizar IDs para que sean continuos
        self.reorganize_ids('productos')
    
    def search_productos(self, query: str) -> List[Dict]:
        """Busca productos por nombre"""
        from utils import normalize_text
        normalized_query = normalize_text(query)
        
        self.cursor.execute('SELECT * FROM productos WHERE activo = 1')
        productos = [dict(row) for row in self.cursor.fetchall()]
        
        resultados = [p for p in productos 
                     if normalized_query in normalize_text(p['nombre'])]
        
        return resultados
    
    def get_next_producto_id(self) -> int:
        """Obtiene el siguiente ID disponible para productos"""
        self.cursor.execute('SELECT MAX(id) as max_id FROM productos')
        result = self.cursor.fetchone()
        max_id = result['max_id'] if result['max_id'] else 0
        return max_id + 1
    
    # ==================== INGREDIENTES ====================
    
    def add_ingrediente(self, id_ingrediente: int, nombre: str, costo_unitario: float,
                       unidad: str = 'Kg', cantidad: float = 0,
                       gestion_stock: bool = False) -> int:
        """Añade un nuevo ingrediente con ID específico"""
        if self.id_exists('ingredientes', id_ingrediente):
            raise ValueError(f"El ID {id_ingrediente} ya existe")
        
        fecha = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        
        self.cursor.execute('''
            INSERT INTO ingredientes (id, nombre, unidad_almacen, costo_unitario,
                                    cantidad_stock, gestion_stock, fecha_creacion)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (id_ingrediente, nombre, unidad, costo_unitario, cantidad, 
              1 if gestion_stock else 0, fecha))
        
        self.conn.commit()
        return id_ingrediente
    
    def get_ingredientes(self, activos_only: bool = True) -> List[Dict]:
        """Obtiene todos los ingredientes"""
        query = 'SELECT * FROM ingredientes'
        if activos_only:
            query += ' WHERE activo = 1'
        query += ' ORDER BY id'
        
        self.cursor.execute(query)
        return [dict(row) for row in self.cursor.fetchall()]
    
    def get_ingrediente(self, id_ingrediente: int) -> Optional[Dict]:
        """Obtiene un ingrediente por ID"""
        self.cursor.execute('SELECT * FROM ingredientes WHERE id = ?', (id_ingrediente,))
        result = self.cursor.fetchone()
        return dict(result) if result else None
    
    def update_ingrediente(self, old_id: int, new_id: int = None, **kwargs):
        """Actualiza un ingrediente (permite cambiar el ID)"""
        if new_id and new_id != old_id:
            if self.id_exists('ingredientes', new_id):
                raise ValueError(f"El ID {new_id} ya existe")
            
            # Actualizar referencias en recetas
            self.cursor.execute('UPDATE recetas SET id_ingrediente = ? WHERE id_ingrediente = ?', 
                              (new_id, old_id))
            
            kwargs['id'] = new_id
        
        if kwargs:
            fields = ', '.join([f"{k} = ?" for k in kwargs.keys()])
            values = list(kwargs.values()) + [old_id]
            
            self.cursor.execute(f'UPDATE ingredientes SET {fields} WHERE id = ?', values)
            self.conn.commit()
    
    def delete_ingrediente(self, id_ingrediente: int):
        """Elimina un ingrediente y reorganiza los IDs"""
        self.cursor.execute('UPDATE ingredientes SET activo = 0 WHERE id = ?', (id_ingrediente,))
        self.conn.commit()
        
        # Reorganizar IDs
        self.reorganize_ids('ingredientes')
    
    def registrar_compra_ingrediente(self, id_ingrediente: int, cantidad: float):
        """Registra una compra de ingrediente (suma al stock)"""
        self.cursor.execute('''
            UPDATE ingredientes 
            SET cantidad_stock = cantidad_stock + ?
            WHERE id = ?
        ''', (cantidad, id_ingrediente))
        self.conn.commit()
    
    def get_next_ingrediente_id(self) -> int:
        """Obtiene el siguiente ID disponible para ingredientes"""
        self.cursor.execute('SELECT MAX(id) as max_id FROM ingredientes')
        result = self.cursor.fetchone()
        max_id = result['max_id'] if result['max_id'] else 0
        return max_id + 1
    
    # ==================== RECETAS ====================
    
    def add_receta(self, id_receta: int, id_producto: int, id_ingrediente: int,
                   cantidad: float, unidad: str = 'Kg') -> int:
        """Añade una receta con ID específico"""
        if self.id_exists('recetas', id_receta):
            raise ValueError(f"El ID {id_receta} ya existe")
        
        self.cursor.execute('''
            INSERT INTO recetas (id, id_producto, id_ingrediente, cantidad_requerida,
                               unidad_porcionamiento)
            VALUES (?, ?, ?, ?, ?)
        ''', (id_receta, id_producto, id_ingrediente, cantidad, unidad))
        
        self.conn.commit()
        
        # Recalcular costo del producto
        self.recalcular_costo_producto(id_producto)
        
        return id_receta
    
    def get_recetas_producto(self, id_producto: int) -> List[Dict]:
        """Obtiene todas las recetas de un producto"""
        self.cursor.execute('''
            SELECT r.*, i.nombre as ingrediente_nombre, i.unidad_almacen,
                   i.costo_unitario, i.cantidad_stock
            FROM recetas r
            JOIN ingredientes i ON r.id_ingrediente = i.id
            WHERE r.id_producto = ? AND i.activo = 1
        ''', (id_producto,))
        return [dict(row) for row in self.cursor.fetchall()]
    
    def get_todas_recetas(self) -> List[Dict]:
        """Obtiene todas las recetas"""
        self.cursor.execute('''
            SELECT r.*, p.nombre as producto_nombre, i.nombre as ingrediente_nombre
            FROM recetas r
            JOIN productos p ON r.id_producto = p.id
            JOIN ingredientes i ON r.id_ingrediente = i.id
            WHERE p.activo = 1 AND i.activo = 1
            ORDER BY r.id
        ''')
        return [dict(row) for row in self.cursor.fetchall()]
    
    def get_receta(self, id_receta: int) -> Optional[Dict]:
        """Obtiene una receta por ID"""
        self.cursor.execute('''
            SELECT r.*, p.nombre as producto_nombre, i.nombre as ingrediente_nombre
            FROM recetas r
            JOIN productos p ON r.id_producto = p.id
            JOIN ingredientes i ON r.id_ingrediente = i.id
            WHERE r.id = ?
        ''', (id_receta,))
        result = self.cursor.fetchone()
        return dict(result) if result else None
    
    def update_receta(self, old_id: int, new_id: int = None, **kwargs):
        """Actualiza una receta (permite cambiar el ID)"""
        if new_id and new_id != old_id:
            if self.id_exists('recetas', new_id):
                raise ValueError(f"El ID {new_id} ya existe")
            kwargs['id'] = new_id
        
        if kwargs:
            fields = ', '.join([f"{k} = ?" for k in kwargs.keys()])
            values = list(kwargs.values()) + [old_id]
            
            self.cursor.execute(f'UPDATE recetas SET {fields} WHERE id = ?', values)
            self.conn.commit()
        
        # Recalcular costo del producto
        receta = self.get_receta(new_id if new_id else old_id)
        if receta:
            self.recalcular_costo_producto(receta['id_producto'])
    
    def delete_receta(self, id_receta: int):
        """Elimina una receta y reorganiza los IDs"""
        # Obtener el producto antes de eliminar
        receta = self.get_receta(id_receta)
        
        self.cursor.execute('DELETE FROM recetas WHERE id = ?', (id_receta,))
        self.conn.commit()
        
        # Reorganizar IDs
        self.reorganize_ids('recetas')
        
        # Recalcular costo del producto
        if receta:
            self.recalcular_costo_producto(receta['id_producto'])
    
    def get_next_receta_id(self) -> int:
        """Obtiene el siguiente ID disponible para recetas"""
        self.cursor.execute('SELECT MAX(id) as max_id FROM recetas')
        result = self.cursor.fetchone()
        max_id = result['max_id'] if result['max_id'] else 0
        return max_id + 1
    
    def recalcular_costo_producto(self, id_producto: int):
        """Recalcula el costo de un producto basado en sus recetas"""
        recetas = self.get_recetas_producto(id_producto)
        
        if not recetas:
            return
        
        costo_total = sum(r['cantidad_requerida'] * r['costo_unitario'] for r in recetas)
        
        self.cursor.execute('''
            UPDATE productos 
            SET costo = ?, ganancia = precio_unitario - ?
            WHERE id = ?
        ''', (costo_total, costo_total, id_producto))
        
        self.conn.commit()
    
    def calcular_stock_estimado(self, id_producto: int) -> float:
        """Calcula el stock estimado de un producto basado en sus ingredientes"""
        recetas = self.get_recetas_producto(id_producto)
        
        if not recetas:
            return 0
        
        capacidades = []
        for receta in recetas:
            if receta['cantidad_requerida'] > 0:
                capacidad = receta['cantidad_stock'] / receta['cantidad_requerida']
                capacidades.append(capacidad)
        
        return int(min(capacidades)) if capacidades else 0
    
    def actualizar_stock_estimado(self, id_producto: int):
        """Actualiza el stock estimado de un producto en la base de datos"""
        stock = self.calcular_stock_estimado(id_producto)
        self.cursor.execute('UPDATE productos SET stock_estimado = ? WHERE id = ?', 
                          (stock, id_producto))
        self.conn.commit()
    
    def actualizar_todos_stocks_estimados(self):
        """Actualiza el stock estimado de todos los productos con gestión de stock"""
        productos = self.cursor.execute('''
            SELECT id FROM productos WHERE gestion_stock = 1 AND activo = 1
        ''').fetchall()
        
        for producto in productos:
            self.actualizar_stock_estimado(producto['id'])
    
    # ==================== VENTAS ====================
    
    def descontar_inventario_por_venta(self, id_producto: int, cantidad_vendida: float):
        """Descuenta del inventario de ingredientes según la venta de un producto"""
        recetas = self.get_recetas_producto(id_producto)
        
        for receta in recetas:
            cantidad_a_descontar = receta['cantidad_requerida'] * cantidad_vendida
            
            self.cursor.execute('''
                UPDATE ingredientes
                SET cantidad_stock = cantidad_stock - ?
                WHERE id = ?
            ''', (cantidad_a_descontar, receta['id_ingrediente']))
        
        self.conn.commit()
        
        # Actualizar stock estimado del producto
        self.actualizar_stock_estimado(id_producto)

# Instancia global de la base de datos
db = Database()