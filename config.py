"""
Configuración global del sistema Mitsy's POS
"""

# Colores del tema (inspirado en las imágenes de referencia)
COLORS = {
    'bg_primary': '#E8E8E8',      # Fondo principal gris claro
    'bg_secondary': '#F5F5F5',    # Fondo secundario
    'button_bg': '#FFFFFF',       # Fondo de botones
    'button_hover': '#D0D0D0',    # Hover de botones
    'border': '#A0A0A0',          # Bordes
    'text_primary': '#2C2C2C',    # Texto principal
    'text_secondary': '#666666',  # Texto secundario
    'accent': '#4A90E2',          # Color de acento
    'success': '#4CAF50',         # Verde éxito
    'warning': '#FF9800',         # Naranja advertencia
    'danger': '#F44336',          # Rojo peligro
    'table_header': '#D9D9D9',    # Encabezado de tabla
    'table_row_even': '#FFFFFF',  # Fila par
    'table_row_odd': '#F9F9F9'    # Fila impar
}

# Fuentes
FONTS = {
    'title': ('Segoe UI', 24, 'bold'),
    'subtitle': ('Segoe UI', 18, 'bold'),
    'heading': ('Segoe UI', 14, 'bold'),
    'normal': ('Segoe UI', 11),
    'small': ('Segoe UI', 9),
    'button': ('Segoe UI', 11, 'bold')
}

# Configuración de ventanas
WINDOW_CONFIG = {
    'splash_duration': 2500,  # ms
    'min_width': 800,
    'min_height': 600
}

# Información del negocio (para tickets)
BUSINESS_INFO = {
    'name': "Mitsy's",
    'address': "C. Liverpool 379, Colonia Ejidal, Reyes Acozac",
    'city': "55757 San Lucas Xolox, Méx.",
    'phone': "713-137-4243"
}

# Denominaciones de dinero
DENOMINACIONES = {
    'billetes': [500, 200, 100, 50, 20],
    'monedas': [10, 5, 2, 1]
}

# Configuración de punto de venta
MESAS = [f"Mesa {i}" for i in range(1, 7)] + ["Para llevar"]

# Información actualizada del negocio (para tickets)
BUSINESS_INFO = {
    'name': "Los Abuelos",
    'subtitle': "Antojitos Mexicanos",
    'address': "C. Liverpool 379, Colonia Ejidal, Reyes Acozac",
    'city': "55757 San Lucas Xolox, Méx.",
    'phone': "713-137-4243",
    'logo_path': "images/logo.png"  # Ruta al logo
}

# Configuración de tickets
TICKET_CONFIG = {
    'width_mm': 58,  # Ancho del ticket en mm
    'font_size_title': 12,
    'font_size_normal': 9,
    'font_size_small': 7,
    'line_spacing': 1.2
}