"""
Módulo de generación de tickets para Mitsy's POS
"""
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import os
from datetime import datetime
from config import BUSINESS_INFO, TICKET_CONFIG
from utils import format_currency, get_current_datetime

class TicketGenerator:
    def __init__(self):
        self.width = TICKET_CONFIG['width_mm'] * mm
        self.margin = 2 * mm
        self.line_height = 3 * mm
        self.current_y = 0
        
    def generate_ticket_pdf(self, venta_data, filename=None):
        """
        Genera un ticket en PDF
        
        venta_data = {
            'numero_venta': 1,
            'fecha': '02/11/2025 19:14:30',
            'productos': [
                {'nombre': 'Tacos', 'cantidad': 2, 'precio': 15.00, 'total': 30.00},
                {'nombre': 'Coca-Cola', 'cantidad': 1, 'precio': 30.00, 'total': 30.00}
            ],
            'subtotal': 60.00,
            'propina': 5.00,
            'total': 60.00,
            'recibido': 100.00,
            'cambio': 40.00,
            'metodo_pago': 'Efectivo',
            'mesa': 'Mesa 1'
        }
        """
        
        # Crear nombre de archivo si no se proporciona
        if not filename:
            os.makedirs('tickets', exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'tickets/ticket_{venta_data["numero_venta"]}_{timestamp}.pdf'
        
        # Calcular altura necesaria
        estimated_height = self._estimate_height(venta_data)
        page_height = max(estimated_height, 100 * mm)
        
        # Crear canvas
        c = canvas.Canvas(filename, pagesize=(self.width, page_height))
        
        # Iniciar desde arriba
        self.current_y = page_height - (5 * mm)
        
        # Dibujar contenido
        self._draw_header(c, venta_data)
        self._draw_separator(c, dashed=False)
        self._draw_products(c, venta_data)
        self._draw_separator(c, dashed=True)
        self._draw_totals(c, venta_data)
        self._draw_separator(c, dashed=False)
        self._draw_footer(c)
        
        # Guardar PDF
        c.save()
        
        return filename
    
    def _estimate_height(self, venta_data):
        """Estima la altura necesaria para el ticket"""
        height = 30 * mm  # Header
        height += len(venta_data['productos']) * 6 * mm  # Productos
        height += 25 * mm  # Totales
        height += 15 * mm  # Footer
        return height
    
    def _draw_header(self, c, venta_data):
        """Dibuja el encabezado del ticket"""
        # Intentar cargar logo
        if os.path.exists(BUSINESS_INFO['logo_path']):
            try:
                logo_width = 25 * mm
                logo_height = 25 * mm
                x_pos = (self.width - logo_width) / 2
                
                c.drawImage(BUSINESS_INFO['logo_path'], 
                           x_pos, self.current_y - logo_height,
                           width=logo_width, height=logo_height,
                           preserveAspectRatio=True, mask='auto')
                
                self.current_y -= (logo_height + 2 * mm)
            except:
                # Si falla, mostrar texto
                self._draw_centered_text(c, BUSINESS_INFO['name'], 12, bold=True)
                self._draw_centered_text(c, BUSINESS_INFO['subtitle'], 9)
        else:
            # Sin logo, mostrar texto
            self._draw_centered_text(c, BUSINESS_INFO['name'], 12, bold=True)
            self._draw_centered_text(c, BUSINESS_INFO['subtitle'], 9)
        
        # Información del negocio
        self._draw_centered_text(c, BUSINESS_INFO['address'], 7)
        self._draw_centered_text(c, BUSINESS_INFO['city'], 7)
        self._draw_centered_text(c, f"Tel: {BUSINESS_INFO['phone']}", 7)
        
        self.current_y -= 2 * mm
        
        # Información del ticket
        self._draw_centered_text(c, f"Ticket #: {venta_data['numero_venta']}", 9, bold=True)
        self._draw_centered_text(c, f"Fecha: {venta_data['fecha']}", 7)
        
        if venta_data.get('mesa'):
            self._draw_centered_text(c, f"{venta_data['mesa']}", 8)
        
        self.current_y -= 2 * mm
    
    def _draw_separator(self, c, dashed=False):
        """Dibuja una línea separadora"""
        if dashed:
            c.setDash(1, 2)
        else:
            c.setDash()
        
        c.line(self.margin, self.current_y, self.width - self.margin, self.current_y)
        self.current_y -= 2 * mm
    
    def _draw_products(self, c, venta_data):
        """Dibuja la lista de productos"""
        self.current_y -= 1 * mm
        
        # Encabezado
        c.setFont("Helvetica-Bold", 8)
        c.drawString(self.margin, self.current_y, "Cant.")
        c.drawString(self.margin + 10 * mm, self.current_y, "Descripción")
        c.drawRightString(self.width - self.margin, self.current_y, "Total")
        self.current_y -= 3 * mm
        
        # Productos
        c.setFont("Helvetica", 8)
        for producto in venta_data['productos']:
            # Cantidad
            c.drawString(self.margin, self.current_y, str(int(producto['cantidad'])))
            
            # Nombre del producto
            nombre = producto['nombre']
            if len(nombre) > 18:
                nombre = nombre[:18] + "..."
            c.drawString(self.margin + 10 * mm, self.current_y, nombre)
            
            # Total
            c.drawRightString(self.width - self.margin, self.current_y, 
                            format_currency(producto['total']))
            
            self.current_y -= 4 * mm
            
            # Precio unitario (línea adicional más pequeña)
            c.setFont("Helvetica", 6)
            c.drawString(self.margin + 10 * mm, self.current_y, 
                        f"  @{format_currency(producto['precio'])} c/u")
            c.setFont("Helvetica", 8)
            self.current_y -= 3 * mm
        
        self.current_y -= 1 * mm
    
    def _draw_totals(self, c, venta_data):
        """Dibuja los totales"""
        self.current_y -= 1 * mm
        
        # Subtotal (si hay propina)
        if venta_data.get('propina', 0) > 0:
            c.setFont("Helvetica", 9)
            c.drawString(self.margin, self.current_y, "Subtotal:")
            c.drawRightString(self.width - self.margin, self.current_y, 
                            format_currency(venta_data['subtotal']))
            self.current_y -= 4 * mm
            
            # Propina
            c.drawString(self.margin, self.current_y, "Propina:")
            c.drawRightString(self.width - self.margin, self.current_y, 
                            format_currency(venta_data['propina']))
            self.current_y -= 4 * mm
        
        # Total
        c.setFont("Helvetica-Bold", 11)
        c.drawString(self.margin, self.current_y, "TOTAL:")
        c.drawRightString(self.width - self.margin, self.current_y, 
                        format_currency(venta_data['total']))
        self.current_y -= 5 * mm
        
        # Recibido
        c.setFont("Helvetica", 9)
        c.drawString(self.margin, self.current_y, "Recibido:")
        c.drawRightString(self.width - self.margin, self.current_y, 
                        format_currency(venta_data['recibido']))
        self.current_y -= 4 * mm
        
        # Cambio
        c.drawString(self.margin, self.current_y, "Cambio:")
        c.drawRightString(self.width - self.margin, self.current_y, 
                        format_currency(venta_data['cambio']))
        self.current_y -= 4 * mm
        
        # Método de pago
        c.setFont("Helvetica", 7)
        self._draw_centered_text_at(c, f"Método de pago: {venta_data['metodo_pago']}", 
                                    self.current_y, 7)
        self.current_y -= 3 * mm
    
    def _draw_footer(self, c):
        """Dibuja el pie del ticket"""
        self.current_y -= 2 * mm
        c.setFont("Helvetica-Bold", 9)
        self._draw_centered_text_at(c, "¡Gracias por su compra!", self.current_y, 9)
        self.current_y -= 3 * mm
        
        c.setFont("Helvetica", 8)
        self._draw_centered_text_at(c, "Vuelva pronto", self.current_y, 8)
    
    def _draw_centered_text(self, c, text, size, bold=False):
        """Dibuja texto centrado y actualiza current_y"""
        font = "Helvetica-Bold" if bold else "Helvetica"
        c.setFont(font, size)
        
        text_width = c.stringWidth(text, font, size)
        x = (self.width - text_width) / 2
        
        c.drawString(x, self.current_y, text)
        self.current_y -= (size * 0.5 * mm)
    
    def _draw_centered_text_at(self, c, text, y, size, bold=False):
        """Dibuja texto centrado en una posición Y específica"""
        font = "Helvetica-Bold" if bold else "Helvetica"
        c.setFont(font, size)
        
        text_width = c.stringWidth(text, font, size)
        x = (self.width - text_width) / 2
        
        c.drawString(x, y, text)
    
    def print_ticket(self, filename):
        """
        Imprime el ticket en una impresora térmica
        Usa el comando del sistema operativo por defecto
        """
        import platform
        import subprocess
        
        system = platform.system()
        
        try:
            if system == "Windows":
                os.startfile(filename, "print")
            elif system == "Darwin":  # macOS
                subprocess.run(["lpr", filename])
            else:  # Linux
                subprocess.run(["lp", filename])
            
            return True
        except Exception as e:
            print(f"Error al imprimir: {e}")
            return False


# Instancia global
ticket_generator = TicketGenerator()