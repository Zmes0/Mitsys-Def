"""
Script para crear imagen placeholder
"""
from PIL import Image, ImageDraw, ImageFont
import os

# Crear carpeta images si no existe
os.makedirs('images', exist_ok=True)

# Crear imagen placeholder
img = Image.new('RGB', (200, 200), color='#D9D9D9')
draw = ImageDraw.Draw(img)

# Dibujar borde
draw.rectangle([10, 10, 190, 190], outline='#808080', width=3)

# Dibujar texto
try:
    font = ImageFont.truetype("arial.ttf", 20)
except:
    font = ImageFont.load_default()

text1 = "Sin Imagen"
text2 = "Producto"

# Centrar texto
bbox1 = draw.textbbox((0, 0), text1, font=font)
bbox2 = draw.textbbox((0, 0), text2, font=font)

w1 = bbox1[2] - bbox1[0]
w2 = bbox2[2] - bbox2[0]

draw.text((100 - w1//2, 80), text1, fill='#666666', font=font)
draw.text((100 - w2//2, 110), text2, fill='#666666', font=font)

# Guardar
img.save('images/placeholder.png')
print("✓ Placeholder creado en images/placeholder.png")