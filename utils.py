"""
Funciones auxiliares y utilidades generales
"""
import re
from datetime import datetime
from typing import Optional
import unicodedata

def format_currency(amount: float) -> str:
    """
    Formatea un número como moneda mexicana
    Ejemplo: 1234.56 -> $1,234.56
    """
    if amount is None:
        return "$0.00"
    return f"${amount:,.2f}"

def format_number(number: float, decimals: int = 2) -> str:
    """
    Formatea un número con separadores de miles
    Ejemplo: 1234.56 -> 1,234.56
    """
    if number is None:
        return "0.00"
    return f"{number:,.{decimals}f}"

def parse_currency(text: str) -> float:
    """
    Convierte texto de moneda a float
    Ejemplo: "$1,234.56" -> 1234.56
    """
    if not text:
        return 0.0
    # Remover $, comas y espacios
    clean = re.sub(r'[$,\s]', '', text)
    try:
        return float(clean)
    except ValueError:
        return 0.0

def format_datetime(dt: Optional[datetime] = None) -> str:
    """
    Formatea fecha y hora según el formato requerido
    Formato: dd/mm/yyyy hh:mm:ss
    """
    if dt is None:
        dt = datetime.now()
    return dt.strftime('%d/%m/%Y %H:%M:%S')

def format_date(dt: Optional[datetime] = None) -> str:
    """
    Formatea solo la fecha
    Formato: dd/mm/yyyy
    """
    if dt is None:
        dt = datetime.now()
    return dt.strftime('%d/%m/%Y')

def parse_datetime(date_str: str) -> Optional[datetime]:
    """
    Convierte string a datetime
    """
    try:
        return datetime.strptime(date_str, '%d/%m/%Y %H:%M:%S')
    except:
        try:
            return datetime.strptime(date_str, '%d/%m/%Y')
        except:
            return None

def normalize_text(text: str) -> str:
    """
    Normaliza texto removiendo acentos y convirtiendo a minúsculas
    Para búsquedas que ignoran acentos
    """
    if not text:
        return ""
    # Remover acentos
    nfkd = unicodedata.normalize('NFKD', text)
    text_sin_acentos = ''.join([c for c in nfkd if not unicodedata.combining(c)])
    return text_sin_acentos.lower()

def validate_float(value: str) -> bool:
    """
    Valida si un string puede convertirse a float
    """
    try:
        float(value)
        return True
    except ValueError:
        return False

def validate_int(value: str) -> bool:
    """
    Valida si un string puede convertirse a int
    """
    try:
        int(value)
        return True
    except ValueError:
        return False

def get_current_date() -> str:
    """
    Obtiene la fecha actual en formato dd/mm/yyyy
    """
    return datetime.now().strftime('%d/%m/%Y')

def get_current_datetime() -> str:
    """
    Obtiene la fecha y hora actual en formato dd/mm/yyyy hh:mm:ss
    """
    return datetime.now().strftime('%d/%m/%Y %H:%M:%S')

def calculate_week_range(date: datetime = None) -> tuple:
    """
    Calcula el rango de la semana (viernes a miércoles)
    según la lógica especificada
    """
    if date is None:
        date = datetime.now()
    
    # Obtener día de la semana (0=Lunes, 6=Domingo)
    weekday = date.weekday()
    
    # Calcular días hasta el viernes anterior
    # Viernes = 4
    if weekday >= 4:  # Viernes (4), Sábado (5), Domingo (6)
        days_since_friday = weekday - 4
    else:  # Lunes (0), Martes (1), Miércoles (2), Jueves (3)
        days_since_friday = weekday + 3
    
    # Viernes de la semana
    from datetime import timedelta
    friday = date - timedelta(days=days_since_friday)
    
    # Si hoy es jueves o antes, el miércoles es hoy o pasado
    if weekday <= 2:  # Lunes, Martes, Miércoles
        wednesday = date if weekday == 2 else date - timedelta(days=weekday + 5)
    else:  # Jueves, Viernes, Sábado, Domingo
        wednesday = friday + timedelta(days=5)
    
    return (friday.replace(hour=0, minute=0, second=0, microsecond=0),
            wednesday.replace(hour=23, minute=59, second=59, microsecond=999999))

def calculate_month_range(date: datetime = None) -> tuple:
    """
    Calcula el rango del mes (día 1 hasta hoy)
    """
    if date is None:
        date = datetime.now()
    
    first_day = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_day = date.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    return (first_day, last_day)