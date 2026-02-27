from datetime import datetime, date
import logging
import os
import uuid
from pathlib import Path
import requests
import pandas as pd
from django.conf import settings
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils.dataframe import dataframe_to_rows
import logging

logger = logging.getLogger(__name__)


class ExportService:
    """Servicio de exportación de datos de clientes"""
    
    def __init__(self):
        self.api_base_url = getattr(settings, 'MAIN_API_BASE_URL', 'https://diotest.letran.com.co')
        self.export_dir = Path(settings.MEDIA_ROOT) / 'exports'
        self.export_dir.mkdir(exist_ok=True)
    
    def fetch_client_data(self, client_id: int) -> dict:
        """
        Obtiene datos de clientes desde la API externa
        
        Args:
            client_id: ID del cliente a consultar
            
        Returns:
            dict: Respuesta de la API
        """
        try:
            url = f"{self.api_base_url}/clientes/export/?id_clie={client_id}"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Datos obtenidos para cliente {client_id}: {len(data)} registros")
                return data
            else:
                logger.error(f"Error en API: {response.status_code}")
                return []
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error conectando con API: {e}")
            return []
        except Exception as e:
            logger.error(f"Error inesperado obteniendo datos: {e}")
            return []
    
    def apply_transformations(self, data: list, transformations: dict) -> list:
        """
        Aplica transformaciones a los datos
        
        Args:
            data: Lista de registros
            transformations: Diccionario de reglas de transformación
            
        Returns:
            list: Datos transformados
        """
        transformed_data = []
        
        for record in data:
            new_record = record.copy()
            
            for field, rule in transformations.items():
                if field in new_record:
                    value = new_record[field]
                    
                    # Transformaciones comunes
                    if rule == 'left_pad_5' and isinstance(value, str):
                        new_record[field] = value.zfill(5)
                    elif rule == 'upper' and isinstance(value, str):
                        new_record[field] = value.upper()
                    elif rule == 'lower' and isinstance(value, str):
                        new_record[field] = value.lower()
                    elif rule == 'strip' and isinstance(value, str):
                        new_record[field] = value.strip()
            
            transformed_data.append(new_record)
        
        return transformed_data
    
    def map_columns(self, data: list, column_mapping: dict, column_order: list) -> list:
        """
        Mapea y ordena columnas según configuración
        Fuerza todas las columnas configuradas aunque no tengan datos
        Agrega fecha actual para columnas especiales
        
        Args:
            data: Lista de registros
            column_mapping: Mapeo columna_excel -> campo_api
            column_order: Orden de columnas
            
        Returns:
            list: Datos mapeados y ordenados
        """
        mapped_data = []
        
        # Obtener fecha actual
        fecha_actual = date.today().strftime('%Y-%m-%d')
        
        for record in data:
            mapped_record = {}
            
            # Aplicar mapeo: columna_excel -> campo_api
            for excel_col, api_field in column_mapping.items():
                if api_field in record and record[api_field] is not None:
                    mapped_record[excel_col] = record[api_field]
                else:
                    # Forzar columna aunque no tenga datos
                    mapped_record[excel_col] = ""
            
            # Casos especiales: agregar valores automáticos
            if "FECHA REPORTE FINAL" in column_order:
                mapped_record["FECHA REPORTE FINAL"] = fecha_actual
            
            # Aplicar orden e incluir TODAS las columnas
            ordered_record = {}
            for col in column_order:
                if col in mapped_record:
                    ordered_record[col] = mapped_record[col]
                else:
                    # Forzar columna en orden aunque no esté en mapeo
                    ordered_record[col] = ""
            
            mapped_data.append(ordered_record)
        
        return mapped_data
    
    def export_to_excel(self, data: list, filename: str, config: dict = None) -> str:
        """
        Exporta datos a Excel con formato personalizado
        
        Args:
            data: Lista de registros
            filename: Nombre del archivo
            config: Configuración de formato Excel
            
        Returns:
            str: Ruta del archivo generado
        """
        if not data:
            raise ValueError("No hay datos para exportar")
        
        # Convertir a DataFrame
        df = pd.DataFrame(data)
        
        # Crear archivo Excel
        filepath = self.export_dir / filename
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Datos')
            
            # Aplicar formato si hay configuración
            if config:
                self._apply_excel_formatting(writer.sheets['Datos'], config)
        
        logger.info(f"Excel generado: {filepath}")
        return str(filepath)
    
    def _apply_excel_formatting(self, worksheet, config: dict):
        """Aplica formato personalizado a hoja Excel"""
        # Configuración por defecto
        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
        header_alignment = Alignment(horizontal="center", vertical="center")
        
        # Aplicar formato a encabezados
        for cell in worksheet[1]:
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = header_alignment
        
        # Autoajustar ancho de columnas
        if config.get('auto_width', True):
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        # Congelar filas superiores
        if config.get('freeze_header', True):
            worksheet.freeze_panes = 'A2'
    
    def export_to_csv(self, data: list, filename: str, delimiter: str = ',') -> str:
        """
        Exporta datos a CSV
        
        Args:
            data: Lista de registros
            filename: Nombre del archivo
            delimiter: Delimitador CSV
            
        Returns:
            str: Ruta del archivo generado
        """
        if not data:
            raise ValueError("No hay datos para exportar")
        
        df = pd.DataFrame(data)
        filepath = self.export_dir / filename
        df.to_csv(filepath, index=False, sep=delimiter, encoding='utf-8')
        
        logger.info(f"CSV generado: {filepath}")
        return str(filepath)
    
    def export_to_json(self, data: list, filename: str) -> str:
        """
        Exporta datos a JSON
        
        Args:
            data: Lista de registros
            filename: Nombre del archivo
            
        Returns:
            str: Ruta del archivo generado
        """
        if not data:
            raise ValueError("No hay datos para exportar")
        
        import json
        filepath = self.export_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"JSON generado: {filepath}")
        return str(filepath)
    
    def generate_filename(self, client_code: str, export_format: str) -> str:
        """
        Genera nombre de archivo único
        
        Args:
            client_code: Código del cliente
            export_format: Formato de exportación
            
        Returns:
            str: Nombre de archivo generado
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"{client_code}_export_{timestamp}.{export_format}"
