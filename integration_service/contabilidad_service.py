"""
Servicio de Exportación Contabilidad
Reutiliza la misma API de ExportService
"""
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from django.conf import settings
from .export_service import ExportService
from .contabilidad_models import ContabilidadExportHistory

logger = logging.getLogger(__name__)


class ContabilidadExportService(ExportService):
    """
    Servicio de Exportación Contabilidad
    Hereda de ExportService para reutilizar API y agregar funcionalidades contables
    """
    
    def __init__(self):
        super().__init__()
        self.export_dir = Path(settings.MEDIA_ROOT) / 'contabilidad_exports'
        self.export_dir.mkdir(exist_ok=True)
    
    def export_to_excel_contabilidad(self, data, filename, config):
        """
        Exportar a Excel con reglas contables
        """
        logger.info(f"🔥 Exportación Contabilidad Excel: {len(data)} registros")
        
        # Aplicar reglas contables específicas
        processed_data = self._apply_contabilidad_rules(data, config)
        
        # Reutilizar el método export_to_excel del padre
        return self.export_to_excel(processed_data, filename, config)
    
    def export_to_pdf_factura(self, data, filename, config):
        """
        Exportar a PDF para facturas
        """
        logger.info(f"🔥 Exportación PDF Factura: {len(data)} registros")
        
        # TODO: Implementar exportación a PDF
        # Por ahora exportamos a Excel
        return self.export_to_excel_contabilidad(data, filename, config)
    
    def _apply_contabilidad_rules(self, data, config):
        """
        Aplicar reglas específicas de contabilidad
        """
        processed_data = []
        
        for record in data:
            processed_record = record.copy()
            
            # 🔥 AGREGAR COLUMNAS CALCULADAS
            self._apply_calculated_columns(processed_record)
            
            # Aplicar reglas de facturación si existen
            if config.get('billing_info'):
                self._apply_billing_rules(processed_record, config)
            
            # Aplicar reglas de impuestos si existen
            if config.get('tax_rules'):
                self._apply_tax_rules(processed_record, config)
            
            processed_data.append(processed_record)
        
        return processed_data
    
    def _calcular_tarifa(self, record):
        """
        Calcular tarifa según COD y RANGO DE DÍAS
        """
        cod = record.get('COD', '')
        rango_dias = record.get('RANGO DE DÍAS', '')
        
        # 🔥 Limpiar COD: tomar solo los primeros caracteres numéricos
        cod_limpio = cod.strip()
        if ' ' in cod_limpio:
            cod_limpio = cod_limpio.split()[0]  # Tomar primera parte
        
        # Tabla de tarifas
        tarifas = {
            '01': {'0-6 días': '$22.700', '7-10 días': '$22.700', '> 11 días': '$22.700'},
            '17': {'0-6 días': '$17.796', '7-10 días': '$17.796', '> 11 días': '$17.796'},
            '02': {'0-6 días': '$22.700', '7-10 días': '$22.700', '> 11 días': '$22.700'},
            '02-1': {'0-6 días': '$22.700', '7-10 días': '$22.700', '> 11 días': '$22.700'},
            '06': {'0-6 días': '$22.700', '7-10 días': '$22.700', '> 11 días': '$22.700'},
            '05': {'0-6 días': '$22.700', '7-10 días': '$22.700', '> 11 días': '$22.700'},
            '04': {'0-6 días': '$22.700', '7-10 días': '$22.700', '> 11 días': '$22.700'},
            '15': {'0-6 días': '$8.700', '7-10 días': '$8.700', '> 11 días': '$8.700'},
            '03': {'0-6 días': '$22.700', '7-10 días': '$22.700', '> 11 días': '$22.700'}
        }
        
        # 🔥 DEBUG: Mostrar valores originales y limpios
        print(f"🔥 DEBUG - COD original: '{cod}' | COD limpio: '{cod_limpio}' | RANGO: '{rango_dias}'")
        
        # Buscar tarifa
        if cod_limpio in tarifas and rango_dias in tarifas[cod_limpio]:
            tarifa = tarifas[cod_limpio][rango_dias]
            record['TARIFA'] = tarifa
            print(f"🔥 DEBUG - Tarifa encontrada: {tarifa}")
        else:
            # Si no encuentra, dejar vacío o valor por defecto
            record['TARIFA'] = '$0.000'
            print(f"🔥 DEBUG - Tarifa NO encontrada, usando $0.000")
    
    def _apply_calculated_columns(self, record):
        """
        Aplicar columnas calculadas específicas
        """
        from datetime import datetime, timedelta
        
        try:
            # 📅 TOMAR FECHA DE INGRESO de la columna
            fecha_ingreso_str = record.get('FECHA DE INGRESO', '')
            # 📅 TOMAR FECHA DE ENTREGA de la columna
            fecha_entrega_str = record.get('FECHA DE ENTREGA', '')
            
            if fecha_ingreso_str and fecha_entrega_str:
                # Parsear fechas simples
                fecha_ingreso = datetime.strptime(fecha_ingreso_str, '%Y-%m-%d')
                fecha_entrega = datetime.strptime(fecha_entrega_str, '%Y-%m-%d')
                
                # 🧮 CÁLCULO DE DÍAS HÁBILES (primer día = 0)
                dias_habiles = 0
                fecha_actual = fecha_ingreso + timedelta(days=1)  # Empezar desde el día siguiente
                
                while fecha_actual <= fecha_entrega:
                    # Solo contar lunes a viernes (0-4)
                    if fecha_actual.weekday() < 5:
                        dias_habiles += 1
                    
                    fecha_actual += timedelta(days=1)
                
                # 📊 RANGO DE DÍAS (calcular primero)
                if dias_habiles <= 6:
                    rango = "0-6 días"
                elif dias_habiles <= 10:
                    rango = "7-10 días"
                else:
                    rango = "> 11 días"
                
                
                record['RANGO DE DÍAS'] = rango
                
                # 🧮 CÁLCULO DE TARIFA según COD y RANGO DE DÍAS (después del rango)
                self._calcular_tarifa(record)
                
                # Guardar resultado
                record['DÍAS'] = dias_habiles
                
            else:
                # Si no hay fechas, dejar valores vacíos
                record['DÍAS'] = 0
                record['RANGO DE DÍAS'] = ''
            
            # 📅 FECHA DE RADICACIÓN (usar la misma fecha de ingreso)
            if fecha_ingreso_str:
                record['FECHA DE RADICACIÓN'] = fecha_ingreso_str
            else:
                record['FECHA DE RADICACIÓN'] = ''
            
        except Exception as e:
            print(f"❌ Error calculando días hábiles: {e}")
            record['DÍAS'] = 0
            record['RANGO DE DÍAS'] = ''
            record['FECHA DE RADICACIÓN'] = ''
    
    def _calcular_dias_habiles_colombia(self, fecha_inicio, fecha_fin):
        """
        Calcular días hábiles entre dos fechas (sin sábados, domingos)
        """
        dias_habiles = 0
        fecha_actual = fecha_inicio
        
        # 🔥 DEBUG: Mostrar rango completo
        logger.info(f"🔍 DEBUG - Calculando del {fecha_inicio} al {fecha_fin}")
        
        while fecha_actual <= fecha_fin:
            weekday = fecha_actual.weekday()
            
            # 🔥 DEBUG: Mostrar cada día
            logger.info(f"🔍 DEBUG - Día: {fecha_actual} ({fecha_actual.strftime('%A')}) weekday:{weekday}")
            
            # Verificar si es día hábil (Lunes=0, Martes=1, Miércoles=2, Jueves=3, Viernes=4)
            if weekday < 5:  # Lunes a Viernes (0-4)
                dias_habiles += 1
                logger.info(f"🔍 DEBUG - ✅ Día hábil contado: {dias_habiles}")
            else:
                logger.info(f"� DEBUG - ❌ No es día hábil (fin de semana)")
            
            fecha_actual += timedelta(days=1)
        
        logger.info(f"🔍 DEBUG - Total días hábiles: {dias_habiles}")
        return dias_habiles
    
    def _calcular_dias_habiles(self, fecha_inicio, fecha_fin):
        """
        Calcular días hábiles entre dos fechas (sin sábados, domingos y festivos)
        """
        import holidays
        
        # Festivos de Colombia
        co_holidays = holidays.CO()
        
        dias_habiles = 0
        fecha_actual = fecha_inicio
        
        while fecha_actual <= fecha_fin:
            # Verificar si es día hábil
            if (fecha_actual.weekday() < 5 and  # Lunes a Viernes (0-4)
                fecha_actual not in co_holidays):  # No es festivo
                dias_habiles += 1
            
            fecha_actual += timedelta(days=1)
        
        return dias_habiles
    
    def _apply_billing_rules(self, record, config):
        """
        Aplicar reglas de facturación
        """
        billing_info = config.get('billing_info', {})
        
        # Agregar información de facturación
        if 'invoice_number' in billing_info:
            record['NUMERO_FACTURA'] = billing_info['invoice_number']
        
        if 'due_date' in billing_info:
            record['FECHA_VENCIMIENTO'] = billing_info['due_date']
        
        if 'payment_terms' in billing_info:
            record['CONDICIONES_PAGO'] = billing_info['payment_terms']
    
    def _apply_tax_rules(self, record, config):
        """
        Aplicar reglas de impuestos
        """
        tax_rules = config.get('tax_rules', {})
        
        # Calcular impuestos si hay monto
        if 'amount' in record and 'tax_rate' in tax_rules:
            amount = float(record.get('amount', 0))
            tax_rate = float(tax_rules['tax_rate'])
            
            tax_amount = amount * (tax_rate / 100)
            total_amount = amount + tax_amount
            
            record['IMPUESTO'] = round(tax_amount, 2)
            record['TOTAL_CON_IMPUESTO'] = round(total_amount, 2)
    
    def generate_contabilidad_filename(self, client_code, export_format):
        """
        Generar nombre de archivo para contabilidad
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"{client_code}_contabilidad_{timestamp}.{export_format}"
    
    def save_contabilidad_history(self, export_config, filename, filepath, record_count, filters_used=None):
        """
        Guardar en historial de exportación contabilidad
        """
        ContabilidadExportHistory.objects.create(
            export_config=export_config,
            filename=filename,
            file_path=filepath,
            record_count=record_count,
            filters_used=filters_used or {}
        )
        logger.info(f"📝 Historial contabilidad guardado: {filename}")
