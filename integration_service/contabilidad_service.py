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
            
            # Aplicar reglas de facturación si existen
            if config.get('billing_info'):
                self._apply_billing_rules(processed_record, config)
            
            # Aplicar reglas de impuestos si existen
            if config.get('tax_rules'):
                self._apply_tax_rules(processed_record, config)
            
            processed_data.append(processed_record)
        
        return processed_data
    
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
