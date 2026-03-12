"""
Modelos para Exportación Contabilidad
"""
from django.db import models
from django.utils import timezone
import uuid


class ContabilidadExportConfig(models.Model):
    """
    Configuración de Exportación Contabilidad
    Para informes personalizados de facturación
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    client_code = models.CharField(
        max_length=50, 
        unique=True,
        verbose_name="Código Cliente"
    )
    client_name = models.CharField(
        max_length=200,
        verbose_name="Nombre Cliente"
    )
    report_name = models.CharField(
        max_length=200,
        verbose_name="Nombre del Reporte"
    )
    
    # Mapeo y configuración (reutilizado de ExportConfig)
    column_mapping = models.JSONField(
        default=dict,
        verbose_name="Mapeo de Columnas"
    )
    column_order = models.JSONField(
        default=list,
        verbose_name="Orden de Columnas"
    )
    export_format = models.CharField(
        max_length=10,
        choices=[
            ('xlsx', 'Excel'),
            ('csv', 'CSV'),
            ('json', 'JSON'),
            ('pdf', 'PDF'),  # Nuevo para facturas
        ],
        default='xlsx',
        verbose_name="Formato de Exportación"
    )
    
    # Filtros y transformaciones
    default_filters = models.JSONField(
        default=dict,
        blank=True,
        verbose_name="Filtros por Defecto",
        help_text="Filtros que se aplican automáticamente (ej: {'id_clie': 17, 'motivo_operacion': 'Entregado', 'pub_date': '2024-01-01'})"
    )
    transformations = models.JSONField(
        default=dict,
        blank=True,
        verbose_name="Transformaciones",
        help_text="Reglas de transformación de datos (ej: {'ciudad': 'left_pad_5'})"
    )
    excel_config = models.JSONField(
        default=dict,
        blank=True,
        verbose_name="Configuración Excel"
    )
    
    # Campos específicos de contabilidad
    invoice_template = models.CharField(
        max_length=100,
        default='standard',
        verbose_name="Plantilla de Factura"
    )
    tax_rules = models.JSONField(
        default=dict,
        blank=True,
        verbose_name="Reglas de Impuestos"
    )
    billing_info = models.JSONField(
        default=dict,
        blank=True,
        verbose_name="Información de Facturación"
    )
    
    # Control
    is_active = models.BooleanField(default=True, verbose_name="Activo")
    created_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name="Creado"
    )
    updated_at = models.DateTimeField(
        auto_now=True,
        verbose_name="Actualizado"
    )
    
    class Meta:
        verbose_name = "Configuración Exportación Contabilidad"
        verbose_name_plural = "Configuraciones Exportación Contabilidad"
        ordering = ['client_code']
    
    def __str__(self):
        return f"{self.client_code} - {self.report_name}"


class ContabilidadExportHistory(models.Model):
    """
    Historial de Exportaciones Contabilidad
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    export_config = models.ForeignKey(
        ContabilidadExportConfig,
        on_delete=models.CASCADE,
        related_name='contabilidad_exports'
    )
    filename = models.CharField(max_length=255, verbose_name="Nombre Archivo")
    file_path = models.CharField(max_length=500, verbose_name="Ruta Archivo")
    record_count = models.IntegerField(verbose_name="Cantidad Registros")
    filters_used = models.JSONField(default=dict, verbose_name="Filtros Usados")
    exported_at = models.DateTimeField(auto_now_add=True, verbose_name="Exportado")
    
    class Meta:
        verbose_name = "Historial Exportación Contabilidad"
        verbose_name_plural = "Historial Exportaciones Contabilidad"
        ordering = ['-exported_at']
    
    def __str__(self):
        return f"{self.export_config.client_code} - {self.filename}"
