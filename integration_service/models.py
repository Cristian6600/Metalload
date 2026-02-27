from django.db import models
from django.core.validators import FileExtensionValidator
import uuid
import os
import json


def upload_to(instance, filename):
    """Genera ruta única para archivos subidos"""
    ext = filename.split('.')[-1]
    filename = f"{uuid.uuid4()}.{ext}"
    return os.path.join('client_files', instance.client_code, filename)


class ClientFile(models.Model):
    """Archivos recibidos de clientes"""
    STATUS_CHOICES = [
        ('pending', 'Pendiente de procesar'),
        ('processing', 'Procesando'),
        ('processed', 'Procesado'),
        ('error', 'Error'),
        ('failed', 'Fallido'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    client_code = models.CharField(max_length=50, verbose_name="Código Cliente")
    file = models.FileField(
        upload_to=upload_to,
        validators=[FileExtensionValidator(allowed_extensions=['csv', 'xlsx', 'xls', 'txt'])],
        verbose_name="Archivo"
    )
    original_filename = models.CharField(max_length=255, verbose_name="Nombre Original")
    file_size = models.PositiveIntegerField(verbose_name="Tamaño (bytes)")
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    uploaded_at = models.DateTimeField(auto_now_add=True, verbose_name="Fecha de Subida")
    processed_at = models.DateTimeField(null=True, blank=True, verbose_name="Fecha de Procesamiento")
    error_message = models.TextField(null=True, blank=True, verbose_name="Mensaje de Error")
    
    class Meta:
        db_table = 'integration_client_files'
        verbose_name = "Archivo de Cliente"
        verbose_name_plural = "Archivos de Clientes"
        ordering = ['-uploaded_at']
    
    def __str__(self):
        return f"{self.client_code} - {self.original_filename}"


class ProcessingLog(models.Model):
    """Log de procesamiento de archivos"""
    LOG_LEVELS = [
        ('INFO', 'Información'),
        ('WARNING', 'Advertencia'),
        ('ERROR', 'Error'),
        ('DEBUG', 'Depuración'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    client_file = models.ForeignKey(ClientFile, on_delete=models.CASCADE, related_name='logs')
    level = models.CharField(max_length=10, choices=LOG_LEVELS)
    message = models.TextField()
    details = models.JSONField(null=True, blank=True, verbose_name="Detalles Adicionales")
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'integration_processing_logs'
        verbose_name = "Log de Procesamiento"
        verbose_name_plural = "Logs de Procesamiento"
        ordering = ['-created_at']


class ClientMapping(models.Model):
    """Configuración de mapeo por cliente"""
    client_code = models.CharField(max_length=50, unique=True, verbose_name="Código Cliente")
    mapping_config = models.JSONField(verbose_name="Configuración de Mapeo")
    validation_rules = models.JSONField(null=True, blank=True, verbose_name="Reglas de Validación")
    is_active = models.BooleanField(default=True, verbose_name="Activo")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'integration_client_mappings'
        verbose_name = "Mapeo de Cliente"
        verbose_name_plural = "Mapeos de Clientes"
    
    def __str__(self):
        return f"Mapeo - {self.client_code}"


class Report(models.Model):
    """Reportes generados"""
    REPORT_TYPES = [
        ('processing_summary', 'Resumen de Procesamiento'),
        ('client_errors', 'Errores por Cliente'),
        ('daily_summary', 'Resumen Diario'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    report_type = models.CharField(max_length=20, choices=REPORT_TYPES)
    title = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True)
    data = models.JSONField(verbose_name="Datos del Reporte")
    file_path = models.CharField(max_length=500, null=True, blank=True)
    generated_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'integration_reports'
        verbose_name = "Reporte"
        verbose_name_plural = "Reportes"
        ordering = ['-generated_at']
    
    def __str__(self):
        return f"{self.title} - {self.generated_at.strftime('%Y-%m-%d %H:%M')}"


class ExportConfig(models.Model):
    """Configuración de exportación por cliente"""
    EXPORT_FORMATS = [
        ('xlsx', 'Excel (.xlsx)'),
        ('csv', 'CSV plano'),
        ('txt', 'Texto plano'),
        ('json', 'JSON'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    client_code = models.CharField(max_length=50, unique=True, verbose_name="Código Cliente")
    client_name = models.CharField(max_length=100, verbose_name="Nombre del Cliente")
    description = models.TextField(blank=True, verbose_name="Descripción")
    
    # Configuración de columnas
    column_mapping = models.JSONField(
        verbose_name="Mapeo de Columnas",
        help_text="Mapeo de columnas Excel a campos de la API (ej: {'nombre_completo': 'nombre'})"
    )
    column_order = models.JSONField(
        verbose_name="Orden de Columnas",
        help_text="Orden en que aparecerán las columnas en el Excel"
    )
    
    # Configuración de formato
    export_format = models.CharField(
        max_length=10, 
        choices=EXPORT_FORMATS, 
        default='xlsx',
        verbose_name="Formato de Exportación"
    )
    excel_config = models.JSONField(
        default=dict,
        blank=True,
        verbose_name="Configuración Excel",
        help_text="Configuración específica para Excel (headers, estilos, etc)"
    )
    
    # Filtros y transformaciones
    default_filters = models.JSONField(
        default=dict,
        blank=True,
        verbose_name="Filtros por Defecto",
        help_text="Filtros que se aplican automáticamente (ej: {'id_clie': 3})"
    )
    transformations = models.JSONField(
        default=dict,
        blank=True,
        verbose_name="Transformaciones",
        help_text="Reglas de transformación de datos (ej: {'ciudad': 'left_pad_5'})"
    )
    
    is_active = models.BooleanField(default=True, verbose_name="Activo")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'integration_export_configs'
        verbose_name = "Configuración de Exportación"
        verbose_name_plural = "Configuraciones de Exportación"
        ordering = ['client_code']
    
    def __str__(self):
        return f"{self.client_code} - {self.client_name}"


class ExportHistory(models.Model):
    """Historial de exportaciones realizadas"""
    STATUS_CHOICES = [
        ('completed', 'Completado'),
        ('failed', 'Fallido'),
        ('cancelled', 'Cancelado'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    export_config = models.ForeignKey(
        ExportConfig, 
        on_delete=models.CASCADE, 
        related_name='exports',
        verbose_name="Configuración"
    )
    
    # Parámetros de exportación
    filters_used = models.JSONField(
        default=dict,
        verbose_name="Filtros Aplicados"
    )
    record_count = models.PositiveIntegerField(
        default=0,
        verbose_name="Número de Registros"
    )
    
    # Archivo generado
    filename = models.CharField(
        max_length=255,
        verbose_name="Nombre del Archivo"
    )
    file_path = models.CharField(
        max_length=500,
        verbose_name="Ruta del Archivo"
    )
    file_size = models.PositiveIntegerField(
        default=0,
        verbose_name="Tamaño (bytes)"
    )
    
    # Estado y fechas
    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default='completed',
        verbose_name="Estado"
    )
    error_message = models.TextField(
        blank=True,
        null=True,
        verbose_name="Mensaje de Error"
    )
    exported_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name="Fecha de Exportación"
    )
    
    class Meta:
        db_table = 'integration_export_history'
        verbose_name = "Historial de Exportación"
        verbose_name_plural = "Historial de Exportaciones"
        ordering = ['-exported_at']
    
    def __str__(self):
        return f"{self.export_config.client_code} - {self.exported_at.strftime('%Y-%m-%d %H:%M')}"
