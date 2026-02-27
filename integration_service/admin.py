from django.contrib import admin
from .models import ClientFile, ProcessingLog, ClientMapping, Report, ExportConfig, ExportHistory


@admin.register(ClientFile)
class ClientFileAdmin(admin.ModelAdmin):
    """Admin mejorado para carga automática de archivos de clientes"""
    list_display = [
        'client_code', 'original_filename', 'status', 
        'file_size_display', 'uploaded_at', 'processed_at', 'auto_process_button'
    ]
    list_filter = ['status', 'client_code', 'uploaded_at']
    search_fields = ['client_code', 'original_filename']
    readonly_fields = [
        'id', 'uploaded_at', 'processed_at', 'error_message', 'file_size_display'
    ]
    ordering = ['-uploaded_at']
    
    fieldsets = (
        ('Información del Archivo', {
            'fields': ('client_code', 'file', 'original_filename', 'file_size_display')
        }),
        ('Estado y Procesamiento', {
            'fields': ('status', 'processed_at', 'error_message'),
            'classes': ('collapse',)
        }),
        ('Metadatos', {
            'fields': ('id', 'uploaded_at'),
            'classes': ('collapse',)
        })
    )
    
    def save_model(self, request, obj, form, change):
        """Override mejorado para carga automática"""
        if not change:  # Solo al crear nuevo registro
            # Calcular tamaño del archivo
            if obj.file:
                obj.file_size = obj.file.size
            
            # Establecer nombre original si no está definido
            if not obj.original_filename and obj.file:
                obj.original_filename = obj.file.name
            
            # Marcar como pendiente para procesamiento automático
            obj.status = 'pending'
            
        super().save_model(request, obj, form, change)
        
        # Procesamiento automático después de guardar
        if not change and obj.status == 'pending':
            from .services import FileProcessor
            try:
                processor = FileProcessor()
                processor.process_file(obj)
                self.message_user(request, f'✅ Archivo "{obj.original_filename}" procesado automáticamente', level='SUCCESS')
            except Exception as e:
                self.message_user(request, f'❌ Error procesando archivo: {str(e)}', level='ERROR')
    
    def file_size_display(self, obj):
        """Formatea el tamaño del archivo"""
        if not obj.file_size:
            return "N/A"
        size = obj.file_size
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} TB"
    file_size_display.short_description = 'Tamaño'
    
    def auto_process_button(self, obj):
        """Botón para reprocesar archivos"""
        if obj.status in ['error', 'failed']:
            return f'''
                <a href="/admin/process-file/{obj.id}/" 
                   class="button" 
                   style="background-color: #f4a460; color: white; padding: 3px 8px; text-decoration: none; border-radius: 3px; font-size: 11px;">
                    🔄 Reprocesar
                </a>
            '''
        elif obj.status == 'completed':
            return f'''
                <span style="color: #28a745; font-weight: bold;">✅ Completado</span>
            '''
        elif obj.status == 'processing':
            return f'''
                <span style="color: #ffc107; font-weight: bold;">⏳ Procesando...</span>
            '''
        else:
            return f'''
                <span style="color: #6c757d;">⏸️ {obj.get_status_display()}</span>
            '''
    auto_process_button.short_description = 'Acción'
    auto_process_button.allow_tags = True
    
    actions = ['process_selected_files', 'reprocess_failed_files']
    
    def process_selected_files(self, request, queryset):
        """Procesar archivos seleccionados manualmente"""
        from .services import FileProcessor
        processed = 0
        errors = 0
        
        for file_obj in queryset.filter(status__in=['pending', 'error', 'failed']):
            try:
                processor = FileProcessor()
                processor.process_file(file_obj)
                processed += 1
            except Exception as e:
                errors += 1
        
        self.message_user(request, f'✅ {processed} archivos procesados, {errors} errores', level='SUCCESS')
    
    process_selected_files.short_description = '🔄 Procesar seleccionados'
    
    def reprocess_failed_files(self, request, queryset):
        """Reprocesar solo archivos con error"""
        from .services import FileProcessor
        reprocessed = 0
        
        for file_obj in queryset.filter(status__in=['error', 'failed']):
            try:
                processor = FileProcessor()
                processor.process_file(file_obj)
                reprocessed += 1
            except Exception as e:
                pass
        
        self.message_user(request, f'🔄 {reprocessed} archivos reprocesados', level='SUCCESS')
    
    reprocess_failed_files.short_description = '🔧 Reprocesar con errores'


@admin.register(ProcessingLog)
class ProcessingLogAdmin(admin.ModelAdmin):
    list_display = ['client_file', 'level', 'message_short', 'created_at']
    list_filter = ['level', 'created_at', 'client_file__client_code']
    search_fields = ['message', 'client_file__original_filename']
    readonly_fields = ['id', 'client_file', 'level', 'message', 'details', 'created_at']
    ordering = ['-created_at']
    
    def message_short(self, obj):
        """Muestra versión corta del mensaje"""
        return obj.message[:100] + '...' if len(obj.message) > 100 else obj.message
    message_short.short_description = 'Mensaje'


@admin.register(ClientMapping)
class ClientMappingAdmin(admin.ModelAdmin):
    list_display = ['client_code', 'is_active', 'created_at', 'updated_at']
    list_filter = ['is_active', 'created_at']
    search_fields = ['client_code']
    readonly_fields = ['created_at', 'updated_at']
    ordering = ['client_code']
    
    fieldsets = (
        ('Información Básica', {
            'fields': ('client_code', 'is_active')
        }),
        ('Configuración de Mapeo', {
            'fields': ('mapping_config',),
            'description': 'Define cómo se mapean los campos del archivo del cliente a los campos del sistema principal'
        }),
        ('Reglas de Validación', {
            'fields': ('validation_rules',),
            'description': 'Define las reglas de validación para los datos del cliente'
        }),
        ('Metadatos', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        })
    )


@admin.register(Report)
class ReportAdmin(admin.ModelAdmin):
    list_display = ['title', 'report_type', 'generated_at']
    list_filter = ['report_type', 'generated_at']
    search_fields = ['title', 'description']
    readonly_fields = ['id', 'data', 'generated_at']
    ordering = ['-generated_at']
    
    fieldsets = (
        ('Información del Reporte', {
            'fields': ('title', 'description', 'report_type')
        }),
        ('Datos', {
            'fields': ('data',),
            'description': 'Datos generados del reporte (JSON)'
        }),
        ('Metadatos', {
            'fields': ('generated_at',),
            'classes': ('collapse',)
        })
    )


@admin.register(ExportConfig)
class ExportConfigAdmin(admin.ModelAdmin):
    """Admin para configuraciones de exportación con botón de exportación"""
    list_display = [
        'client_code', 'client_name', 'export_format', 
        'is_active', 'export_count', 'created_at', 'export_button'
    ]
    list_filter = ['export_format', 'is_active', 'created_at']
    search_fields = ['client_code', 'client_name']
    readonly_fields = ['id', 'created_at', 'updated_at', 'export_count']
    ordering = ['client_code']
    
    fieldsets = (
        ('Información Básica', {
            'fields': ('client_code', 'client_name', 'description', 'is_active')
        }),
        ('Configuración de Columnas', {
            'fields': ('column_mapping', 'column_order'),
            'description': 'Define cómo se mapean y ordenan las columnas en el archivo exportado'
        }),
        ('Formato de Exportación', {
            'fields': ('export_format', 'excel_config'),
            'description': 'Configuración del formato de archivo y estilo Excel'
        }),
        ('Filtros y Transformaciones', {
            'fields': ('default_filters', 'transformations'),
            'description': 'Filtros automáticos y reglas de transformación de datos'
        }),
        ('Metadatos', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        })
    )
    
    actions = ['export_selected_configs']
    
    def export_count(self, obj):
        """Muestra número de exportaciones realizadas"""
        return obj.exports.count()
    export_count.short_description = 'Exportaciones'
    
    def export_button(self, obj):
        """Botón de exportación directa"""
        if obj.is_active:
            return f'''
                <a href="/admin/export/now/{obj.id}/" 
                   class="button" 
                   style="background-color: #417690; color: white; padding: 5px 10px; text-decoration: none; border-radius: 3px;">
                    📥 Exportar Ahora
                </a>
            '''
        return "Inactivo"
    export_button.short_description = 'Acción'
    export_button.allow_tags = True
    
    def export_selected_configs(self, request, queryset):
        """Acción para exportar configuraciones seleccionadas"""
        from django.http import HttpResponseRedirect
        from django.contrib import messages
        
        if queryset.count() == 1:
            config = queryset.first()
            return HttpResponseRedirect(f"/admin/export/now/{config.id}/")
        else:
            messages.warning(request, "Selecciona solo una configuración para exportar")
    
    export_selected_configs.short_description = 'Exportar seleccionada'


@admin.register(ExportHistory)
class ExportHistoryAdmin(admin.ModelAdmin):
    """Admin para historial de exportaciones"""
    list_display = [
        'export_config', 'filename', 'record_count', 
        'file_size_display', 'status', 'exported_at', 'download_button'
    ]
    list_filter = ['status', 'exported_at', 'export_config__client_code']
    search_fields = ['filename', 'export_config__client_code']
    readonly_fields = ['id', 'export_config', 'filters_used', 'record_count', 
                      'filename', 'file_path', 'file_size', 'status', 'exported_at']
    ordering = ['-exported_at']
    
    def file_size_display(self, obj):
        """Formatea el tamaño del archivo"""
        size = obj.file_size
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} TB"
    file_size_display.short_description = 'Tamaño'
    
    def download_button(self, obj):
        """Botón de descarga si el archivo existe"""
        if obj.status == 'completed' and obj.file_path:
            return f'''
                <a href="/admin/export/download/{obj.id}/" 
                   class="button" 
                   style="background-color: #417690; color: white; padding: 5px 10px; text-decoration: none; border-radius: 3px;">
                    📥 Descargar
                </a>
            '''
        return "No disponible"
    download_button.short_description = 'Descargar'
    download_button.allow_tags = True
