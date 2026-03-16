"""
Admin para Exportación Contabilidad
"""
from django.contrib import admin
from django.urls import path
from django.shortcuts import render
from django.http import JsonResponse
from django.utils.html import format_html
from django.contrib.admin import SimpleListFilter
from django import forms
import uuid

from ..contabilidad_models import ContabilidadExportConfig, ContabilidadExportHistory
from ..contabilidad_views import export_contabilidad_admin, get_contabilidad_exports_history


# 🔥 FILTRO PERSONALIZADO PARA FECHA DE ESTADO
class FechaEstadoDataFilter(SimpleListFilter):
    title = 'Fecha de Estado (Datos)'
    parameter_name = 'fecha_estado_data'
    
    def lookups(self, request, model_admin):
        return [
            ('hoy', 'Hoy'),
            ('ayer', 'Ayer'),
            ('ultima_semana', 'Última Semana'),
            ('ultimo_mes', 'Último Mes'),
            ('personalizado', 'Rango Personalizado'),
        ]
    
    def queryset(self, request, queryset):
        # Este filtro no filtra el queryset del admin
        # Solo agrega el parámetro a la URL para que la exportación lo use
        return queryset


@admin.register(ContabilidadExportConfig)
class ContabilidadExportConfigAdmin(admin.ModelAdmin):
    """
    Admin para Configuración de Exportación Contabilidad
    """
    list_display = [
        'client_code', 
        'client_name', 
        'report_name',
        'export_format',
        'is_active',
        'created_at',
        'export_button'
    ]
    list_filter = ['export_format', 'is_active', 'created_at', FechaEstadoDataFilter]
    search_fields = ['client_code', 'client_name', 'report_name']
    readonly_fields = ['id', 'created_at', 'updated_at']
    
    fieldsets = (
        ('Información Básica', {
            'fields': ('client_code', 'client_name', 'report_name', 'is_active')
        }),
        ('Configuración de Exportación', {
            'fields': ('export_format', 'column_mapping', 'column_order', 'default_filters')
        }),
        ('Configuración Específica Contabilidad', {
            'fields': ('excel_config', 'invoice_template', 'tax_rules', 'billing_info')
        }),
        ('Control', {
            'fields': ('id', 'created_at', 'updated_at'),
            'classes': ('collapse',)
        })
    )
    
    def export_button(self, obj):
        """Botón de exportación personalizado con selector de rango de fechas"""
        if obj.is_active and obj.default_filters.get('id_clie'):
            button_html = format_html('''
                <div style="display: flex; gap: 5px; flex-wrap: wrap;">
                    <a href="javascript:void(0);" 
                       onclick="showDateRangeModal('{}')"
                       class="button" 
                       style="background-color: #417690; color: white; padding: 5px 10px; text-decoration: none; border-radius:3px;">
                        📅 Exportar con Rango
                    </a>
                </div>
                
                <!-- Modal de Rango de Fechas -->
                <div id="dateRangeModal_{}" style="display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.5); z-index: 9999;">
                    <div style="position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); background: white; padding: 20px; border-radius: 8px; min-width: 400px;">
                        <h3 style="margin-top: 0;">📅 Seleccionar Rango de Fechas</h3>
                        
                        <div style="margin-bottom: 15px;">
                            <label style="display: block; margin-bottom: 5px;"><strong>Fecha Inicio:</strong></label>
                            <input type="date" id="fecha_inicio_{}" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                        </div>
                        
                        <div style="margin-bottom: 15px;">
                            <label style="display: block; margin-bottom: 5px;"><strong>Fecha Fin:</strong></label>
                            <input type="date" id="fecha_fin_{}" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                        </div>
                        
                        <div style="display: flex; gap: 10px; justify-content: flex-end;">
                            <button onclick="closeDateRangeModal('{}')" style="padding: 8px 16px; border: 1px solid #ddd; background: #f5f5f5; border-radius: 4px; cursor: pointer;">Cancelar</button>
                            <button onclick="exportWithDateRange('{}')" style="padding: 8px 16px; background: #417690; color: white; border: none; border-radius: 4px; cursor: pointer;">Exportar</button>
                        </div>
                    </div>
                </div>
                
                <script>
                    function showDateRangeModal(configId) {{
                        document.getElementById('dateRangeModal_' + configId).style.display = 'block';
                    }}
                    
                    function closeDateRangeModal(configId) {{
                        document.getElementById('dateRangeModal_' + configId).style.display = 'none';
                    }}
                    
                    function exportWithDateRange(configId) {{
                        var fechaInicio = document.getElementById('fecha_inicio_' + configId).value;
                        var fechaFin = document.getElementById('fecha_fin_' + configId).value;
                        
                        if (!fechaInicio || !fechaFin) {{
                            alert('Por favor selecciona ambas fechas');
                            return;
                        }}
                        
                        var url = '/admin/contabilidad/export/now/' + configId + '/?fecha_desde=' + fechaInicio + '&fecha_hasta=' + fechaFin;
                        console.log('🔥 URL generada:', url);
                        
                        // 🔥 Abrir en nueva pestaña para ver si descarga
                        window.open(url, '_blank');
                    }}
                </script>
            ''', obj.id, obj.id, obj.id, obj.id, obj.id, obj.id)
            return button_html
        return format_html('<span style="color: #999;">Inactivo</span>')
    export_button.short_description = 'Exportar'
    export_button.allow_tags = True
    
    def get_urls(self):
        """Agregar URLs personalizadas"""
        urls = super().get_urls()
        custom_urls = [
            path('export/now/<uuid:config_id>/', self.admin_site.admin_view(export_contabilidad_admin), name='contabilidad_export_now'),
            path('history/<uuid:config_id>/', self.admin_site.admin_view(get_contabilidad_exports_history), name='contabilidad_history'),
        ]
        return custom_urls + urls


@admin.register(ContabilidadExportHistory)
class ContabilidadExportHistoryAdmin(admin.ModelAdmin):
    """
    Admin para Historial de Exportaciones Contabilidad
    """
    list_display = [
        'export_config',
        'filename',
        'record_count',
        'exported_at',
        'download_link'
    ]
    list_filter = ['exported_at', 'export_config']
    search_fields = ['filename', 'export_config__client_code']
    readonly_fields = ['id', 'export_config', 'filename', 'file_path', 'record_count', 'filters_used', 'exported_at']
    
    def download_link(self, obj):
        """Enlace de descarga"""
        if obj.file_path:
            link_html = format_html(
                '<a href="/media/contabilidad_exports/{}" class="button" style="background-color: #2196F3; color: white; padding: 5px 10px; text-decoration: none; border-radius: 3px;">Descargar</a>',
                obj.filename
            )
            return link_html
        return "No disponible"
    download_link.short_description = 'Descarga'
    
    def has_add_permission(self, request):
        """No permitir agregar manualmente"""
        return False
    
    def has_change_permission(self, request, obj=None):
        """No permitir editar"""
        return False
