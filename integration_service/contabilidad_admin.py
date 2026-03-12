"""
Admin para Exportación Contabilidad
"""
from django.contrib import admin
from django.urls import path
from django.shortcuts import render
from django.http import JsonResponse
from django.utils.html import format_html
import uuid

from .contabilidad_models import ContabilidadExportConfig, ContabilidadExportHistory
from .contabilidad_views import export_contabilidad_admin, export_contabilidad_admin_with_date_filter, get_contabilidad_exports_history


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
        'export_buttons'  # 🔥 Botón en lista
    ]
    list_filter = ['export_format', 'is_active', 'created_at']
    search_fields = ['client_code', 'client_name', 'report_name']
    readonly_fields = ['id', 'created_at', 'updated_at']
    
    # 🔥 AGREGAR FILTRO DE FECHA JERÁRQUICO
    date_hierarchy = 'created_at'
    
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
        """Botón de exportación personalizado con filtro de fecha"""
        return format_html('''
            <div style="display: flex; gap: 5px; flex-wrap: wrap;">
                <a href="/admin/contabilidad/export/now/{}/" 
                   class="button" 
                   style="background-color: #417690; color: white; padding: 5px 10px; text-decoration: none; border-radius:3px; font-size: 11px;">
                    📥 Exportar Todo
                </a>
                <a href="javascript:void(0);" 
                   onclick="showDateFilterModal('{}')"
                   class="button" 
                   style="background-color: #17a2b8; color: white; padding: 5px 10px; text-decoration: none; border-radius:3px; font-size: 11px;">
                    📅 Exportar con Filtro
                </a>
            </div>
            
            <!-- Modal para filtro de fechas -->
            <div id="dateFilterModal_{}" style="display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.5); z-index: 9999;">
                <div style="position: relative; top: 50%; left: 50%; transform: translate(-50%, -50%); background: white; padding: 20px; border-radius: 5px; min-width: 400px;">
                    <h3 style="margin-top: 0;">📅 Filtrar por FECHA DE ENTREGA</h3>
                    <div style="margin-bottom: 15px;">
                        <label style="display: block; margin-bottom: 5px;">📅 Fecha Desde:</label>
                        <input type="date" id="fecha_desde_{}" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 3px;">
                    </div>
                    <div style="margin-bottom: 15px;">
                        <label style="display: block; margin-bottom: 5px;">📅 Fecha Hasta:</label>
                        <input type="date" id="fecha_hasta_{}" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 3px;">
                    </div>
                    <div style="text-align: right;">
                        <button onclick="closeDateFilterModal('{}')" style="background: #6c757d; color: white; padding: 8px 15px; border: none; border-radius: 3px; margin-right: 10px;">Cancelar</button>
                        <button onclick="exportWithDateFilter('{}')" style="background: #28a745; color: white; padding: 8px 15px; border: none; border-radius: 3px;">📥 Exportar</button>
                    </div>
                </div>
            </div>
            
            <script>
                function showDateFilterModal(configId) {{
                    document.getElementById('dateFilterModal_' + configId).style.display = 'block';
                }}
                
                function closeDateFilterModal(configId) {{
                    document.getElementById('dateFilterModal_' + configId).style.display = 'none';
                }}
                
                function exportWithDateFilter(configId) {{
                    const fechaDesde = document.getElementById('fecha_desde_' + configId).value;
                    const fechaHasta = document.getElementById('fecha_hasta_' + configId).value;
                    
                    let url = '/admin/contabilidad/export/with-date/' + configId + '/';
                    
                    if (fechaDesde) {{
                        url += '?fecha_entrega_desde=' + fechaDesde;
                    }}
                    if (fechaHasta) {{
                        url += (fechaDesde ? '&' : '?') + 'fecha_entrega_hasta=' + fechaHasta;
                    }}
                    
                    window.location.href = url;
                }}
            </script>
        ''', obj.id, obj.id, obj.id, obj.id, obj.id)
    export_button.short_description = 'Acción'
    export_button.allow_tags = True
    
    def export_buttons(self, obj):
        """Botones de exportación para la lista con filtro de fecha jerárquico"""
        return format_html('''
            <div style="display: flex; gap: 3px; flex-wrap: wrap;">
                <a href="/admin/contabilidad/export/now/{}/" 
                   class="button" 
                   style="background-color: #417690; color: white; padding: 3px 6px; text-decoration: none; border-radius:3px; font-size: 10px;">
                    📥 Exportar Todo
                </a>
                <a href="javascript:void(0);" 
                   onclick="alert('🔥 Botón de filtro clickeado - ID: {}'); showDateFilterModal('{}')"
                   class="button" 
                   style="background-color: #17a2b8; color: white; padding: 3px 6px; text-decoration: none; border-radius:3px; font-size: 10px;">
                    📅 Exportar con Filtro
                </a>
            </div>
        ''', obj.id, obj.id)
    export_buttons.short_description = 'Exportación'
    export_buttons.allow_tags = True
    
    def get_urls(self):
        """Agregar URLs personalizadas"""
        urls = super().get_urls()
        custom_urls = [
            path('export/now/<uuid:config_id>/', self.admin_site.admin_view(export_contabilidad_admin), name='contabilidad_export_now'),
            path('export/with-date/<uuid:config_id>/', self.admin_site.admin_view(export_contabilidad_admin_with_date_filter), name='contabilidad_export_with_date'),
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
