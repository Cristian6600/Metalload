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

from .contabilidad_models import ContabilidadExportConfig, ContabilidadExportHistory
from .contabilidad_views import export_contabilidad_admin, export_contabilidad_admin_with_date_filter, get_contabilidad_exports_history


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
        'export_buttons'  # 🔥 Botón en lista
    ]
    list_filter = ['export_format', 'is_active', 'created_at', FechaEstadoDataFilter]
    search_fields = ['client_code', 'client_name', 'report_name']
    readonly_fields = ['id', 'created_at', 'updated_at']
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
        """Botón de exportación personalizado"""
        return format_html('''
            <div style="display: flex; gap: 5px; flex-wrap: wrap;">
                <a href="/admin/contabilidad/export/now/{}/?{{ request.GET.urlencode }}" 
                   class="button" 
                   style="background-color: #417690; color: white; padding: 5px 10px; text-decoration: none; border-radius:3px; font-size: 11px;">
                    📥 Exportar Todo
                </a>
            </div>
        ''', obj.id)
    export_button.short_description = 'Acción'
    export_button.allow_tags = True
    
    def export_buttons(self, obj):
        """Botones de exportación para la lista con filtro de fecha jerárquico"""
        return format_html('''
            <div style="display: flex; gap: 3px; flex-wrap: wrap;">
                <a href="/admin/contabilidad/export/now/{}/?{{ request.GET.urlencode }}" 
                   class="button" 
                   style="background-color: #417690; color: white; padding: 3px 6px; text-decoration: none; border-radius:3px; font-size: 10px;">
                    📥 Exportar Todo
                </a>
                <a href="javascript:void(0);" 
                   onclick="showDateFilterModal('{}')"
                   class="button" 
                   style="background-color: #17a2b8; color: white; padding: 3px 6px; text-decoration: none; border-radius:3px; font-size: 10px;">
                    � Exportar con Filtro
                </a>
            </div>
            
            <!-- Modal para filtro de fechas con date picker jerárquico -->
            <div id="dateFilterModal_{}" style="display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.5); z-index: 9999;">
                <div style="position: relative; top: 50%; left: 50%; transform: translate(-50%, -50%); background: white; padding: 20px; border-radius: 5px; min-width: 450px; max-width: 500px;">
                    <h3 style="margin-top: 0; color: #333; border-bottom: 2px solid #17a2b8; padding-bottom: 10px;">📅 Filtrar por FECHA DE ENTREGA</h3>
                    
                    <!-- Selector rápido de períodos -->
                    <div style="margin-bottom: 20px;">
                        <label style="display: block; margin-bottom: 8px; font-weight: bold;">📅 Período Rápido:</label>
                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px;">
                            <button type="button" onclick="setDateRange('{}', 'today')" style="padding: 8px; border: 1px solid #ddd; border-radius: 3px; background: #f8f9fa; cursor: pointer;">
                                📅 Hoy
                            </button>
                            <button type="button" onclick="setDateRange('{}', 'yesterday')" style="padding: 8px; border: 1px solid #ddd; border-radius: 3px; background: #f8f9fa; cursor: pointer;">
                                📅 Ayer
                            </button>
                            <button type="button" onclick="setDateRange('{}', 'thisweek')" style="padding: 8px; border: 1px solid #ddd; border-radius: 3px; background: #f8f9fa; cursor: pointer;">
                                📅 Esta Semana
                            </button>
                            <button type="button" onclick="setDateRange('{}', 'lastweek')" style="padding: 8px; border: 1px solid #ddd; border-radius: 3px; background: #f8f9fa; cursor: pointer;">
                                📅 Semana Pasada
                            </button>
                            <button type="button" onclick="setDateRange('{}', 'thismonth')" style="padding: 8px; border: 1px solid #ddd; border-radius: 3px; background: #f8f9fa; cursor: pointer;">
                                📅 Este Mes
                            </button>
                            <button type="button" onclick="setDateRange('{}', 'lastmonth')" style="padding: 8px; border: 1px solid #ddd; border-radius: 3px; background: #f8f9fa; cursor: pointer;">
                                📅 Mes Pasado
                            </button>
                        </div>
                    </div>
                    
                    <!-- Selector de fechas personalizado -->
                    <div style="margin-bottom: 20px;">
                        <label style="display: block; margin-bottom: 8px; font-weight: bold;">📅 Rango Personalizado:</label>
                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px;">
                            <div>
                                <label style="display: block; margin-bottom: 5px; font-size: 12px;">📅 Fecha Desde:</label>
                                <input type="date" id="fecha_desde_{}" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 3px; font-size: 14px;">
                            </div>
                            <div>
                                <label style="display: block; margin-bottom: 5px; font-size: 12px;">📅 Fecha Hasta:</label>
                                <input type="date" id="fecha_hasta_{}" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 3px; font-size: 14px;">
                            </div>
                        </div>
                    </div>
                    
                    <!-- Botones de acción -->
                    <div style="text-align: right; border-top: 2px solid #eee; padding-top: 15px;">
                        <button onclick="clearDateFilters('{}')" style="background: #6c757d; color: white; padding: 8px 15px; border: none; border-radius: 3px; margin-right: 10px; cursor: pointer;">
                            🗑️ Limpiar
                        </button>
                        <button onclick="closeDateFilterModal('{}')" style="background: #dc3545; color: white; padding: 8px 15px; border: none; border-radius: 3px; cursor: pointer;">
                            ❌ Cancelar
                        </button>
                        <button onclick="exportWithDateFilter('{}')" style="background: #28a745; color: white; padding: 8px 15px; border: none; border-radius: 3px; cursor: pointer; font-weight: bold;">
                            � Exportar
                        </button>
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
                
                // 🔥 Funciones para fechas jerárquicas
                function setDateRange(configId, period) {{
                    const today = new Date();
                    const desde = document.getElementById('fecha_desde_' + configId);
                    const hasta = document.getElementById('fecha_hasta_' + configId);
                    
                    switch(period) {{
                        case 'today':
                            desde.value = formatDate(today);
                            hasta.value = formatDate(today);
                            break;
                        case 'yesterday':
                            const yesterday = new Date(today);
                            yesterday.setDate(yesterday.getDate() - 1);
                            desde.value = formatDate(yesterday);
                            hasta.value = formatDate(yesterday);
                            break;
                        case 'thisweek':
                            const startOfWeek = new Date(today);
                            startOfWeek.setDate(today.getDate() - today.getDay());
                            desde.value = formatDate(startOfWeek);
                            hasta.value = formatDate(today);
                            break;
                        case 'lastweek':
                            const endOfLastWeek = new Date(today);
                            endOfLastWeek.setDate(today.getDate() - today.getDay() - 1);
                            const startOfLastWeek = new Date(endOfLastWeek);
                            startOfLastWeek.setDate(endOfLastWeek.getDate() - 6);
                            desde.value = formatDate(startOfLastWeek);
                            hasta.value = formatDate(endOfLastWeek);
                            break;
                        case 'thismonth':
                            const startOfMonth = new Date(today.getFullYear(), today.getMonth(), 1);
                            desde.value = formatDate(startOfMonth);
                            hasta.value = formatDate(today);
                            break;
                        case 'lastmonth':
                            const endOfLastMonth = new Date(today.getFullYear(), today.getMonth(), 0);
                            endOfLastMonth.setDate(endOfLastMonth.getDate() - 1);
                            const startOfLastMonth = new Date(endOfLastMonth.getFullYear(), endOfLastMonth.getMonth(), 1);
                            desde.value = formatDate(startOfLastMonth);
                            hasta.value = formatDate(endOfLastMonth);
                            break;
                    }}
                }}
                
                function clearDateFilters(configId) {{
                    document.getElementById('fecha_desde_' + configId).value = '';
                    document.getElementById('fecha_hasta_' + configId).value = '';
                }}
                
                function formatDate(date) {{
                    const year = date.getFullYear();
                    const month = String(date.getMonth() + 1).padStart(2, '0');
                    const day = String(date.getDate()).padStart(2, '0');
                    return year + '-' + month + '-' + day;
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
                    
                    console.log('🔥 Exportando con fechas - Desde:', fechaDesde, 'Hasta:', fechaHasta);
                    window.location.href = url;
                }}
            </script>
        ''', obj.id, obj.id, obj.id, obj.id, obj.id, obj.id, obj.id)
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
