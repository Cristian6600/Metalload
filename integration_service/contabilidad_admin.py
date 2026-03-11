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
from .contabilidad_views import export_contabilidad_admin, get_contabilidad_exports_history


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
    list_filter = ['export_format', 'is_active', 'created_at']
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
        """Botón de exportación personalizado"""
        if obj.is_active and obj.default_filters.get('id_clie'):
            button_html = format_html(
                '<a href="/admin/contabilidad/export/now/{}/" class="button" style="background-color: #4CAF50; color: white; padding: 5px 10px; text-decoration: none; border-radius: 3px;">Exportar Ahora</a>',
                obj.id
            )
            return button_html
        return "No disponible"
    export_button.short_description = 'Acción'
    
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
