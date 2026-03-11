"""
Views para Exportación Contabilidad
"""
import uuid
import logging
from django.shortcuts import get_object_or_404
from django.http import HttpResponse, JsonResponse
from django.contrib.admin.views.decorators import staff_member_required
from django.conf import settings
from pathlib import Path

from .contabilidad_models import ContabilidadExportConfig, ContabilidadExportHistory
from .contabilidad_service import ContabilidadExportService

logger = logging.getLogger(__name__)


@staff_member_required
def export_contabilidad_admin(request, config_id):
    """
    Botón de exportación directa desde admin para contabilidad
    URL: /admin/contabilidad/export/now/{config_id}/
    """
    try:
        # Obtener configuración
        export_config = get_object_or_404(ContabilidadExportConfig, id=config_id)
        
        if not export_config.is_active:
            return JsonResponse({'error': 'Configuración inactiva'}, status=400)
        
        # Iniciar servicio de exportación contabilidad
        export_service = ContabilidadExportService()
        
        # Obtener filtros por defecto (misma API)
        filters = export_config.default_filters
        client_id = filters.get('id_clie')
        
        # 🔥 PASAR FILTROS AL SERVICIO
        export_service.current_filters = filters
        
        if not client_id:
            return JsonResponse({'error': 'No hay id_clie configurado'}, status=400)
        
        # Obtener datos de la MISMA API
        api_data = export_service.fetch_client_data(int(client_id))
        
        if not api_data:
            return JsonResponse({'error': 'No se encontraron datos'}, status=404)
        
        # Aplicar transformaciones si existen
        if export_config.transformations:
            api_data = export_service.apply_transformations(api_data, export_config.transformations)
        
        # Mapear columnas
        if export_config.column_mapping and export_config.column_order:
            api_data = export_service.map_columns(
                api_data, 
                export_config.column_mapping, 
                export_config.column_order
            )
        
        # Generar archivo según formato
        filename = export_service.generate_contabilidad_filename(
            export_config.client_code,
            export_config.export_format
        )
        
        # Exportar según formato configurado
        if export_config.export_format == 'xlsx':
            filepath = export_service.export_to_excel_contabilidad(
                api_data, 
                filename, 
                export_config.excel_config
            )
        elif export_config.export_format == 'csv':
            delimiter = export_config.excel_config.get('delimiter', ',')
            filepath = export_service.export_to_csv(api_data, filename, delimiter)
        elif export_config.export_format == 'json':
            filepath = export_service.export_to_json(api_data, filename)
        elif export_config.export_format == 'pdf':
            filepath = export_service.export_to_pdf_factura(
                api_data, 
                filename, 
                export_config
            )
        else:
            filepath = export_service.export_to_csv(api_data, filename)
        
        # Guardar en historial contabilidad
        export_service.save_contabilidad_history(
            export_config, 
            filename, 
            filepath, 
            len(api_data), 
            filters
        )
        
        # Preparar respuesta para descarga
        filepath = Path(filepath)
        if filepath.exists():
            with open(filepath, 'rb') as f:
                response = HttpResponse(f.read(), content_type='application/octet-stream')
                response['Content-Disposition'] = f'attachment; filename="{filename}"'
                return response
        else:
            return JsonResponse({'error': 'Archivo no encontrado'}, status=404)
        
    except Exception as e:
        logger.error(f"Error en exportación contabilidad: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@staff_member_required
def get_contabilidad_exports_history(request, config_id=None):
    """
    Obtener historial de exportaciones contabilidad
    """
    try:
        if config_id:
            export_config = get_object_or_404(ContabilidadExportConfig, id=config_id)
            history = ContabilidadExportHistory.objects.filter(export_config=export_config)
        else:
            history = ContabilidadExportHistory.objects.all()
        
        history_data = []
        for h in history:
            history_data.append({
                'id': str(h.id),
                'client_code': h.export_config.client_code,
                'filename': h.filename,
                'record_count': h.record_count,
                'exported_at': h.exported_at.strftime('%Y-%m-%d %H:%M:%S'),
                'filters_used': h.filters_used
            })
        
        return JsonResponse({'history': history_data})
        
    except Exception as e:
        logger.error(f"Error obteniendo historial contabilidad: {e}")
        return JsonResponse({'error': str(e)}, status=500)
