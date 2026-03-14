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
def export_contabilidad_admin_with_date_filter(request, config_id):
    """
    Botón de exportación con filtro de fecha desde admin para contabilidad
    URL: /admin/contabilidad/export/with-date/{config_id}/
    """
    try:
        # 🔥 DEBUG: Mostrar que se ejecuta esta función
        print(f"🔥🔥🔥 EJECUTANDO export_contabilidad_admin_with_date_filter")
        print(f"🔥🔥🔥 Parámetros recibidos: {request.GET}")
        
        # Obtener configuración
        export_config = get_object_or_404(ContabilidadExportConfig, id=config_id)
        
        if not export_config.is_active:
            return JsonResponse({'error': 'Configuración inactiva'}, status=400)
        
        # Obtener filtros del request
        fecha_entrega_desde = request.GET.get('fecha_entrega_desde', '')
        fecha_entrega_hasta = request.GET.get('fecha_entrega_hasta', '')
        
        # 🔥 DEBUG: Mostrar fechas recibidas
        print(f"🔥🔥🔥 Fechas recibidas - Desde: {fecha_entrega_desde} | Hasta: {fecha_entrega_hasta}")
        
        # Iniciar servicio de exportación contabilidad
        export_service = ContabilidadExportService()
        
        # Obtener filtros por defecto
        filters = export_config.default_filters.copy()
        client_id = filters.get('id_clie')
        
        # 🔥 AGREGAR FILTRO DE FECHA DE ENTREGA
        if fecha_entrega_desde:
            filters['fecha_entrega_desde'] = fecha_entrega_desde
        if fecha_entrega_hasta:
            filters['fecha_entrega_hasta'] = fecha_entrega_hasta
        
        # 🔥 DEBUG: Mostrar filtros finales
        print(f"🔥🔥🔥 Filtros finales: {filters}")
        
        # 🔥 PASAR FILTROS AL SERVICIO
        export_service.current_filters = filters
        
        if not client_id:
            return JsonResponse({'error': 'No hay id_clie configurado'}, status=400)
        
        # Obtener datos de la MISMA API
        api_data = export_service.fetch_client_data(int(client_id))
        
        if not api_data:
            return JsonResponse({'error': 'No se encontraron datos'}, status=404)
        
        # 🔥 DEBUG: Mostrar cantidad de datos antes de filtrar
        print(f"🔥🔥🔥 Datos totales antes de filtrar: {len(api_data)}")
        
        # Aplicar transformaciones si existen
        if export_config.transformations:
            api_data = export_service.apply_transformations(api_data, export_config.transformations)
        
        # 🔥 FILTRAR POR FECHA DE ENTREGA si se especificó
        if fecha_entrega_desde or fecha_entrega_hasta:
            filtered_data = []
            for record in api_data:
                fecha_entrega = record.get('fecha_estado', '')  # Usar mapeo "FECHA DE ENTREGA": "fecha_estado"
                
                if fecha_entrega:
                    # Aplicar filtros de fecha
                    incluir = True
                    
                    if fecha_entrega_desde and fecha_entrega < fecha_entrega_desde:
                        incluir = False
                    
                    if fecha_entrega_hasta and fecha_entrega > fecha_entrega_hasta:
                        incluir = False
                    
                    if incluir:
                        filtered_data.append(record)
            
            api_data = filtered_data
            print(f"🔥🔥🔥 Datos después de filtrar por fecha: {len(api_data)}")
        
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
        
        # 🔥 DEBUG: Mostrar resultado final
        print(f"🔥🔥🔥 Exportación completada - Archivo: {filename} | Registros: {len(api_data)}")
        
        # Preparar respuesta para descarga
        filepath = Path(filepath)
        if filepath.exists():
            with open(filepath, 'rb') as f:
                response = HttpResponse(f.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                response['Content-Disposition'] = f'attachment; filename="{filename}"'
                return response
        
        return JsonResponse({'error': 'No se pudo generar el archivo'}, status=500)
        
    except Exception as e:
        logger.error(f"Error en exportación contabilidad con filtro de fecha: {e}")
        return JsonResponse({'error': str(e)}, status=500)


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
        
        # Obtener filtros por defecto
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
        
        # 🔥 FILTRAR POR FECHA USANDO EL MAPEO DINÁMICO
        # Obtener el campo de fecha del mapeo JSON
        campo_fecha = None
        if export_config.column_mapping:
            campo_fecha = export_config.column_mapping.get("FECHA DE ENTREGA")
        
        # 🔥 Detectar filtros del admin
        fecha_desde = request.GET.get('created_at__gte')
        fecha_hasta = request.GET.get('created_at__lt')
        
        # 🔥 Detectar filtros personalizados de fecha_estado
        filtro_estado = request.GET.get('fecha_estado_data')
        
        if filtro_estado and campo_fecha:
            from datetime import date, timedelta
            hoy = date.today()
            
            if filtro_estado == 'hoy':
                fecha_desde = hoy.isoformat()
                fecha_hasta = hoy.isoformat()
            elif filtro_estado == 'ayer':
                ayer = hoy - timedelta(days=1)
                fecha_desde = ayer.isoformat()
                fecha_hasta = ayer.isoformat()
            elif filtro_estado == 'ultima_semana':
                semana_pasada = hoy - timedelta(days=7)
                fecha_desde = semana_pasada.isoformat()
                fecha_hasta = hoy.isoformat()
            elif filtro_estado == 'ultimo_mes':
                mes_pasado = hoy - timedelta(days=30)
                fecha_desde = mes_pasado.isoformat()
                fecha_hasta = hoy.isoformat()
        
        if campo_fecha and (fecha_desde or fecha_hasta):
            print(f"🔥🔥🔥 Filtrando por {campo_fecha} - Desde: {fecha_desde} | Hasta: {fecha_hasta}")
            
            filtered_data = []
            for record in api_data:
                fecha_valor = record.get(campo_fecha, '')
                
                if fecha_valor:
                    # Aplicar filtros de fecha
                    incluir = True
                    
                    if fecha_desde and fecha_valor < fecha_desde:
                        incluir = False
                    
                    if fecha_hasta and fecha_valor > fecha_hasta:
                        incluir = False
                    
                    if incluir:
                        filtered_data.append(record)
            
            api_data = filtered_data
            print(f"🔥🔥🔥 Datos después de filtrar por {campo_fecha}: {len(api_data)}")
        
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
