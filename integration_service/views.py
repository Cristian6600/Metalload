from rest_framework import viewsets, status, generics
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.parsers import MultiPartParser, FormParser
from django_filters.rest_framework import DjangoFilterBackend
from django.utils import timezone
from datetime import timedelta
from django.shortcuts import render, get_object_or_404
from django.http import HttpResponse, JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.contrib.admin.views.decorators import staff_member_required
from django.conf import settings
from pathlib import Path
import mimetypes
import logging
import uuid

from .models import ClientFile, ProcessingLog, ClientMapping, Report, ExportConfig, ExportHistory
from .serializers import (
    ClientFileSerializer, ClientFileUploadSerializer, ProcessingLogSerializer,
    ClientMappingSerializer, ReportSerializer, FileProcessingRequestSerializer
)
from .services import FileProcessor, ReportGenerator
from .export_service import ExportService

logger = logging.getLogger(__name__)


class ClientFileViewSet(viewsets.ModelViewSet):
    """ViewSet para gestionar archivos de clientes"""
    queryset = ClientFile.objects.all()
    serializer_class = ClientFileSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_fields = ['client_code', 'status']
    parser_classes = [MultiPartParser, FormParser]
    
    def get_serializer_class(self):
        """Retorna el serializer apropiado según la acción"""
        if self.action == 'create':
            return ClientFileUploadSerializer
        return ClientFileSerializer
    
    @action(detail=True, methods=['post'])
    def process(self, request, pk=None):
        """
        Procesa un archivo de cliente
        """
        client_file = self.get_object()
        
        if client_file.status != 'pending':
            return Response(
                {'error': 'El archivo ya fue procesado o está en proceso'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        processor = FileProcessor()
        result = processor.process_file(client_file)
        
        if result['success']:
            return Response({
                'message': 'Archivo procesado exitosamente',
                'result': result
            }, status=status.HTTP_200_OK)
        else:
            return Response({
                'error': 'Error procesando archivo',
                'details': result['error']
            }, status=status.HTTP_400_BAD_REQUEST)
    
    @action(detail=True, methods=['get'])
    def logs(self, request, pk=None):
        """
        Obtiene los logs de procesamiento de un archivo
        """
        client_file = self.get_object()
        logs = client_file.logs.all()
        serializer = ProcessingLogSerializer(logs, many=True)
        return Response(serializer.data)
    
    @action(detail=False, methods=['get'])
    def stats(self, request):
        """
        Obtiene estadísticas generales
        """
        total_files = ClientFile.objects.count()
        processed_files = ClientFile.objects.filter(status='processed').count()
        error_files = ClientFile.objects.filter(status='error').count()
        pending_files = ClientFile.objects.filter(status='pending').count()
        
        # Últimos 7 días
        week_ago = timezone.now() - timedelta(days=7)
        recent_files = ClientFile.objects.filter(uploaded_at__gte=week_ago).count()
        
        return Response({
            'total_files': total_files,
            'processed_files': processed_files,
            'error_files': error_files,
            'pending_files': pending_files,
            'recent_files': recent_files,
            'success_rate': (processed_files / total_files * 100) if total_files > 0 else 0
        })


class ClientMappingViewSet(viewsets.ModelViewSet):
    """ViewSet para gestionar mapeos de clientes"""
    queryset = ClientMapping.objects.all()
    serializer_class = ClientMappingSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_fields = ['client_code', 'is_active']
    
    @action(detail=True, methods=['post'])
    def test_mapping(self, request, pk=None):
        """
        Permite probar un mapeo con datos de ejemplo
        """
        mapping = self.get_object()
        test_data = request.data.get('test_data', {})
        
        if not test_data:
            return Response(
                {'error': 'Se requieren datos de prueba'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Simular transformación
        mapping_config = mapping.mapping_config
        transformed = {}
        
        for target_field, source_config in mapping_config.items():
            if isinstance(source_config, str):
                transformed[target_field] = test_data.get(source_config, '')
            elif isinstance(source_config, dict):
                source_field = source_config.get('source')
                transform_type = source_config.get('transform', 'direct')
                value = test_data.get(source_field, '')
                
                if transform_type == 'upper':
                    transformed[target_field] = str(value).upper()
                elif transform_type == 'lower':
                    transformed[target_field] = str(value).lower()
                elif transform_type == 'strip':
                    transformed[target_field] = str(value).strip()
                else:
                    transformed[target_field] = value
            else:
                transformed[target_field] = ''
        
        return Response({
            'original_data': test_data,
            'transformed_data': transformed,
            'mapping_config': mapping_config
        })


class ProcessingLogViewSet(viewsets.ReadOnlyModelViewSet):
    """ViewSet para visualizar logs de procesamiento (solo lectura)"""
    queryset = ProcessingLog.objects.all()
    serializer_class = ProcessingLogSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_fields = ['client_file', 'level']


class ReportViewSet(viewsets.ModelViewSet):
    """ViewSet para gestionar reportes"""
    queryset = Report.objects.all()
    serializer_class = ReportSerializer
    filter_backends = [DjangoFilterBackend]
    filterset_fields = ['report_type']
    
    @action(detail=False, methods=['post'])
    def generate_summary(self, request):
        """
        Genera un reporte de resumen de procesamiento
        """
        date_from = request.data.get('date_from')
        date_to = request.data.get('date_to')
        
        # Convertir strings a fechas si se proporcionan
        if date_from:
            date_from = timezone.datetime.fromisoformat(date_from.replace('Z', '+00:00'))
        if date_to:
            date_to = timezone.datetime.fromisoformat(date_to.replace('Z', '+00:00'))
        
        try:
            report = ReportGenerator.generate_processing_summary(date_from, date_to)
            serializer = ReportSerializer(report)
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        except Exception as e:
            return Response(
                {'error': f'Error generando reporte: {str(e)}'},
                status=status.HTTP_400_BAD_REQUEST
            )
    
    @action(detail=False, methods=['get'])
    def dashboard_data(self, request):
        """
        Obtiene datos para el dashboard principal
        """
        # Estadísticas generales
        total_files = ClientFile.objects.count()
        processed_files = ClientFile.objects.filter(status='processed').count()
        error_files = ClientFile.objects.filter(status='error').count()
        
        # Últimos 30 días para gráficos
        thirty_days_ago = timezone.now() - timedelta(days=30)
        daily_stats = []
        
        for i in range(30):
            date = thirty_days_ago + timedelta(days=i)
            day_start = date.replace(hour=0, minute=0, second=0, microsecond=0)
            day_end = date.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            day_files = ClientFile.objects.filter(
                uploaded_at__gte=day_start,
                uploaded_at__lte=day_end
            )
            
            daily_stats.append({
                'date': date.strftime('%Y-%m-%d'),
                'total': day_files.count(),
                'processed': day_files.filter(status='processed').count(),
                'errors': day_files.filter(status='error').count()
            })
        
        # Top clientes con más archivos
        top_clients = ClientFile.objects.values('client_code').annotate(
            total_files=models.Count('id'),
            processed_files=models.Count('id', filter=models.Q(status='processed'))
        ).order_by('-total_files')[:10]
        
        return Response({
            'summary': {
                'total_files': total_files,
                'processed_files': processed_files,
                'error_files': error_files,
                'success_rate': (processed_files / total_files * 100) if total_files > 0 else 0
            },
            'daily_stats': daily_stats,
            'top_clients': list(top_clients)
        })


class BatchProcessingView(generics.GenericAPIView):
    """Vista para procesamiento batch de archivos"""
    serializer_class = FileProcessingRequestSerializer
    
    def post(self, request):
        """
        Procesa múltiples archivos en batch
        """
        file_ids = request.data.get('file_ids', [])
        processor = FileProcessor()
        results = []
        
        for file_id in file_ids:
            try:
                client_file = ClientFile.objects.get(id=file_id, status='pending')
                result = processor.process_file(client_file)
                results.append({
                    'file_id': str(file_id),
                    'success': result['success'],
                    'result': result
                })
            except ClientFile.DoesNotExist:
                results.append({
                    'file_id': str(file_id),
                    'success': False,
                    'error': 'Archivo no encontrado o no está pendiente'
                })
        
        return Response({'results': results})


# ==================== VISTAS DE EXPORTACIÓN ====================

@csrf_exempt
@require_http_methods(["GET"])
def export_client_data(request):
    """
    Endpoint principal de exportación de clientes
    URL: /clientes/export/?id_clie=X
    """
    try:
        # Obtener parámetros
        client_id = request.GET.get('id_clie')
        if not client_id:
            return JsonResponse({'error': 'Se requiere id_clie'}, status=400)
        
        # Buscar configuración de exportación para este cliente
        try:
            # Buscar por id_clie o por código de cliente
            export_config = ExportConfig.objects.filter(
                default_filters__id_clie=client_id
            ).first()
            
            if not export_config:
                # Configuración por defecto si no encuentra específica
                export_config = _create_default_config(client_id)
        
        except ExportConfig.DoesNotExist:
            export_config = _create_default_config(client_id)
        
        # Iniciar servicio de exportación
        export_service = ExportService()
        
        # Obtener datos de la API
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
        filename = export_service.generate_filename(
            export_config.client_code or f"cliente_{client_id}",
            export_config.export_format
        )
        
        # Exportar según formato configurado
        if export_config.export_format == 'xlsx':
            filepath = export_service.export_to_excel(
                api_data, 
                filename, 
                export_config.excel_config
            )
        elif export_config.export_format == 'csv':
            delimiter = export_config.excel_config.get('delimiter', ',')
            filepath = export_service.export_to_csv(api_data, filename, delimiter)
        elif export_config.export_format == 'json':
            filepath = export_service.export_to_json(api_data, filename)
        else:
            filepath = export_service.export_to_csv(api_data, filename)
        
        # Guardar en historial
        export_history = ExportHistory.objects.create(
            export_config=export_config,
            filters_used={'id_clie': client_id},
            record_count=len(api_data),
            filename=filename,
            file_path=filepath,
            file_size=Path(filepath).stat().st_size,
            status='completed'
        )
        
        # Descargar archivo
        return _download_file(filepath, filename)
        
    except Exception as e:
        logger.error(f"Error en exportación: {e}")
        return JsonResponse({'error': str(e)}, status=500)


def _create_default_config(client_id: str) -> ExportConfig:
    """Crea configuración por defecto para cliente"""
    # Configuración por defecto tipo SERFINANZA
    default_mapping = {
        "pseudo_id": "seudo_bd",
        "cliente_id": "id_clie", 
        "nombre_completo": "nombre",
        "apellidos": "surname",
        "documento": "cc",
        "tipo_doc": "documento",
        "ciudad_cod": "ciudad",
        "producto": "nom_pro"
    }
    
    default_order = [
        "pseudo_id", "cliente_id", "nombre_completo", "apellidos", 
        "documento", "tipo_doc", "ciudad_cod", "producto"
    ]
    
    config = ExportConfig.objects.create(
        client_code=f"CLIENTE_{client_id}",
        client_name=f"Cliente {client_id}",
        column_mapping=default_mapping,
        column_order=default_order,
        export_format='xlsx',
        default_filters={'id_clie': int(client_id)},
        excel_config={
            'header_style': 'bold',
            'auto_width': True,
            'freeze_header': True
        }
    )
    
    logger.info(f"Creada configuración por defecto para cliente {client_id}")
    return config


def _download_file(filepath: str, filename: str) -> HttpResponse:
    """
    Prepara respuesta para descarga de archivo
    
    Args:
        filepath: Ruta del archivo
        filename: Nombre del archivo para descarga
        
    Returns:
        HttpResponse: Respuesta de descarga
    """
    try:
        path = Path(filepath)
        if not path.exists():
            return JsonResponse({'error': 'Archivo no encontrado'}, status=404)
        
        # Determinar MIME type
        mime_type, _ = mimetypes.guess_type(str(path))
        if mime_type is None:
            mime_type = 'application/octet-stream'
        
        # Leer archivo
        with open(path, 'rb') as f:
            response = HttpResponse(f.read(), content_type=mime_type)
        
        # Configurar headers para descarga
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        response['Content-Length'] = path.stat().st_size
        
        return response
        
    except Exception as e:
        logger.error(f"Error preparando descarga: {e}")
        return JsonResponse({'error': 'Error al preparar archivo'}, status=500)


@staff_member_required
def export_configs_list(request):
    """Lista todas las configuraciones de exportación (Admin)"""
    configs = ExportConfig.objects.all()
    data = []
    
    for config in configs:
        data.append({
            'id': str(config.id),
            'client_code': config.client_code,
            'client_name': config.client_name,
            'export_format': config.export_format,
            'is_active': config.is_active,
            'created_at': config.created_at.isoformat(),
            'export_count': config.exports.count()
        })
    
    return JsonResponse({'configs': data})


@staff_member_required  
def export_history_list(request):
    """Lista historial de exportaciones (Admin)"""
    history = ExportHistory.objects.select_related('export_config').all()
    data = []
    
    for export in history:
        data.append({
            'id': str(export.id),
            'client_code': export.export_config.client_code,
            'filename': export.filename,
            'record_count': export.record_count,
            'file_size': export.file_size,
            'status': export.status,
            'exported_at': export.exported_at.isoformat()
        })
    
    return JsonResponse({'history': data})


@staff_member_required
def export_now_admin(request, config_id: uuid):
    """
    Botón de exportación directa desde admin
    URL: /admin/export/now/{config_id}/
    """
    try:
        # Obtener configuración
        export_config = get_object_or_404(ExportConfig, id=config_id)
        
        if not export_config.is_active:
            return JsonResponse({'error': 'Configuración inactiva'}, status=400)
        
        # Iniciar servicio de exportación
        export_service = ExportService()
        
        # Obtener filtros por defecto
        filters = export_config.default_filters
        client_id = filters.get('id_clie')
        
        if not client_id:
            return JsonResponse({'error': 'No hay id_clie configurado'}, status=400)
        
        # Obtener datos de la API
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
        filename = export_service.generate_filename(
            export_config.client_code,
            export_config.export_format
        )
        
        # Exportar según formato configurado
        if export_config.export_format == 'xlsx':
            filepath = export_service.export_to_excel(
                api_data, 
                filename, 
                export_config.excel_config
            )
        elif export_config.export_format == 'csv':
            delimiter = export_config.excel_config.get('delimiter', ',')
            filepath = export_service.export_to_csv(api_data, filename, delimiter)
        elif export_config.export_format == 'json':
            filepath = export_service.export_to_json(api_data, filename)
        else:
            filepath = export_service.export_to_csv(api_data, filename)
        
        # Guardar en historial
        export_history = ExportHistory.objects.create(
            export_config=export_config,
            filters_used=filters,
            record_count=len(api_data),
            filename=filename,
            file_path=filepath,
            file_size=Path(filepath).stat().st_size,
            status='completed'
        )
        
        # Descargar archivo
        return _download_file(filepath, filename)
        
    except Exception as e:
        logger.error(f"Error en exportación admin: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@staff_member_required
def download_export_admin(request, history_id: uuid):
    """
    Descarga archivo del historial desde admin
    URL: /admin/export/download/{history_id}/
    """
    try:
        export_history = get_object_or_404(ExportHistory, id=history_id)
        
        if export_history.status != 'completed':
            return JsonResponse({'error': 'Exportación no completada'}, status=400)
        
        return _download_file(export_history.file_path, export_history.filename)
        
    except Exception as e:
        logger.error(f"Error descargando archivo: {e}")
        return JsonResponse({'error': str(e)}, status=500)
