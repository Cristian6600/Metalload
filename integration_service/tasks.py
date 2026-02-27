from celery import shared_task
from django.utils import timezone
from .models import ClientFile
from .services import FileProcessor, ReportGenerator
import logging

logger = logging.getLogger(__name__)


@shared_task(bind=True, max_retries=3)
def process_client_file(self, client_file_id):
    """
    Tarea asíncrona para procesar un archivo de cliente
    """
    try:
        client_file = ClientFile.objects.get(id=client_file_id)
        
        if client_file.status != 'pending':
            logger.warning(f"Archivo {client_file_id} no está en estado pendiente")
            return {'status': 'skipped', 'reason': 'not_pending'}
        
        processor = FileProcessor()
        result = processor.process_file(client_file)
        
        return {
            'status': 'success' if result['success'] else 'error',
            'result': result,
            'client_file_id': str(client_file_id)
        }
        
    except ClientFile.DoesNotExist:
        logger.error(f"Archivo {client_file_id} no encontrado")
        return {'status': 'error', 'reason': 'not_found'}
    
    except Exception as e:
        logger.error(f"Error procesando archivo {client_file_id}: {e}")
        
        # Reintentar si hay fallos temporales
        if self.request.retries < self.max_retries:
            raise self.retry(countdown=60 * (2 ** self.request.retries))
        
        return {
            'status': 'error',
            'reason': str(e),
            'client_file_id': str(client_file_id)
        }


@shared_task
def process_pending_files():
    """
    Tarea programada para procesar archivos pendientes
    """
    pending_files = ClientFile.objects.filter(status='pending')
    
    processed_count = 0
    for client_file in pending_files:
        process_client_file.delay(str(client_file.id))
        processed_count += 1
    
    logger.info(f"Encoladas {processed_count} tareas de procesamiento")
    return {'enqueued_tasks': processed_count}


@shared_task
def generate_daily_report():
    """
    Tarea programada para generar reporte diario
    """
    try:
        yesterday = timezone.now() - timezone.timedelta(days=1)
        start_of_day = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        report = ReportGenerator.generate_processing_summary(start_of_day, end_of_day)
        
        logger.info(f"Reporte diario generado: {report.id}")
        return {'report_id': str(report.id), 'date': yesterday.date().isoformat()}
        
    except Exception as e:
        logger.error(f"Error generando reporte diario: {e}")
        return {'status': 'error', 'error': str(e)}


@shared_task
def cleanup_old_logs():
    """
    Tarea para limpiar logs antiguos (más de 30 días)
    """
    from .models import ProcessingLog
    
    cutoff_date = timezone.now() - timezone.timedelta(days=30)
    deleted_count, _ = ProcessingLog.objects.filter(created_at__lt=cutoff_date).delete()
    
    logger.info(f"Eliminados {deleted_count} logs antiguos")
    return {'deleted_logs': deleted_count}
