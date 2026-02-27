from django.core.management.base import BaseCommand
from django.conf import settings
from integration_service.monitor import FolderMonitor, process_existing_files
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Monitorea carpetas de clientes para procesar archivos automáticamente'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--folder',
            type=str,
            help='Carpeta base para monitorear (default: settings.CLIENT_FILES_FOLDER)'
        )
        parser.add_argument(
            '--process-existing',
            action='store_true',
            help='Procesa archivos existentes antes de iniciar monitoreo'
        )
    
    def handle(self, *args, **options):
        folder = options.get('folder') or getattr(settings, 'CLIENT_FILES_FOLDER', '/tmp/client_files')
        
        self.stdout.write(
            self.style.SUCCESS(f'Iniciando monitoreo en carpeta: {folder}')
        )
        
        # Procesar archivos existentes si se solicita
        if options.get('process_existing'):
            self.stdout.write('Procesando archivos existentes...')
            process_existing_files()
            self.stdout.write(self.style.SUCCESS('Archivos existentes procesados'))
        
        # Iniciar monitoreo
        monitor = FolderMonitor(folder)
        
        try:
            monitor.start_monitoring()
        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING('Monitoreo detenido por usuario'))
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Error en monitoreo: {e}')
            )
