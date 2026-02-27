import os
import time
import shutil
from pathlib import Path
from django.conf import settings
from django.core.files import File
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from .models import ClientFile, ClientMapping
from .services import FileProcessor
import logging

logger = logging.getLogger(__name__)


class ClientFolderHandler(FileSystemEventHandler):
    """Maneja eventos de archivos en carpetas de clientes"""
    
    def __init__(self, base_folder):
        # El base_folder debe apuntar a la carpeta client_files, no a la subcarpeta del cliente
        if 'input' in str(base_folder):
            # Si base_folder contiene 'input', subir 2 niveles para llegar a client_files
            self.base_folder = Path(base_folder).parent.parent
        else:
            # Si no, subir 1 nivel
            self.base_folder = Path(base_folder).parent
        
        self.processed_folder = self.base_folder / 'processed'
        self.error_folder = self.base_folder / 'errors'
        
        # Crear carpetas si no existen
        self.processed_folder.mkdir(exist_ok=True)
        self.error_folder.mkdir(exist_ok=True)
    
    def on_created(self, event):
        """Se ejecuta cuando se crea un nuevo archivo"""
        if event.is_directory:
            return
            
        file_path = Path(event.src_path)
        
        # Ignorar archivos temporales y ocultos
        if file_path.name.startswith('.') or file_path.name.startswith('~'):
            return
            
        # Ignorar archivos de lock
        if 'lock' in file_path.name.lower():
            return
            
        # Esperar a que el archivo se complete de copiar
        time.sleep(2)
        
        if not file_path.exists():
            return
            
        # Extraer código del cliente del nombre de la carpeta padre
        client_code = file_path.parent.name
        
        # Si estamos en la carpeta input, obtener el cliente del padre
        if client_code == 'input':
            client_code = file_path.parent.parent.name
        
        # Procesar solo si el cliente tiene configuración
        if not ClientMapping.objects.filter(client_code=client_code, is_active=True).exists():
            logger.warning(f"No hay configuración para el cliente: {client_code}")
            return
        
        try:
            self.process_file(file_path, client_code)
        except Exception as e:
            logger.error(f"Error procesando archivo {file_path}: {e}")
            self.move_to_error(file_path)
    
    def process_file(self, file_path, client_code):
        """Procesa un archivo individual"""
        try:
            # Verificar si el archivo ya fue procesado (permitir reprocesar)
            existing_file = ClientFile.objects.filter(original_filename=file_path.name, client_code=client_code).first()
            if existing_file and existing_file.status != 'error':
                # Si el archivo existe pero tiene un error, permitir reprocesar
                logger.info(f"Archivo {file_path.name} ya fue procesado anteriormente, pero se permite reprocesar")
            elif existing_file and existing_file.status == 'completed':
                # Si el archivo está completado, omitir para evitar duplicados
                logger.warning(f"Archivo {file_path.name} ya fue procesado y completado anteriormente, omitiendo")
                return
            # Si no existe o tiene error, procesar normalmente
            
            # Crear registro en la base de datos
            with open(file_path, 'rb') as f:
                django_file = File(f)
                client_file = ClientFile.objects.create(
                    client_code=client_code,
                    file=django_file,
                    original_filename=file_path.name,
                    file_size=file_path.stat().st_size
                )
            
            # Procesar el archivo
            processor = FileProcessor()
            result = processor.process_file(client_file)
            
            if result['success']:
                logger.info(f"Archivo procesado exitosamente: {file_path}")
                self.move_to_processed(file_path)
            else:
                logger.error(f"Error procesando archivo: {result['error']}")
                self.move_to_error(file_path)
                
        except Exception as e:
            logger.error(f"Error en process_file: {e}")
            self.move_to_error(file_path)
    
    def move_to_processed(self, file_path):
        """Mueve archivo a carpeta de procesados"""
        try:
            # Obtener la carpeta del cliente correcta
            # Si la ruta es: /base_folder/CLIENTE_REMESA/input/archivo.xlsx
            # entonces client_code es 'CLIENTE_REMESA' (sin duplicar)
            if 'input' in str(file_path):
                # Extraer el nombre del cliente de la ruta
                path_parts = file_path.parts
                input_index = path_parts.index('input')
                # El nombre del cliente está justo antes de 'input'
                client_code = path_parts[input_index - 1]
                client_processed_folder = self.base_folder / client_code / 'processed'
            else:
                client_processed_folder = self.processed_folder
                
            # Asegurar que la carpeta exista
            client_processed_folder.mkdir(parents=True, exist_ok=True)
            processed_path = client_processed_folder / file_path.name
            
            # Copiar el archivo y luego eliminar el original
            if file_path.exists():
                shutil.copy2(str(file_path), str(processed_path))
                file_path.unlink()  # Eliminar el original
                logger.info(f"Archivo movido a procesados: {processed_path}")
            else:
                logger.warning(f"El archivo original ya no existe: {file_path}")
                
        except Exception as e:
            logger.error(f"Error moviendo archivo a procesados: {e}")
    
    def move_to_error(self, file_path):
        """Mueve archivo a carpeta de errores"""
        try:
            # Obtener la carpeta del cliente correcta
            # Si la ruta es: /base_folder/CLIENTE_REMESA/input/archivo.xlsx
            # entonces client_code es 'CLIENTE_REMESA'
            if 'input' in str(file_path):
                # Extraer el nombre del cliente de la ruta
                path_parts = file_path.parts
                input_index = path_parts.index('input')
                client_code = path_parts[input_index - 1] if input_index > 0 else 'CLIENTE_REMESA'
                client_error_folder = self.base_folder / client_code / 'errors'
            else:
                client_error_folder = self.errors_folder
                
            # Asegurar que la carpeta exista
            client_error_folder.mkdir(parents=True, exist_ok=True)
            error_path = client_error_folder / file_path.name
            
            # Copiar el archivo y luego eliminar el original
            if file_path.exists():
                shutil.copy2(str(file_path), str(error_path))
                file_path.unlink()  # Eliminar el original
                logger.info(f"Archivo movido a errores: {error_path}")
            else:
                logger.warning(f"El archivo original ya no existe: {file_path}")
                
        except Exception as e:
            logger.error(f"Error moviendo archivo a errores: {e}")


class FolderMonitor:
    """Monitor de carpetas para clientes"""
    
    def __init__(self, base_folder=None):
        self.base_folder = Path(base_folder or getattr(settings, 'CLIENT_FILES_FOLDER', '/tmp/client_files'))
        self.observer = Observer()
        
        # Crear estructura de carpetas
        self.setup_folder_structure()
    
    def setup_folder_structure(self):
        """Crea la estructura de carpetas"""
        self.base_folder.mkdir(exist_ok=True)
        
        # Crear carpetas para cada cliente activo
        for mapping in ClientMapping.objects.filter(is_active=True):
            client_folder = self.base_folder / mapping.client_code
            client_folder.mkdir(exist_ok=True)
            
            # Crear subcarpetas
            (client_folder / 'input').mkdir(exist_ok=True)
            (client_folder / 'processed').mkdir(exist_ok=True)
            (client_folder / 'errors').mkdir(exist_ok=True)
    
    def start_monitoring(self):
        """Inicia el monitoreo de carpetas"""
        logger.info(f"Iniciando monitoreo en: {self.base_folder}")
        
        # Monitorear cada carpeta de cliente
        for mapping in ClientMapping.objects.filter(is_active=True):
            client_folder = self.base_folder / mapping.client_code / 'input'
            if client_folder.exists():
                event_handler = ClientFolderHandler(client_folder.parent)
                self.observer.schedule(event_handler, str(client_folder), recursive=False)
                logger.info(f"Monitoreando carpeta: {client_folder}")
        
        self.observer.start()
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.observer.stop()
        
        self.observer.join()
    
    def stop_monitoring(self):
        """Detiene el monitoreo"""
        self.observer.stop()
        self.observer.join()


# Función para usar como tarea Celery
def process_existing_files():
    """Procesa archivos existentes en las carpetas"""
    base_folder = Path(getattr(settings, 'CLIENT_FILES_FOLDER', '/tmp/client_files'))
    
    for mapping in ClientMapping.objects.filter(is_active=True):
        input_folder = base_folder / mapping.client_code / 'input'
        
        if input_folder.exists():
            for file_path in input_folder.glob('*'):
                if file_path.is_file():
                    handler = ClientFolderHandler(input_folder.parent)
                    handler.process_file(file_path, mapping.client_code)
