"""
Configuración de la aplicación Contabilidad
"""
from django.apps import AppConfig


class ContabilidadConfig(AppConfig):
    """Configuración del módulo de Exportación Contabilidad"""
    
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'integration_service.contabilidad'
    verbose_name = 'Exportación Contabilidad'
    
    def ready(self):
        """Inicialización de la aplicación"""
        # Importar señales si es necesario
        try:
            from . import signals
        except ImportError:
            pass
