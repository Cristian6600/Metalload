from django.core.management.base import BaseCommand
from integration_service.models import ExportConfig
import json


class Command(BaseCommand):
    help = 'Corrige el mapeo de columnas para usar campos reales de la API'
    
    def handle(self, *args, **options):
        # Mapeo CORRECTO usando campos reales de la API
        correct_mapping = {
            # Nombre columna Excel -> Campo API REAL
            "pseudo_id": "seudo_bd",
            "cliente_id": "id_clie", 
            "nombre_completo": "nombre",
            "apellidos": "surname",
            "documento": "cc",
            "tipo_doc": "documento",
            "ciudad_cod": "ciudad",
            "ciudad_nombre": "ciudad_nombre",
            "producto": "nom_pro",
            "direccion": "direccion",  # Este campo no está en la API, quedará vacío
            "barrio": "barrio",       # Este campo no está en la API, quedará vacío
            "telefono": "telefono",   # Este campo no está en la API, quedará vacío
            "celular": "celular",     # Este campo no está en la API, quedará vacío
            "referencia": "referencia", # Este campo no está en la API, quedará vacío
            "tarjeta": "tarjeta",     # Este campo no está en la API, quedará vacío
            "marcacion": "marcacion", # Este campo no está en la API, quedará vacío
            "convenio": "convenio",   # Este campo no está en la API, quedará vacío
            "tipo_entrega": "tipo_entrega" # Este campo no está en la API, quedará vacío
        }
        
        # Orden de columnas
        column_order = [
            "pseudo_id", "cliente_id", "nombre_completo", "apellidos", 
            "documento", "tipo_doc", "ciudad_cod", "ciudad_nombre", "producto",
            "direccion", "barrio", "telefono", "celular",
            "referencia", "tarjeta", "marcacion", "convenio", "tipo_entrega"
        ]
        
        # Actualizar configuración SERFINANZA
        config = ExportConfig.objects.get(client_code="SERFINANZA")
        
        config.column_mapping = correct_mapping
        config.column_order = column_order
        
        # Actualizar filtros para usar id_clie=3
        config.default_filters = {"id_clie": 3}
        
        config.save()
        
        self.stdout.write(
            self.style.SUCCESS('✅ Mapeo corregido para SERFINANZA')
        )
        
        self.stdout.write('\n📊 Campos REALES de la API:')
        api_fields = ['seudo_bd', 'id_clie', 'nombre', 'surname', 'cc', 'documento', 'ciudad', 'ciudad_nombre', 'nom_pro']
        for field in api_fields:
            self.stdout.write(f'   • {field}')
        
        self.stdout.write('\n🎯 Columnas que tendrán datos:')
        real_columns = ['pseudo_id', 'cliente_id', 'nombre_completo', 'apellidos', 'documento', 'tipo_doc', 'ciudad_cod', 'ciudad_nombre', 'producto']
        for col in real_columns:
            self.stdout.write(f'   • {col}')
        
        self.stdout.write('\n📋 Columnas que quedarán vacías (no existen en API):')
        empty_columns = ['direccion', 'barrio', 'telefono', 'celular', 'referencia', 'tarjeta', 'marcacion', 'convenio', 'tipo_entrega']
        for col in empty_columns:
            self.stdout.write(f'   • {col}')
        
        self.stdout.write(
            self.style.SUCCESS('\n🚀 Ahora prueba la exportación de nuevo!')
        )
