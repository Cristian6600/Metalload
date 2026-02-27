from django.core.management.base import BaseCommand
from integration_service.models import ExportConfig
import json


class Command(BaseCommand):
    help = 'Crea configuración de exportación para SERFINANZA'
    
    def handle(self, *args, **options):
        # Configuración específica para SERFINANZA
        serfinanza_config = {
            "client_code": "SERFINANZA",
            "client_name": "Serfinanza",
            "description": "Configuración de exportación para cliente Serfinanza",
            
            # Mapeo de columnas: Nombre columna Excel → Campo API
            "column_mapping": {
                "pseudo_id": "seudo_bd",
                "cliente_id": "id_clie", 
                "nombre_completo": "nombre",
                "apellidos": "surname",
                "documento": "cc",
                "tipo_doc": "documento",
                "ciudad_cod": "ciudad",
                "producto": "nom_pro",
                "direccion": "direccion",
                "barrio": "barrio",
                "telefono": "telefono",
                "celular": "celular",
                "referencia": "referencia",
                "tarjeta": "tarjeta",
                "marcacion": "marcacion",
                "convenio": "convenio",
                "tipo_entrega": "tipo_entrega"
            },
            
            # Orden de columnas en el Excel
            "column_order": [
                "pseudo_id", "cliente_id", "nombre_completo", "apellidos", 
                "documento", "tipo_doc", "ciudad_cod", "producto",
                "direccion", "barrio", "telefono", "celular",
                "referencia", "tarjeta", "marcacion", "convenio", "tipo_entrega"
            ],
            
            # Formato de exportación
            "export_format": "xlsx",
            
            # Configuración específica de Excel
            "excel_config": {
                "header_style": "bold",
                "header_color": "#366092",
                "header_font_color": "#FFFFFF",
                "auto_width": True,
                "freeze_header": True,
                "filter_buttons": True
            },
            
            # Filtros por defecto
            "default_filters": {
                "id_clie": 3  # Para SERFINANZA
            },
            
            # Transformaciones de datos
            "transformations": {
                "ciudad_cod": "left_pad_5",  # Asegurar 5 dígitos para códigos DANE
                "documento": "upper",  # Documento en mayúsculas
                "nombre_completo": "upper"  # Nombres en mayúsculas
            }
        }
        
        # Crear o actualizar configuración
        config, created = ExportConfig.objects.update_or_create(
            client_code="SERFINANZA",
            defaults=serfinanza_config
        )
        
        if created:
            self.stdout.write(
                self.style.SUCCESS(f'✅ Configuración creada para {config.client_name}')
            )
        else:
            self.stdout.write(
                self.style.WARNING(f'⚠️ Configuración actualizada para {config.client_name}')
            )
        
        # Mostrar resumen
        self.stdout.write(f"\n📊 Resumen de configuración:")
        self.stdout.write(f"   Cliente: {config.client_code} - {config.client_name}")
        self.stdout.write(f"   Formato: {config.export_format}")
        self.stdout.write(f"   Columnas mapeadas: {len(config.column_mapping)}")
        self.stdout.write(f"   Transformaciones: {len(config.transformations)}")
        self.stdout.write(f"   Activo: {config.is_active}")
        
        self.stdout.write(
            self.style.SUCCESS('\n🚀 Configuración lista para usar!')
        )
