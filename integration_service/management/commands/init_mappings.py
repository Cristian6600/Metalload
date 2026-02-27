from django.core.management.base import BaseCommand
from integration_service.models import ClientMapping
import json


class Command(BaseCommand):
    help = 'Inicializa mapeos de clientes para el servicio de integración'
    
    def handle(self, *args, **options):
        # Mapeo para CLIENTE_REMESA
        remesa_mapping = {
            "seudo_bd": "CUENTA 1",  # Corregido: usar CUENTA 1 real
            "id_clie": 17,  # Cambiado a 17 por solicitud
            "nombre": "NOMBRE",
            "surname": "",  # Valor vacío
            "cc": "NIT",
            "documento": 1,  # Valor fijo
            "ciudad": "CIUDAD RESIDENCIA",
            "nom_pro": "01",  # Valor fijo que funciona
            "referencia": "REMESA",  # Nuevo campo: usa columna REMESA
            "tarjeta": "CUENTA 1",  # Nuevo campo: usa columna CUENTA 1
            "marcacion": "HRA ENTREGA",  # Nuevo campo: usa columna HRA ENTREGA
            "convenio": "COD",  # Nuevo campo: usa columna COD
            "tipo_entrega": "COD",  # Nuevo campo: usa columna COD con lógica condicional
            "direccion": "DIR RESIDENCIA",
            "barrio": "BARRIO",
            "telefono": "CELULAR",
            "celular": "CELULAR",
            "direccion_oficina": "DIR OFICINA",
            "ciudad_oficina": "CIUDAD OFICINA",
            "telefono_oficina": "TEL OFICINA",
            "mercado": "MERCADO",
            "fecha_asignacion": "FECHA DE ASIGNACION ",
            "fecha_entrega": "FECHA DE ENTREGA ",
            "telefono_entrega": "TEL ENTREGA",
            "direccion_entrega": "DIREC ENTREGA",
            "hora_entrega": "HRA ENTREGA",
            "cuenta1": "CUENTA 1",
            "cuenta2": "CUENTA 2",
            "sec": "SEC",
            "cod": "COD"
        }
        
        remesa_validation = {
            "required_fields": ["seudo_bd", "nombre", "cc"],
            "optional_fields": ["id_clie", "direccion", "barrio", "ciudad", "telefono", "celular"]
        }
        
        # Mapeo para CLIENTE_EJEMPLO (formato estándar)
        ejemplo_mapping = {
            "seudo_bd": "BASE_DATOS",
            "id_clie": "ID_CLIENTE",
            "nombre": "NOMBRE_COMPLETO", 
            "cc": "CEDULA",
            "direccion": "DIRECCION",
            "barrio": "BARRIO",
            "ciudad": "CIUDAD",
            "telefono": "TELEFONO",
            "celular": "CELULAR"
        }
        
        ejemplo_validation = {
            "required_fields": ["seudo_bd", "nombre", "cc"],
            "optional_fields": ["id_clie", "direccion", "telefono"]
        }
        
        mappings = [
            {
                'client_code': 'CLIENTE_REMESA',
                'mapping_config': remesa_mapping,
                'validation_rules': remesa_validation
            },
            {
                'client_code': 'CLIENTE_EJEMPLO', 
                'mapping_config': ejemplo_mapping,
                'validation_rules': ejemplo_validation
            }
        ]
        
        created_count = 0
        updated_count = 0
        
        for mapping_data in mappings:
            client_code = mapping_data['client_code']
            
            mapping, created = ClientMapping.objects.update_or_create(
                client_code=client_code,
                defaults=mapping_data
            )
            
            if created:
                created_count += 1
                self.stdout.write(
                    self.style.SUCCESS(f'Creado mapeo para {client_code}')
                )
            else:
                updated_count += 1
                self.stdout.write(
                    self.style.WARNING(f'Actualizado mapeo para {client_code}')
                )
        
        self.stdout.write(
            self.style.SUCCESS(
                f'Proceso completado: {created_count} creados, {updated_count} actualizados'
            )
        )
