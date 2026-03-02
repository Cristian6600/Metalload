from django.core.management.base import BaseCommand
from integration_service.models import ClientMapping


class Command(BaseCommand):
    help = 'Corrige mapeo con campos reales del Excel'
    
    def handle(self, *args, **options):
        # Mapeo CORRECTO basado en el Excel real
        real_mapping = {
            # Campos requeridos por la API
            "seudo_bd": "CUENTA 1",
            "id_clie": "REMESA", 
            "nombre": "NOMBRE",
            "ciudad": "CIUDAD RESIDENCIA",  # ← CAMBIO CLAVE
            
            # Otros campos del Excel
            "cc": "NIT",
            "documento": "COD",
            "direccion": "DIR RESIDENCIA",
            "barrio": "BARRIO",
            "telefono": "TEL RESIDENCIA",
            "celular": "CELULAR",
            "ciudad_nombre": "CIUDAD RESIDENCIA",
            "nom_pro": "MERCADO",
            "fecha_ingreso_fisico": "FECHA DE ASIGNACION ",
            "cuenta2": "CUENTA 2",
            "sec": "SEC",
            "dir_oficina": "DIR OFICINA",
            "ciudad_oficina": "CIUDAD OFICINA",
            "tel_oficina": "TEL OFICINA",
            "fecha_entrega": "FECHA DE ENTREGA ",
            "tel_entrega": "TEL ENTREGA",
            "direc_entrega": "DIREC ENTREGA",
            "hra_entrega": "HRA ENTREGA"
        }
        
        # Actualizar mapeo para CLIENTE_REMESA
        try:
            mapping = ClientMapping.objects.get(client_code='CLIENTE_REMESA')
            mapping.mapping_config = real_mapping
            mapping.save()
            
            self.stdout.write(
                self.style.SUCCESS('✅ Mapeo corregido con campos reales del Excel')
            )
            
            self.stdout.write('\n📋 Mapeo actualizado:')
            for api_field, excel_field in real_mapping.items():
                self.stdout.write(f'   {api_field} ← {excel_field}')
                
        except ClientMapping.DoesNotExist:
            self.stdout.write(
                self.style.ERROR('❌ No existe mapeo para CLIENTE_REMESA')
            )
        
        self.stdout.write(
            self.style.SUCCESS('\n🚀 Ahora la API recibirá los campos correctos del Excel')
        )
