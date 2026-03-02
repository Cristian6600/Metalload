from django.core.management.base import BaseCommand
from integration_service.models import ClientMapping


class Command(BaseCommand):
    help = 'Corrige mapeo con campos exactos de la API real'
    
    def handle(self, *args, **options):
        # Mapeo CORRECTO basado en Postman y Excel
        api_mapping = {
            # Campos exactos que espera la API
            "cc": "NIT",                    # ← NIT del Excel
            "documento": "COD",              # ← COD del Excel  
            "nombre": "NOMBRE",             # ← NOMBRE del Excel
            "direccion": "DIR RESIDENCIA",  # ← DIR RESIDENCIA del Excel
            "telefono": "TEL RESIDENCIA",   # ← TEL RESIDENCIA del Excel
            "ciudad": "CIUDAD RESIDENCIA",  # ← CIUDAD RESIDENCIA del Excel
            "id_cliente": "REMESA",        # ← REMESA del Excel (generar ID único)
            
            # Campos adicionales del Excel (no requeridos pero útiles)
            "cuenta1": "CUENTA 1",
            "cuenta2": "CUENTA 2", 
            "sec": "SEC",
            "barrio": "BARRIO",
            "celular": "CELULAR",
            "dir_oficina": "DIR OFICINA",
            "ciudad_oficina": "CIUDAD OFICINA",
            "tel_oficina": "TEL OFICINA",
            "mercado": "MERCADO",
            "fecha_asignacion": "FECHA DE ASIGNACION ",
            "fecha_entrega": "FECHA DE ENTREGA ",
            "tel_entrega": "TEL ENTREGA",
            "direc_entrega": "DIREC ENTREGA",
            "hra_entrega": "HRA ENTREGA"
        }
        
        # Actualizar mapeo para CLIENTE_REMESA
        try:
            mapping = ClientMapping.objects.get(client_code='CLIENTE_REMESA')
            mapping.mapping_config = api_mapping
            mapping.save()
            
            self.stdout.write(
                self.style.SUCCESS('✅ Mapeo corregido con campos exactos de la API')
            )
            
            self.stdout.write('\n📋 Mapeo actualizado:')
            for api_field, excel_field in api_mapping.items():
                self.stdout.write(f'   {api_field} ← {excel_field}')
                
        except ClientMapping.DoesNotExist:
            self.stdout.write(
                self.style.ERROR('❌ No existe mapeo para CLIENTE_REMESA')
            )
        
        self.stdout.write(
            self.style.SUCCESS('\n🚀 Ahora la API recibirá los campos exactos que espera')
        )
        self.stdout.write(
            self.style.SUCCESS('📍 Endpoint: /api/bdclie/asignar/')
        )
