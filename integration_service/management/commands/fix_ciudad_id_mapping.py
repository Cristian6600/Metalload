from django.core.management.base import BaseCommand
from integration_service.models import ClientMapping


class Command(BaseCommand):
    help = 'Corrige mapeo para incluir ciudad_id en lugar de ciudad'
    
    def handle(self, *args, **options):
        # Mapeo CORRECTO con ciudad_id
        correct_mapping = {
            # Campos que espera la API
            "cc": "NIT",                    
            "documento": "COD",              
            "nombre": "NOMBRE",             
            "direccion": "DIR RESIDENCIA",      
            "telefono": "TEL RESIDENCIA",       
            "ciudad_id": "CIUDAD RESIDENCIA",   # ← CAMBIO CLAVE
            "id_cliente": "REMESA",            
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
            mapping.mapping_config = correct_mapping
            mapping.save()
            
            self.stdout.write(
                self.style.SUCCESS('✅ Mapeo corregido para usar ciudad_id')
            )
            
            self.stdout.write('\n📋 Mapeo actualizado:')
            for api_field, excel_field in correct_mapping.items():
                self.stdout.write(f'   {api_field} ← {excel_field}')
                
        except ClientMapping.DoesNotExist:
            self.stdout.write(
                self.style.ERROR('❌ No existe mapeo para CLIENTE_REMESA')
            )
