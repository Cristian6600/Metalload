from django.core.management.base import BaseCommand
from integration_service.models import ClientMapping


class Command(BaseCommand):
    help = 'Corrige mapeo para enviar campos originales a la API'
    
    def handle(self, *args, **options):
        # Mapeo CORRECTO - campos originales del Excel → campos de la API
        correct_mapping = {
            # Campo API → Campo Excel (originales)
            "seudo_bd": "CUENTA 1",
            "id_clie": "REMESA", 
            "nombre": "NOMBRE",
            "ciudad": "CIUDAD",
            "cc": "NIT",
            "documento": "COD",
            "direccion": "DIR RESIDENCIA",
            "barrio": "BARRIO",
            "telefono": "TEL RESIDENCIA",
            "celular": "CELULAR",
            "ciudad_nombre": "CIUDAD RESIDENCIA",  # Para nombre completo
            "nom_pro": "MERCADO",
            "fecha_ingreso_fisico": "FECHA DE ASIGNACION",
            "total_llamadas": "TOTAL LLAMADAS",
            "salidas_en_frio": "SALIDAS EN FRIO",
            "visitas": "VISITAS",
            "fecha": "FECHA CARGUE SISTEMA",
            "fecha_reporte_final": "FECHA REPORTE FINAL",
            "estado": "ESTADO",
            "motivos_rechazo_devuelta": "MOTIVOS RECHAZO Y DEVUELTAS",
            "estado_gestion_telefonica": "ESTADO GESTION TELEFONICA",
            "resultado_gestion_telefonica": "RESULTADO GESTION TELEFONICA",
            "fecha_marcacion_telefonica": "FECHA MARCACION TELEFONICA",
            "biometria": "BIOMETRIA",
            "cd_proceso": "CD PROCESO",
            "descripcion_proceso": "DESCRIPCIÓN PROCESO",
            "archivo": "SEC",

        }
        
        # Actualizar mapeo para CLIENTE_REMESA
        try:
            mapping = ClientMapping.objects.get(client_code='CLIENTE_REMESA')
            mapping.mapping_config = correct_mapping
            mapping.save()
            
            self.stdout.write(
                self.style.SUCCESS('✅ Mapeo corregido para CLIENTE_REMESA')
            )
            
            self.stdout.write('\n📋 Mapeo actualizado:')
            for api_field, excel_field in correct_mapping.items():
                self.stdout.write(f'   {api_field} ← {excel_field}')
                
        except ClientMapping.DoesNotExist:
            self.stdout.write(
                self.style.ERROR('❌ No existe mapeo para CLIENTE_REMESA')
            )
        
        self.stdout.write(
            self.style.SUCCESS('\n🚀 Ahora la API recibirá los campos correctos')
        )
