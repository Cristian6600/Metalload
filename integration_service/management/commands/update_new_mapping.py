from django.core.management.base import BaseCommand
from integration_service.models import ExportConfig
import json


class Command(BaseCommand):
    help = 'Actualiza mapeo de columnas con nuevos campos de la API'
    
    def handle(self, *args, **options):
        # Nuevo mapeo proporcionado por el usuario
        nuevo_mapeo = {
            "CUENTA 1": "tarjeta",
            "UNICO": "seudo_bd",
            "PROCESO": "tipo_entrega",
            "NIT": "cc",
            "NOMBRE": "nombre",
            "DIR RESIDENCIA": "direccion",
            "CIUDAD": "ciudad_nombre",
            "CD CIUDAD": "ciudad",
            "TELEFÓNO": "telefono",
            "HORA ENTREGA": "marcacion",
            "FECHA INGRESO": "fecha_ingreso_fisico",
            "TOTAL LLAMADAS": "total_llamadas",
            "SALIDAS EN FRIO": "salidas_en_frio",
            "VISITAS": "visitas",
            "FECHA CARGUE SISTEMA": "fecha",
            "FECHA REPORTE FINAL": "fecha_reporte_final",
            "ESTADO": "estado",
            "MOTIVOS RECHAZO Y DEVUELTAS": "motivos_rechazo_devuelta",
            "ESTADO GESTION TELEFONICA": "estado_gestion_telefonica",
            "RESULTADO GESTION TELEFONICA": "resultado_gestion_telefonica",
            "FECHA MARCACION TELEFONICA": "fecha_marcacion_telefonica",
            "BIOMETRIA": "biometria",
            "CD PROCESO": "cd_proceso",
            "DESCRIPCIÓN PROCESO": "descripcion_proceso"
        }
        
        # Mantener el mismo orden de columnas
        orden_columnas = [
            "CUENTA 1", "UNICO", "PROCESO", "NIT", "NOMBRE", "DIR RESIDENCIA", 
            "CIUDAD", "CD CIUDAD", "TELEFÓNO", "HORA ENTREGA", "FECHA INGRESO", 
            "TOTAL LLAMADAS", "SALIDAS EN FRIO", "VISITAS", "FECHA CARGUE SISTEMA", 
            "FECHA REPORTE FINAL", "ESTADO", "MOTIVOS RECHAZO Y DEVUELTAS", 
            "ESTADO GESTION TELEFONICA", "RESULTADO GESTION TELEFONICA", 
            "FECHA MARCACION TELEFONICA", "BIOMETRIA", "CD PROCESO", "DESCRIPCIÓN PROCESO"
        ]
        
        # Actualizar configuración SERFINANZA
        config = ExportConfig.objects.get(client_code="SERFINANZA")
        
        config.column_mapping = nuevo_mapeo
        config.column_order = orden_columnas
        
        config.save()
        
        self.stdout.write(
            self.style.SUCCESS('✅ Mapeo actualizado con nuevos campos de la API')
        )
        
        self.stdout.write('\n📋 Nuevo mapeo de columnas:')
        for header, api_field in nuevo_mapeo.items():
            self.stdout.write(f'   {header} → {api_field}')
        
        self.stdout.write(f'\n🎯 Total de columnas: {len(nuevo_mapeo)}')
        self.stdout.write(f'📊 Orden mantenido: {len(orden_columnas)} columnas')
        
        self.stdout.write(
            self.style.SUCCESS('\n🚀 Configuración actualizada! Prueba la exportación.')
        )
