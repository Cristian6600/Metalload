from django.core.management.base import BaseCommand
from integration_service.models import ExportConfig
import json


class Command(BaseCommand):
    help = 'Actualiza encabezados y orden exacto para SERFINANZA'
    
    def handle(self, *args, **options):
        # Encabezados EXACTOS solicitados por el usuario
        headers_exactos = [
            "CUENTA 1", "UNICO", "PROCESO", "NIT", "NOMBRE", "DIR RESIDENCIA", 
            "CIUDAD", "CD CIUDAD", "TELEFÓNO", "HORA ENTREGA", "FECHA INGRESO", 
            "TOTAL LLAMADAS", "SALIDAS EN FRIO", "VISITAS", "FECHA CARGUE SISTEMA", 
            "FECHA REPORTE FINAL", "ESTADO", "MOTIVOS RECHAZO Y DEVUELTAS", 
            "ESTADO GESTION TELEFONICA", "RESULTADO GESTION TELEFONICA", 
            "FECHA MARCACION TELEFONICA", "BIOMETRIA", "CD PROCESO", "DESCRIPCIÓN PROCESO"
        ]
        
        # Mapeo: Encabezado Excel -> Campo API (los que existen)
        mapeo_campos = {
            "CUENTA 1": "seudo_bd",
            "UNICO": "id_clie",
            "PROCESO": "nombre", 
            "NIT": "cc",
            "NOMBRE": "nombre",
            "DIR RESIDENCIA": "direccion",
            "CIUDAD": "ciudad_nombre",
            "CD CIUDAD": "ciudad",
            "TELEFÓNO": "telefono",
            "HORA ENTREGA": "hora_entrega",
            "FECHA INGRESO": "fecha_ingreso",
            "TOTAL LLAMADAS": "total_llamadas",
            "SALIDAS EN FRIO": "salidas_en_frio",
            "VISITAS": "visitas",
            "FECHA CARGUE SISTEMA": "fecha_cargue_sistema",
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
        
        # Campos que REALMENTE existen en la API
        campos_api_reales = ['seudo_bd', 'id_clie', 'nombre', 'surname', 'cc', 'documento', 'ciudad', 'ciudad_nombre', 'nom_pro']
        
        # Actualizar configuración SERFINANZA
        config = ExportConfig.objects.get(client_code="SERFINANZA")
        
        config.column_mapping = mapeo_campos
        config.column_order = headers_exactos
        
        config.save()
        
        self.stdout.write(
            self.style.SUCCESS('✅ Encabezados actualizados para SERFINANZA')
        )
        
        self.stdout.write('\n📋 Encabezados configurados (24 columnas):')
        for i, header in enumerate(headers_exactos, 1):
            self.stdout.write(f'   {i:2d}. {header}')
        
        self.stdout.write('\n🎯 Campos que tendrán datos (API real):')
        for header in headers_exactos:
            api_field = mapeo_campos.get(header)
            if api_field in campos_api_reales:
                self.stdout.write(f'   ✅ {header} → {api_field}')
        
        self.stdout.write('\n📋 Campos que quedarán vacíos (no existen en API):')
        for header in headers_exactos:
            api_field = mapeo_campos.get(header)
            if api_field not in campos_api_reales:
                self.stdout.write(f'   📝 {header} → {api_field}')
        
        self.stdout.write(
            self.style.SUCCESS('\n🚀 Ahora exporta con los encabezados exactos!')
        )
