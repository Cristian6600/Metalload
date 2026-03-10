from datetime import datetime, date
import logging
import os
import uuid
from pathlib import Path
import requests
import pandas as pd
from django.conf import settings
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils.dataframe import dataframe_to_rows
import logging

logger = logging.getLogger(__name__)


class ExportService:
    """Servicio de exportación de datos de clientes"""
    
    def __init__(self):
        self.api_base_url = getattr(settings, 'EXPORT_API_BASE_URL', 'https://diotest.letran.com.co')
        self.export_dir = Path(settings.MEDIA_ROOT) / 'exports'
        self.export_dir.mkdir(exist_ok=True)
        
        # Desactivar verificación SSL para servidores de prueba
        self.session = requests.Session()
        self.session.verify = False
        
        # 🔥 TABLA DE TRADUCCIÓN DE PROCESOS
        self.proceso_descriptions = {
            '01': 'TCO (SOLA)',
            '01 FGA': 'TCO (SOLA) + FGA',
            '02': '"COMBO" (TCO+ TARJETA DÉBITO)',
            '02 FGA': '"COMBO" (TCO+ TARJETA DÉBITO) +FGA',
            '02-1': 'RECOLECCION DOC CUENTA AHORRO  +  ENTREGA TCO',
            '02-1 FGA': 'RECOLECCION DOC CUENTA AHORRO  + ENTREGA TCO +FGA',
            '03': 'ENTREGA TARJETA DEBITO',
            '15': 'ENTREGA CERTIFICADA',
            '04': 'MIGRACION',
            '05': 'REXP ROBO',
            '06': 'RENOVACION',  # 🔥 AGREGADO 06
            '17': 'ENTREGA SIN DOCUMENTOS CON BIOMETRIA'
        }
    
    def get_proceso_description(self, proceso_code: str) -> str:
        """🔥 Obtener descripción del proceso según el código"""
        if not proceso_code:
            return ''
        
        # Buscar coincidencia exacta primero
        if proceso_code in self.proceso_descriptions:
            return self.proceso_descriptions[proceso_code]
        
        # Si no encuentra, devolver el código original
        return proceso_code
    
    def fetch_client_data(self, client_id: int) -> dict:
        """
        Obtiene datos de clientes desde la API externa
        
        Args:
            client_id: ID del cliente a consultar
            
        Returns:
            dict: Respuesta de la API
        """
        try:
            # 🔥 USAR EL ENDPOINT CORRECTO DE LA API PARA EXPORTAR
            url = f"{self.api_base_url}/clientes/export/?id_clie={client_id}"
            
            # 🔥 AGREGAR AUTORIZACIÓN
            api_key = getattr(settings, 'MAIN_API_KEY', '')
            headers = {
                'Authorization': f'Token {api_key}',
                'Content-Type': 'application/json'
            }
            
            response = self.session.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Datos obtenidos para cliente {client_id}: {len(data)} registros")
                
                # 🔥 FILTRAR LOCALMENTE por motivo_operacion si está en los filtros
                if hasattr(self, 'current_filters') and 'motivo_operacion' in self.current_filters:
                    motivo_filtro = self.current_filters['motivo_operacion']
                    data = [record for record in data if record.get('motivo_operacion') == motivo_filtro]
                    logger.info(f"🔍 Filtrados {len(data)} registros por motivo_operacion='{motivo_filtro}'")
                
                # 🔥 FILTRAR POR FECHA DEL DÍA ANTERIOR si está en los filtros
                if hasattr(self, 'current_filters') and 'fecha_estado' in self.current_filters:
                    fecha_filtro = self.current_filters['fecha_estado']
                    if fecha_filtro == 'dia_anterior':
                        # Obtener fecha del día anterior
                        from datetime import datetime, timedelta
                        ayer = datetime.now() - timedelta(days=1)
                        fecha_ayer = ayer.strftime('%Y-%m-%d')
                        
                        # Filtrar por fecha_estado
                        data = [record for record in data if record.get('fecha_estado') == fecha_ayer]
                        logger.info(f"🔍 Filtrados {len(data)} registros por fecha_estado del día anterior: {fecha_ayer}")
                    elif fecha_filtro == 'hoy':
                        # Obtener fecha de hoy
                        from datetime import datetime
                        hoy = datetime.now().strftime('%Y-%m-%d')
                        
                        # Filtrar por fecha_estado
                        data = [record for record in data if record.get('fecha_estado') == hoy]
                        logger.info(f"🔍 Filtrados {len(data)} registros por fecha_estado de hoy: {hoy}")
                    else:
                        # Filtrar por fecha específica
                        data = [record for record in data if record.get('fecha_estado') == fecha_filtro]
                        logger.info(f"🔍 Filtrados {len(data)} registros por fecha_estado: {fecha_filtro}")
                
                return data
            else:
                logger.error(f"Error en API: {response.status_code}")
                return []
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error conectando con API: {e}")
            return []
        except Exception as e:
            logger.error(f"Error inesperado obteniendo datos: {e}")
            return []
    
    def apply_transformations(self, data: list, transformations: dict) -> list:
        """
        Aplica transformaciones a los datos
        
        Args:
            data: Lista de registros
            transformations: Diccionario de reglas de transformación
            
        Returns:
            list: Datos transformados
        """
        transformed_data = []
        
        for record in data:
            new_record = record.copy()
            
            for field, rule in transformations.items():
                if field in new_record:
                    value = new_record[field]
                    
                    # Transformaciones comunes
                    if rule == 'left_pad_5' and isinstance(value, str):
                        new_record[field] = value.zfill(5)
                    elif rule == 'upper' and isinstance(value, str):
                        new_record[field] = value.upper()
                    elif rule == 'lower' and isinstance(value, str):
                        new_record[field] = value.lower()
                    elif rule == 'strip' and isinstance(value, str):
                        new_record[field] = value.strip()
            
            transformed_data.append(new_record)
        
        return transformed_data
    
    def map_columns(self, data: list, column_mapping: dict, column_order: list) -> list:
        """
        Mapea y ordena columnas según configuración
        Fuerza todas las columnas configuradas aunque no tengan datos
        Agrega fecha actual para columnas especiales
        
        Args:
            data: Lista de registros
            column_mapping: Mapeo columna_excel -> campo_api
            column_order: Orden de columnas
            
        Returns:
            list: Datos mapeados y ordenados
        """
        mapped_data = []
        
        # Obtener fecha actual
        fecha_actual = date.today().strftime('%Y-%m-%d')
        
        for record in data:
            mapped_record = {}
            
            # Aplicar mapeo: columna_excel -> campo_api
            for excel_col, api_field in column_mapping.items():
                if api_field in record and record[api_field] is not None:
                    mapped_record[excel_col] = record[api_field]
                else:
                    # Forzar columna aunque no tenga datos
                    mapped_record[excel_col] = ""
            
            # Casos especiales: agregar valores automáticos
            if "FECHA REPORTE FINAL" in column_order:
                mapped_record["FECHA REPORTE FINAL"] = fecha_actual
            
            # Aplicar orden e incluir TODAS las columnas
            ordered_record = {}
            for col in column_order:
                if col in mapped_record:
                    ordered_record[col] = mapped_record[col]
                else:
                    # Forzar columna en orden aunque no esté en mapeo
                    ordered_record[col] = ""
            
            mapped_data.append(ordered_record)
        
        return mapped_data
    
    def export_to_excel(self, data: list, filename: str, config: dict = None) -> str:
        """
        Exporta datos a Excel con formato personalizado
        
        Args:
            data: Lista de registros
            filename: Nombre del archivo
            config: Configuración de formato Excel
            
        Returns:
            str: Ruta del archivo generado
        """
        if not data:
            raise ValueError("No hay datos para exportar")
        
        # 🔥 PROCESAR DATOS ANTES DE EXPORTAR
        processed_data = []
        for record in data:
            processed_record = record.copy()
            
            # 🔥 DEBUG: Ver qué campos vienen en el registro
            logger.info(f"🔍 DEBUG - Campos disponibles: {list(record.keys())}")
            
            # 🔥 ASIGNAR FECHA ACTUAL A FECHAREPORTE
            # from datetime import datetime
            # fecha_actual = datetime.now().strftime('%Y-%m-%d')
            # processed_record['FECHAREPORTE'] = fecha_actual
            # logger.info(f"🔅 FECHAREPORTE asignada con fecha actual: {fecha_actual}")
            
            # 🔥 DEBUG ESPECIAL: Ver valor del campo ESTADO
            estado_valor = record.get('ESTADO', 'NO_EXISTE')
            logger.info(f"🔍 DEBUG ESPECIAL: ESTADO='{estado_valor}' (tipo: {type(estado_valor)})")
            
            # 🔥 TRADUCIR DESCRIPCIÓN PROCESO según CD PROCESO
            proceso_code = None
            if 'CD PROCESO' in record:  # 🔥 BUSCAR CON ESPACIO (como viene de la API)
                proceso_code = str(record['CD PROCESO'])
                logger.info(f"🔍 DEBUG - CD PROCESO encontrado: {proceso_code}")
            elif 'cd_proceso' in record:  # 🔥 BUSCAR SIN ESPACIO (por si acaso)
                proceso_code = str(record['cd_proceso'])
                logger.info(f"🔍 DEBUG - cd_proceso encontrado: {proceso_code}")
            elif 'convenio' in record:  # 🔥 INTENTAR CON convenio
                proceso_code = str(record['convenio'])
                logger.info(f"🔍 DEBUG - usando convenio como cd_proceso: {proceso_code}")
            else:
                logger.warning(f"🔍 DEBUG - No se encontró CD PROCESO ni cd_proceso ni convenio en: {record}")
            
            if proceso_code:
                # 🔥 MODIFICAR LA COLUMNA DESCRIPCIÓN PROCESO EXISTENTE
                descripcion = self.get_proceso_description(proceso_code)
                if 'DESCRIPCIÓN PROCESO' in record:  # 🔥 BUSCAR CON TILDE Y ESPACIO
                    processed_record['DESCRIPCIÓN PROCESO'] = descripcion
                    logger.info(f"🔄 Traducción: CD PROCESO '{proceso_code}' → DESCRIPCIÓN PROCESO '{descripcion}'")
                elif 'descripcion_proceso' in record:  # 🔥 BUSCAR SIN TILDE (por si acaso)
                    processed_record['descripcion_proceso'] = descripcion
                    logger.info(f"🔄 Traducción: CD PROCESO '{proceso_code}' → descripcion_proceso '{descripcion}'")
                else:
                    logger.warning(f"⚠️ No existe la columna DESCRIPCIÓN PROCESO en el registro")
            
            # 🔥 REGLA ELIMINADA: Ya no se modifica MOTIVOS RECHAZO Y DEVUELTAS ni ESTADO
            # La columna MOTIVOS RECHAZO Y DEVUELTAS ahora trae motivo_operacion del mapeo
            # La columna ESTADO está vacía porque quitamos el mapeo


            # 🔥 NUEVA REGLA: Si MOTIVOS RECHAZO Y DEVUELTAS = Entregado
            motivos_valor = str(record.get('MOTIVOS RECHAZO Y DEVUELTAS', ''))
            proceso_valor = str(record.get('PROCESO', ''))
            calificacion_call_valor = str(record.get('ESTADO GESTION TELEFONICA', ''))
            cantidad_llamadas_valor = record.get('TOTAL LLAMADAS', 0)
            biometria_valor = str(record.get('BIOMETRIA', ''))
            visitas_valor = str(record.get('VISITAS', ''))

            # VARIABLES PARA ARCHIVO ENTREGAS
            biometria_entregas = str(record.get('RESPUESTA BIOMETRIA', ''))

            if biometria_entregas == 'PERSONALIZADA':
                processed_record['RESPUESTA BIOMETRIA'] = 'HIT'

            var_call = ['Traslado oficina', 'Cambio total', 'Complementa direccion', 'Cita futura']
            if calificacion_call_valor in var_call:
                processed_record['SALIDAS EN FRIO'] = 0

            print("--, ------------------------------------------------", motivos_valor)
            
            # 🔥 Convertir a número para comparación correcta
            try:
                cantidad_llamadas_num = int(cantidad_llamadas_valor) if cantidad_llamadas_valor else 0
            except (ValueError, TypeError):
                cantidad_llamadas_num = 0
            #####################################################
            var_operacion = ['En ruta ciudad', 'En Ruta', 'Entregado', 'Direccion no existe', 'Cambio de domicilio', 
            'Cerrado', 'Dificil acceso', 'Radicado fuera de la ciudad', 'Destruido', 'Perdida', 'Devolucion Proveedor', 
            'DOM. EN PROCESO DE DEVOLUCION', 'Destinatario Desconocido', 'Rehusado', 'Ausente', 'NO PASA VALIDACION BIOMETRICA', 
            'Direccion Incompleta']

            if calificacion_call_valor == 'No Contesta' and cantidad_llamadas_num >= 3 and proceso_valor == 'PERSONALIZADA' and motivos_valor not in var_operacion:
                processed_record['ESTADO'] = 'ILOCALIZADO'  # 🔥 MAYÚSCULAS
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'NO CONTESTA'

            elif calificacion_call_valor == 'Telefono Apagado' and cantidad_llamadas_num >= 3 and proceso_valor == 'PERSONALIZADA' and motivos_valor not in var_operacion:
                processed_record['ESTADO'] = 'ILOCALIZADO'  # 🔥 MAYÚSCULAS
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'BUZON DE VOZ'

            elif calificacion_call_valor == 'Ocupado' and cantidad_llamadas_num >= 3 and proceso_valor == 'PERSONALIZADA' and motivos_valor not in var_operacion:
                processed_record['ESTADO'] = 'ILOCALIZADO'  # 🔥 MAYÚSCULAS
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'TELEFONO OCUPADO'

            elif calificacion_call_valor == 'Falla operador telefónico' and cantidad_llamadas_num >= 3 and proceso_valor == 'PERSONALIZADA' and motivos_valor not in var_operacion:
                processed_record['ESTADO'] = 'ILOCALIZADO'  # 🔥 MAYÚSCULAS
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'TELEFONO FUERA DE SERVICIO'

            ####################################1###################################

            elif calificacion_call_valor == 'Equivocado' and cantidad_llamadas_num >= 1 and proceso_valor == 'PERSONALIZADA' and motivos_valor not in var_operacion:
                processed_record['ESTADO'] = 'ILOCALIZADO'  
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'TELEFONO ERRADO'

            elif calificacion_call_valor == 'Sin información telefónica' and cantidad_llamadas_num >= 1 and proceso_valor == 'PERSONALIZADA' and motivos_valor not in var_operacion:
                processed_record['ESTADO'] = 'ILOCALIZADO'  
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'TELEFONO ERRADO'

            elif calificacion_call_valor == 'Datos errados' and cantidad_llamadas_num >= 1 and proceso_valor == 'PERSONALIZADA' and motivos_valor not in var_operacion:
                processed_record['ESTADO'] = 'ILOCALIZADO' 
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'TELEFONO ERRADO'

            elif calificacion_call_valor == 'Radicado fuera del pais' and cantidad_llamadas_num >= 1 and proceso_valor == 'PERSONALIZADA' and motivos_valor not in var_operacion:
                processed_record['ESTADO'] = 'ILOCALIZADO'  
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'CAMBIO DE DOMICILIO'

            elif calificacion_call_valor == 'Cliente Fallecido'and cantidad_llamadas_num >= 1 and proceso_valor == 'PERSONALIZADA' and motivos_valor not in var_operacion:
                processed_record['ESTADO'] = 'DEVUELTO'  
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'CLIENTE FALLECIDO'

            elif calificacion_call_valor == 'Se comunica con el banco'and cantidad_llamadas_num >= 1 and proceso_valor == 'PERSONALIZADA' and motivos_valor not in var_operacion:
                processed_record['ESTADO'] = 'REHUSADO' 
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'NO INTERESADO POR CUOTA'

            elif calificacion_call_valor == 'Cliente cancelo el producto'and cantidad_llamadas_num >= 1 and proceso_valor == 'PERSONALIZADA' and motivos_valor not in var_operacion:
                processed_record['ESTADO'] = 'REHUSADO' 
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'MALA EXPERIENCIA CANCELO O CANCELARA'

            elif calificacion_call_valor == 'Cliente ya tiene el producto' and cantidad_llamadas_num >= 1 and proceso_valor == 'PERSONALIZADA' and motivos_valor not in var_operacion:
                processed_record['ESTADO'] = 'REHUSADO' 
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'NO INTERESADO, NO ESPECIFICA'

            elif calificacion_call_valor == 'Rehusado' and cantidad_llamadas_num >= 1 and proceso_valor == 'PERSONALIZADA' and motivos_valor not in var_operacion:
                processed_record['ESTADO'] = 'REHUSADO' 
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'NO INTERESADO, NO ESPECIFICA'


            ####################################2###################################
            
            elif motivos_valor == 'Entregado':
                processed_record['ESTADO'] = 'ENTREGADO'  # 
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'ENTREGADO'  # 

            elif motivos_valor == 'NO PASA VALIDACION BIOMETRICA':
                processed_record['ESTADO'] = 'EN GESTION'  # 
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'NO PASA VALIDACION BIOMETRICA'  

            elif motivos_valor == 'No cobertura':
                processed_record['ESTADO'] = 'DEVUELTO'  # 
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'NO CUBRIMINETO'  
                
            elif motivos_valor == 'Custodia' and proceso_valor == 'PERSONALIZADA':
                # 🔥 REGLA ESPECIAL: Custodia + PERSONALIZADA
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'EN GESTION TELEFONICA'
                processed_record['ESTADO'] = 'EN GESTION'
                logger.info(f"🔄 Regla especial: MOTIVOS RECHAZO Y DEVUELTAS='Custodia' + PROCESO='PERSONALIZADA' → 'EN GESTION TELEFONICA' + ESTADO='EN GESTION'")
            elif motivos_valor == 'Custodia' and proceso_valor == 'CERTIFICADA':
                # 🔥 REGLA ESPECIAL: Custodia + CERTIFICADA
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'EN DISTRIBUCION'
                processed_record['ESTADO'] = 'EN GESTION'
                logger.info(f"🔄 Regla especial: MOTIVOS RECHAZO Y DEVUELTAS='Custodia' + PROCESO='CERTIFICADA' → 'EN DISTRIBUCION' + ESTADO='EN GESTION'")
            elif motivos_valor == 'En Ruta' or motivos_valor == 'En ruta ciudad':
                # 🔥 REGLA ESPECIAL: En Ruta o En ruta ciudad
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'EN DISTRIBUCION'
                processed_record['ESTADO'] = 'EN GESTION'
                logger.info(f"🔄 Regla especial: MOTIVOS RECHAZO Y DEVUELTAS='En Ruta' o 'En ruta ciudad' → 'EN DISTRIBUCION' + ESTADO='EN GESTION'")
            elif motivos_valor == 'Ausente' or motivos_valor == 'Cerrado':
                
                try:
                    visitas_num = int(visitas_valor) if visitas_valor and visitas_valor.isdigit() else 0
                except (ValueError, TypeError):
                    visitas_num = 0
                    
                if visitas_num <= 2:
                    processed_record['ESTADO'] = 'EN GESTION'
                    processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'NO HAY QUIEN RECIBA'
                    logger.info(f"🔄 Regla especial: MOTIVOS RECHAZO Y DEVUELTAS='Ausente' o 'Cerrado' → 'NO HAY QUIEN RECIBA' + ESTADO='EN GESTION'")
                else:
                    processed_record['ESTADO'] = 'ILOCALIZADO'
                    processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'NO HAY QUIEN RECIBA'
                    logger.info(f"🔄 Regla especial: MOTIVOS RECHAZO Y DEVUELTAS='Ausente' o 'Cerrado' → 'NO HAY QUIEN RECIBA' + ESTADO='ILOCALIZADO'")
            elif motivos_valor == 'De viaje':
                # 🔥 REGLA ESPECIAL: De viaje
                processed_record['ESTADO'] = 'EN GESTION'
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'CLIENTE DE VIAJE'
                logger.info(f"🔄 Regla especial: MOTIVOS RECHAZO Y DEVUELTAS='De viaje' → 'CLIENTE DE VIAJE' + ESTADO='EN GESTION'")

            elif motivos_valor == 'NO VISITADO':
                # 🔥 REGLA ESPECIAL: NO VISITADO
                processed_record['ESTADO'] = 'EN GESTION'
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'INCUMPLIMIENTO DE COURRIER '

            elif motivos_valor == 'Direccion Incompleta':
                # 🔥 REGLA ESPECIAL: NO VISITADO
                processed_record['ESTADO'] = 'ILOCALIZADO'
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'DIRECCIÓN INCOMPLETA'

            elif motivos_valor == 'Direccion no existe' or motivos_valor == 'Direccion no existe o errada':
                # 🔥 REGLA ESPECIAL: NO VISITADO
                processed_record['ESTADO'] = 'ILOCALIZADO'
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'DIRECCIÓN NO EXISTE'

            elif motivos_valor == 'Cambio de domicilio' or motivos_valor == 'Radicado fuera de la ciudad':
                # 🔥 REGLA ESPECIAL: NO VISITADO
                processed_record['ESTADO'] = 'ILOCALIZADO'
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'CAMBIO DE DOMICILIO'

            elif motivos_valor == 'Dificil acceso':
                # 🔥 REGLA ESPECIAL: NO VISITADO
                processed_record['ESTADO'] = 'ILOCALIZADO'
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'DIFICIL ACCESO'

            elif motivos_valor == 'Cerrado' or motivos_valor == 'Ausente' or motivos_valor == 'Destinatario Desconocido':
                # 🔥 REGLA ESPECIAL: NO VISITADO
                processed_record['ESTADO'] = 'ILOCALIZADO'
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'NO HAY QUIEN RECIBA'

            elif motivos_valor == 'No cobertura':
                # 🔥 REGLA ESPECIAL: NO VISITADO
                processed_record['ESTADO'] = 'DEVUELTO '
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'NO CUBRIMIENTO'

            elif motivos_valor == 'Devolucion Proveedor':
                # 🔥 REGLA ESPECIAL: NO VISITADO
                processed_record['ESTADO'] = 'DEVUELTO'
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'SOLICITUD DEL BANCO'

            elif motivos_valor == 'DOM. EN PROCESO DE DEVOLUCION':
                # 🔥 REGLA ESPECIAL: NO VISITADO
                processed_record['ESTADO'] = 'DEVUELTO'
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'CLIENTE SOLICITA ENVIO A AGENCIA'

            elif motivos_valor == 'Fallecido':
                # 🔥 REGLA ESPECIAL: NO VISITADO
                processed_record['ESTADO'] = 'DEVUELTO'
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'CLIENTE FALLECIDO'

            elif motivos_valor == 'Rehusado':
                # 🔥 REGLA ESPECIAL: NO VISITADO
                processed_record['ESTADO'] = 'REHUSADO'
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'NO INTERESADO, NO ESPECIFICA'

            elif motivos_valor == 'Perdida':
                # 🔥 REGLA ESPECIAL: NO VISITADO
                processed_record['ESTADO'] = 'EXTRAVIO'
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'EXTRAVIADA EN DISTRIBUCIÓN'

            elif motivos_valor is None or motivos_valor == '' or str(motivos_valor).strip() == '':

                # 🔥 VERIFICAR SI ES SERFINANZA POR NOMBRE DE ARCHIVO
                if 'SERFINANZA_export_' in filename:
                    # 🔥 REGLA ESPECIAL: NO VISITADO (SOLO PARA SERFINANZA)
                    processed_record['ESTADO'] = 'EXTRAVIO'
                    processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'NO LLEGÓ FISICO'

            elif motivos_valor == 'Destruido':
                # 🔥 REGLA ESPECIAL: NO VISITADO
                processed_record['ESTADO'] = 'DESTRUCCIÓN'
                processed_record['MOTIVOS RECHAZO Y DEVUELTAS'] = 'DESTRUIDO SOBRE ABIERTO'
                
                
            
                
#####################################################################################################################            
            # 🔥 NUEVA REGLA: Si ESTADO GESTION TELEFONICA = Cita futura
            estado_gestion_valor = str(record.get('ESTADO GESTION TELEFONICA', ''))
            
            if estado_gestion_valor == 'Cita futura':
                processed_record['ESTADO GESTION TELEFONICA'] = 'AGENDADO'
                processed_record['RESULTADO GESTION TELEFONICA'] = 'CONTACTADO'
                logger.info(f"🔄 Regla especial: ESTADO GESTION TELEFONICA='Cita futura' → 'AGENDADO' + RESULTADO GESTION TELEFONICA='CONTACTADO'")

            elif estado_gestion_valor == 'Cambio total':
                processed_record['ESTADO GESTION TELEFONICA'] = 'AGENDADO'
                processed_record['RESULTADO GESTION TELEFONICA'] = 'CONTACTADO'
                logger.info(f"🔄 Regla especial: ESTADO GESTION TELEFONICA='Cambio total' → 'CONTACTADO' + RESULTADO GESTION TELEFONICA='AGENDADO'")

            elif estado_gestion_valor == 'Complementa direccion':
                processed_record['ESTADO GESTION TELEFONICA'] = 'AGENDADO'
                processed_record['RESULTADO GESTION TELEFONICA'] = 'CONTACTADO'
                logger.info(f"🔄 Regla especial: ESTADO GESTION TELEFONICA='Complementa dirección' → 'CONTACTADO' + RESULTADO GESTION TELEFONICA='AGENDADO'")

            elif estado_gestion_valor == 'Traslado oficina':
                processed_record['ESTADO GESTION TELEFONICA'] = 'CONTACTADO'
                processed_record['RESULTADO GESTION TELEFONICA'] = 'AGENDADO'
                logger.info(f"🔄 Regla especial: ESTADO GESTION TELEFONICA='Complementa dirección' → 'CONTACTADO' + RESULTADO GESTION TELEFONICA='AGENDADO'")

            elif estado_gestion_valor == 'No Contesta':
                processed_record['ESTADO GESTION TELEFONICA'] = 'NO CONTESTA'
                processed_record['RESULTADO GESTION TELEFONICA'] = 'NO CONTACTADO'
                logger.info(f"🔄 Regla especial: ESTADO GESTION TELEFONICA='No Contesta' → 'NO CONTESTA' + RESULTADO GESTION TELEFONICA='NO CONTACTADO'")

            elif estado_gestion_valor == 'Telefono Apagado':
                processed_record['ESTADO GESTION TELEFONICA'] = 'BUZON DE VOZ'
                processed_record['RESULTADO GESTION TELEFONICA'] = 'NO CONTACTADO'
                logger.info(f"🔄 Regla especial: ESTADO GESTION TELEFONICA='Teléfono Apagado' → 'BUZON DE VOZ' + RESULTADO GESTION TELEFONICA='NO CONTACTADO'")

            elif estado_gestion_valor == 'Volver a llamar':
                processed_record['ESTADO GESTION TELEFONICA'] = 'CLIENTE SOLICITA OTRA LLAMADA'
                processed_record['RESULTADO GESTION TELEFONICA'] = 'CONTACTADO'
                logger.info(f"🔄 Regla especial: ESTADO GESTION TELEFONICA='Volver a llamar' → 'CLIENTE SOLICITA OTRA LLAMADA' + RESULTADO GESTION TELEFONICA='CONTACTADO'")
            
            elif estado_gestion_valor == 'Equivocado':
                processed_record['ESTADO GESTION TELEFONICA'] = 'TELEFONO ERRADO'
                processed_record['RESULTADO GESTION TELEFONICA'] = 'NO CONTACTADO'
                logger.info(f" Regla especial: ESTADO GESTION TELEFONICA='Equivocado' → 'TELEFONO ERRADO' + RESULTADO GESTION TELEFONICA='NO CONTACTADO'")
            
            elif estado_gestion_valor == 'Sin información telefónica':
                processed_record['ESTADO GESTION TELEFONICA'] = 'TELEFONO ERRADO'
                processed_record['RESULTADO GESTION TELEFONICA'] = 'NO CONTACTADO'
                logger.info(f" Regla especial: ESTADO GESTION TELEFONICA='Sin información telefónica' → 'TELEFONO ERRADO' + RESULTADO GESTION TELEFONICA='NO CONTACTADO'")
            
            elif estado_gestion_valor == 'Datos errados':
                processed_record['ESTADO GESTION TELEFONICA'] = 'TELEFONO ERRADO'
                processed_record['RESULTADO GESTION TELEFONICA'] = 'NO CONTACTADO'
                logger.info(f"🔄 Regla especial: ESTADO GESTION TELEFONICA='Datos errados' → 'TELEFONO ERRADO' + RESULTADO GESTION TELEFONICA='NO CONTACTADO'")
            
            elif estado_gestion_valor == 'Falla operador telefónico':
                processed_record['ESTADO GESTION TELEFONICA'] = 'TELEFONO FUERA DE SERVICIO'
                processed_record['RESULTADO GESTION TELEFONICA'] = 'NO CONTACTADO'
                logger.info(f"🔄 Regla especial: ESTADO GESTION TELEFONICA='Falla operador telefónico' → 'TELEFONO FUERA DE SERVICIO' + RESULTADO GESTION TELEFONICA='NO CONTACTADO'")
            
            elif estado_gestion_valor == 'Ocupado':
                processed_record['ESTADO GESTION TELEFONICA'] = 'TELEFONO OCUPADO '
                processed_record['RESULTADO GESTION TELEFONICA'] = 'NO CONTACTADO'
                logger.info(f"🔄 Regla especial: ESTADO GESTION TELEFONICA='Ocupado' → 'TELEFONO OCUPADO' + RESULTADO GESTION TELEFONICA='NO CONTACTADO'")

            elif estado_gestion_valor == 'Cliente cancelo el producto':
                processed_record['ESTADO GESTION TELEFONICA'] = 'MALA EXPERIENCIA CANCELO O CANCELARA'
                processed_record['RESULTADO GESTION TELEFONICA'] = 'CONTACTADO'

            ###################################### MAS ######################

            elif estado_gestion_valor == 'Cliente cancelo el producto':
                processed_record['ESTADO GESTION TELEFONICA'] = 'MALA EXPERIENCIA CANCELO O CANCELARA'
                processed_record['RESULTADO GESTION TELEFONICA'] = 'CONTACTADO'

            elif estado_gestion_valor == 'Cliente Fallecido':
                processed_record['ESTADO GESTION TELEFONICA'] = 'CLIENTE FALLECIDO'
                processed_record['RESULTADO GESTION TELEFONICA'] = 'CONTACTADO'

            elif estado_gestion_valor == 'Rehusado':
                processed_record['ESTADO GESTION TELEFONICA'] = 'NO INTERESADO, NO ESPECIFICA'
                processed_record['RESULTADO GESTION TELEFONICA'] = 'CONTACTADO'

            elif estado_gestion_valor == 'Cliente ya tiene el producto':
                processed_record['ESTADO GESTION TELEFONICA'] = 'NO INTERESADO, NO ESPECIFICA'
                processed_record['RESULTADO GESTION TELEFONICA'] = 'CONTACTADO'

            elif estado_gestion_valor == 'Se comunica con el banco':
                processed_record['ESTADO GESTION TELEFONICA'] = 'NO INTERESADO POR CUOTA'
                processed_record['RESULTADO GESTION TELEFONICA'] = 'CONTACTADO'

            elif estado_gestion_valor == 'Radicado fuera del pais':
                processed_record['ESTADO GESTION TELEFONICA'] = 'CAMBIO DE DOMICILIO'
                processed_record['RESULTADO GESTION TELEFONICA'] = 'CONTACTADO'

            ############ COLUMNA BIOMETRIA##############################

            # 🔥 SOLO CREAR BIOMETRIA SI ES SERFINANZA POR NOMBRE DE ARCHIVO
            if 'SERFINANZA_export_' in filename:
                if biometria_valor == 'PERSONALIZADA' and motivos_valor == 'Entregado':
                    processed_record['BIOMETRIA'] = 'HIT'
                else:
                    processed_record['BIOMETRIA'] = ''
            
            processed_data.append(processed_record)
        
        # 🔥 ELIMINAR COLUMNAS NO MAPEADAS ANTES DE EXPORTAR
        if config and 'column_mapping' in config:
            column_mapping = config['column_mapping']
            # Obtener solo las columnas que están en el mapeo
            mapeo_inverso = {v: k for k, v in column_mapping.items()}
            
            for record in processed_data:
                # Eliminar columnas que no están en el mapeo
                columnas_a_eliminar = []
                for columna in record.keys():
                    if columna not in mapeo_inverso:
                        columnas_a_eliminar.append(columna)
                
                for columna in columnas_a_eliminar:
                    del record[columna]
                    logger.info(f"🗑️ Eliminada columna no mapeada: {columna}")
        
        # Convertir a DataFrame
        df = pd.DataFrame(processed_data)
        
        # Crear archivo Excel
        filepath = self.export_dir / filename
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Datos')
            
            # Aplicar formato si hay configuración
            if config:
                self._apply_excel_formatting(writer.sheets['Datos'], config)
        
        logger.info(f"Excel generado: {filepath}")
        return str(filepath)
    
    def _apply_excel_formatting(self, worksheet, config: dict):
        """Aplica formato personalizado a hoja Excel"""
        # Configuración por defecto
        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
        header_alignment = Alignment(horizontal="center", vertical="center")
        
        # Aplicar formato a encabezados
        for cell in worksheet[1]:
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = header_alignment
        
        # Autoajustar ancho de columnas
        if config.get('auto_width', True):
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        # Congelar filas superiores
        if config.get('freeze_header', True):
            worksheet.freeze_panes = 'A2'
    
    def export_to_csv(self, data: list, filename: str, delimiter: str = ',') -> str:
        """
        Exporta datos a CSV
        
        Args:
            data: Lista de registros
            filename: Nombre del archivo
            delimiter: Delimitador CSV
            
        Returns:
            str: Ruta del archivo generado
        """
        if not data:
            raise ValueError("No hay datos para exportar")
        
        df = pd.DataFrame(data)
        filepath = self.export_dir / filename
        df.to_csv(filepath, index=False, sep=delimiter, encoding='utf-8')
        
        logger.info(f"CSV generado: {filepath}")
        return str(filepath)
    
    def export_to_json(self, data: list, filename: str) -> str:
        """
        Exporta datos a JSON
        
        Args:
            data: Lista de registros
            filename: Nombre del archivo
            
        Returns:
            str: Ruta del archivo generado
        """
        if not data:
            raise ValueError("No hay datos para exportar")
        
        import json
        filepath = self.export_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"JSON generado: {filepath}")
        return str(filepath)
    
    def generate_filename(self, client_code: str, export_format: str) -> str:
        """
        Genera nombre de archivo único
        
        Args:
            client_code: Código del cliente
            export_format: Formato de exportación
            
        Returns:
            str: Nombre de archivo generado
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"{client_code}_export_{timestamp}.{export_format}"