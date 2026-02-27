from rest_framework import serializers
from .models import ClientFile, ProcessingLog, ClientMapping, Report


class ClientFileSerializer(serializers.ModelSerializer):
    """Serializer para archivos de clientes"""
    file_size_display = serializers.SerializerMethodField()
    status_display = serializers.SerializerMethodField()
    
    class Meta:
        model = ClientFile
        fields = [
            'id', 'client_code', 'file', 'original_filename', 
            'file_size', 'file_size_display', 'status', 'status_display',
            'uploaded_at', 'processed_at', 'error_message'
        ]
        read_only_fields = [
            'id', 'file_size', 'uploaded_at', 'processed_at', 
            'error_message', 'status_display', 'file_size_display'
        ]
    
    def get_file_size_display(self, obj):
        """Formatea el tamaño del archivo"""
        size = obj.file_size
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} TB"
    
    def get_status_display(self, obj):
        """Retorna el texto del status"""
        return dict(ClientFile.STATUS_CHOICES).get(obj.status, obj.status)
    
    def validate_file(self, value):
        """Valida el archivo subido"""
        # Validar tamaño máximo (10MB)
        max_size = 10 * 1024 * 1024
        if value.size > max_size:
            raise serializers.ValidationError(f"El archivo no puede ser mayor a {max_size/(1024*1024):.1f}MB")
        
        return value


class ClientFileUploadSerializer(serializers.ModelSerializer):
    """Serializer simplificado para subida de archivos"""
    
    class Meta:
        model = ClientFile
        fields = ['client_code', 'file']
    
    def validate_client_code(self, value):
        """Valida que exista configuración para el cliente"""
        if not ClientMapping.objects.filter(client_code=value, is_active=True).exists():
            raise serializers.ValidationError(f"No hay configuración activa para el cliente '{value}'")
        return value
    
    def create(self, validated_data):
        """Crea el registro con datos adicionales"""
        file = validated_data['file']
        validated_data['original_filename'] = file.name
        validated_data['file_size'] = file.size
        return super().create(validated_data)


class ProcessingLogSerializer(serializers.ModelSerializer):
    """Serializer para logs de procesamiento"""
    level_display = serializers.SerializerMethodField()
    
    class Meta:
        model = ProcessingLog
        fields = ['id', 'level', 'level_display', 'message', 'details', 'created_at']
        read_only_fields = ['id', 'created_at', 'level_display']
    
    def get_level_display(self, obj):
        """Retorna el texto del nivel"""
        return dict(ProcessingLog.LOG_LEVELS).get(obj.level, obj.level)


class ClientMappingSerializer(serializers.ModelSerializer):
    """Serializer para mapeos de clientes"""
    
    class Meta:
        model = ClientMapping
        fields = [
            'id', 'client_code', 'mapping_config', 'validation_rules', 
            'is_active', 'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']
    
    def validate_mapping_config(self, value):
        """Valida la configuración de mapeo"""
        if not isinstance(value, dict):
            raise serializers.ValidationError("La configuración debe ser un objeto JSON")
        
        # Validar que tenga los campos básicos para Bd_clie
        required_fields = ['seudo_bd', 'id_clie', 'nombre', 'surname', 'cc', 'documento']
        for field in required_fields:
            if field not in value:
                raise serializers.ValidationError(f"El campo '{field}' es requerido en el mapeo")
        
        return value


class ReportSerializer(serializers.ModelSerializer):
    """Serializer para reportes"""
    report_type_display = serializers.SerializerMethodField()
    
    class Meta:
        model = Report
        fields = [
            'id', 'report_type', 'report_type_display', 'title', 
            'description', 'data', 'generated_at'
        ]
        read_only_fields = ['id', 'generated_at', 'report_type_display']
    
    def get_report_type_display(self, obj):
        """Retorna el texto del tipo de reporte"""
        return dict(Report.REPORT_TYPES).get(obj.report_type, obj.report_type)


class FileProcessingRequestSerializer(serializers.Serializer):
    """Serializer para solicitar procesamiento de archivo"""
    client_file_id = serializers.UUIDField()
    
    def validate_client_file_id(self, value):
        """Valida que el archivo exista y esté en estado pendiente"""
        try:
            client_file = ClientFile.objects.get(id=value)
            if client_file.status != 'pending':
                raise serializers.ValidationError("El archivo ya fue procesado o está en proceso")
            return value
        except ClientFile.DoesNotExist:
            raise serializers.ValidationError("Archivo no encontrado")
