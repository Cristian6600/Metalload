"""
Serializers para Exportación Contabilidad
"""
from rest_framework import serializers
from .contabilidad_models import ContabilidadExportConfig, ContabilidadExportHistory


class ContabilidadExportConfigSerializer(serializers.ModelSerializer):
    """
    Serializer para Configuración de Exportación Contabilidad
    """
    class Meta:
        model = ContabilidadExportConfig
        fields = '__all__'
    
    def validate(self, data):
        """Validar configuración"""
        # Validar que tenga los campos básicos
        if not data.get('column_mapping'):
            raise serializers.ValidationError("El mapeo de columnas es requerido")
        
        # Validar que tenga filtros con id_clie
        default_filters = data.get('default_filters', {})
        if not default_filters.get('id_clie'):
            raise serializers.ValidationError("Se requiere id_clie en los filtros por defecto")
        
        return data


class ContabilidadExportHistorySerializer(serializers.ModelSerializer):
    """
    Serializer para Historial de Exportaciones Contabilidad
    """
    export_config_name = serializers.CharField(source='export_config.client_name', read_only=True)
    
    class Meta:
        model = ContabilidadExportHistory
        fields = '__all__'
