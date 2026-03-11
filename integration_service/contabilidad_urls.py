"""
URLs para Exportación Contabilidad
"""
from django.urls import path
from . import contabilidad_views

app_name = 'contabilidad'

urlpatterns = [
    # Exportación directa desde admin
    path('export/now/<uuid:config_id>/', contabilidad_views.export_contabilidad_admin, name='export_now'),
    
    # Historial de exportaciones
    path('history/<uuid:config_id>/', contabilidad_views.get_contabilidad_exports_history, name='history'),
    path('history/', contabilidad_views.get_contabilidad_exports_history, name='history_all'),
]
