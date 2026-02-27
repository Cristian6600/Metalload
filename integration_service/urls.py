from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

router = DefaultRouter()
router.register(r'files', views.ClientFileViewSet, basename='client-files')
router.register(r'mappings', views.ClientMappingViewSet, basename='client-mappings')
router.register(r'logs', views.ProcessingLogViewSet, basename='processing-logs')
router.register(r'reports', views.ReportViewSet, basename='reports')

urlpatterns = [
    path('api/v1/integration/', include(router.urls)),
    path('api/v1/integration/batch-process/', views.BatchProcessingView.as_view(), name='batch-process'),
    
    # URLs de exportación
    path('clientes/export/', views.export_client_data, name='export-client-data'),
    path('admin/export/configs/', views.export_configs_list, name='export-configs-list'),
    path('admin/export/history/', views.export_history_list, name='export-history-list'),
    
    # URLs de botones del admin
    path('admin/export/now/<uuid:config_id>/', views.export_now_admin, name='export-now-admin'),
    path('admin/export/download/<uuid:history_id>/', views.download_export_admin, name='download-export-admin'),
]
