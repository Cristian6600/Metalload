from django.test import TestCase, override_settings
from django.core.files.uploadedfile import SimpleUploadedFile
from django.contrib.auth import get_user_model
from rest_framework.test import APITestCase
from rest_framework import status
from unittest.mock import patch, MagicMock
import json
import io
import pandas as pd

from .models import ClientFile, ClientMapping, ProcessingLog
from .services import FileProcessor, MainAPIClient, ReportGenerator

User = get_user_model()


class ClientFileModelTest(TestCase):
    """Tests para el modelo ClientFile"""
    
    def setUp(self):
        self.client_mapping = ClientMapping.objects.create(
            client_code='TEST_CLIENT',
            mapping_config={
                'seudo_bd': 'database',
                'id_clie': 'client_id',
                'nombre': 'name',
                'surname': 'last_name',
                'cc': 'id_number',
                'documento': 'document'
            }
        )
    
    def test_client_file_creation(self):
        """Prueba la creación de un archivo de cliente"""
        test_file = SimpleUploadedFile(
            "test.csv", 
            b"name,client_id\nJohn,001", 
            content_type="text/csv"
        )
        
        client_file = ClientFile.objects.create(
            client_code='TEST_CLIENT',
            file=test_file,
            original_filename='test.csv',
            file_size=test_file.size
        )
        
        self.assertEqual(client_file.client_code, 'TEST_CLIENT')
        self.assertEqual(client_file.status, 'pending')
        self.assertIsNotNone(client_file.id)
        self.assertEqual(client_file.original_filename, 'test.csv')


class MainAPIClientTest(TestCase):
    """Tests para el cliente de la API principal"""
    
    def setUp(self):
        self.api_client = MainAPIClient()
    
    @patch('requests.Session.post')
    def test_send_client_data_success(self, mock_post):
        """Prueba envío exitoso de datos a API principal"""
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {'message': 'Created successfully'}
        mock_post.return_value = mock_response
        
        client_data = [
            {
                'seudo_bd': 'test_db',
                'id_clie': '001',
                'nombre': 'John',
                'surname': 'Doe',
                'cc': '12345678',
                'documento': 'DOC001'
            }
        ]
        
        result = self.api_client.send_client_data(client_data)
        
        self.assertTrue(result['success'])
        self.assertEqual(result['status_code'], 201)
        mock_post.assert_called_once()
    
    @patch('requests.Session.post')
    def test_send_client_data_error(self, mock_post):
        """Prueba manejo de error en API principal"""
        mock_post.side_effect = Exception("Connection error")
        
        client_data = [{'seudo_bd': 'test_db'}]
        result = self.api_client.send_client_data(client_data)
        
        self.assertFalse(result['success'])
        self.assertIn('Connection error', result['error'])


class FileProcessorTest(TestCase):
    """Tests para el procesador de archivos"""
    
    def setUp(self):
        self.client_mapping = ClientMapping.objects.create(
            client_code='TEST_CLIENT',
            mapping_config={
                'seudo_bd': 'database',
                'id_clie': 'client_id',
                'nombre': 'name',
                'surname': 'last_name',
                'cc': 'id_number',
                'documento': 'document'
            },
            validation_rules={
                'required_fields': ['seudo_bd', 'id_clie', 'nombre']
            }
        )
        
        # Crear archivo CSV de prueba
        csv_content = """database,client_id,name,last_name,id_number,document
TEST_DB,001,John,Doe,12345678,DOC001
TEST_DB,002,Jane,Smith,87654321,DOC002"""
        
        self.test_file = SimpleUploadedFile(
            "test.csv",
            csv_content.encode('utf-8'),
            content_type="text/csv"
        )
        
        self.client_file = ClientFile.objects.create(
            client_code='TEST_CLIENT',
            file=self.test_file,
            original_filename='test.csv',
            file_size=len(csv_content.encode('utf-8'))
        )
        
        self.processor = FileProcessor()
    
    @patch.object(MainAPIClient, 'send_client_data')
    def test_process_file_success(self, mock_send_data):
        """Prueba procesamiento exitoso de archivo"""
        mock_send_data.return_value = {
            'success': True,
            'data': {'message': 'Data received'}
        }
        
        result = self.processor.process_file(self.client_file)
        
        self.assertTrue(result['success'])
        self.assertEqual(result['records_processed'], 2)
        
        # Verificar que el archivo fue marcado como procesado
        self.client_file.refresh_from_db()
        self.assertEqual(self.client_file.status, 'processed')
        
        # Verificar que se crearon logs
        self.assertTrue(ProcessingLog.objects.filter(client_file=self.client_file).exists())
    
    def test_transform_file(self):
        """Prueba la transformación de archivo"""
        transformed_data = self.processor._transform_file(self.client_file, self.client_mapping)
        
        self.assertEqual(len(transformed_data), 2)
        
        # Verificar primer registro
        first_record = transformed_data[0]
        self.assertEqual(first_record['seudo_bd'], 'TEST_DB')
        self.assertEqual(first_record['id_clie'], '001')
        self.assertEqual(first_record['nombre'], 'John')
        self.assertEqual(first_record['surname'], 'Doe')
    
    def test_validate_data_success(self):
        """Prueba validación exitosa de datos"""
        data = [
            {
                'seudo_bd': 'TEST_DB',
                'id_clie': '001',
                'nombre': 'John',
                'surname': 'Doe',
                'cc': '12345678',
                'documento': 'DOC001'
            }
        ]
        
        result = self.processor._validate_data(data, self.client_mapping)
        
        self.assertTrue(result['valid'])
        self.assertEqual(len(result['errors']), 0)
    
    def test_validate_data_errors(self):
        """Prueba validación con errores"""
        data = [
            {
                'seudo_bd': '',  # Campo requerido vacío
                'id_clie': '001',
                'nombre': 'John',
                'surname': 'Doe',
                'cc': '12345678',
                'documento': 'DOC001'
            }
        ]
        
        result = self.processor._validate_data(data, self.client_mapping)
        
        self.assertFalse(result['valid'])
        self.assertGreater(len(result['errors']), 0)


class ClientFileAPITest(APITestCase):
    """Tests para la API de archivos de cliente"""
    
    def setUp(self):
        self.user = User.objects.create_user(
            username='testuser',
            password='testpass123'
        )
        
        self.client_mapping = ClientMapping.objects.create(
            client_code='TEST_CLIENT',
            mapping_config={
                'seudo_bd': 'database',
                'id_clie': 'client_id',
                'nombre': 'name',
                'surname': 'last_name',
                'cc': 'id_number',
                'documento': 'document'
            }
        )
        
        self.client.force_authenticate(user=self.user)
    
    def test_upload_file_success(self):
        """Prueba subida exitosa de archivo"""
        csv_content = """database,client_id,name,last_name,id_number,document
TEST_DB,001,John,Doe,12345678,DOC001"""
        
        test_file = SimpleUploadedFile(
            "test.csv",
            csv_content.encode('utf-8'),
            content_type="text/csv"
        )
        
        data = {
            'client_code': 'TEST_CLIENT',
            'file': test_file
        }
        
        response = self.client.post('/api/v1/integration/files/', data, format='multipart')
        
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(ClientFile.objects.count(), 1)
        
        client_file = ClientFile.objects.first()
        self.assertEqual(client_file.client_code, 'TEST_CLIENT')
        self.assertEqual(client_file.status, 'pending')
    
    def test_upload_file_invalid_client(self):
        """Prueba subida con código de cliente inválido"""
        test_file = SimpleUploadedFile(
            "test.csv",
            b"test,data",
            content_type="text/csv"
        )
        
        data = {
            'client_code': 'INVALID_CLIENT',
            'file': test_file
        }
        
        response = self.client.post('/api/v1/integration/files/', data, format='multipart')
        
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('No hay configuración activa', str(response.data))
    
    def test_get_files_list(self):
        """Prueba obtener lista de archivos"""
        # Crear archivo de prueba
        test_file = SimpleUploadedFile(
            "test.csv",
            b"test,data",
            content_type="text/csv"
        )
        
        ClientFile.objects.create(
            client_code='TEST_CLIENT',
            file=test_file,
            original_filename='test.csv',
            file_size=len(b"test,data")
        )
        
        response = self.client.get('/api/v1/integration/files/')
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)


class ReportGeneratorTest(TestCase):
    """Tests para el generador de reportes"""
    
    def test_generate_processing_summary(self):
        """Prueba generación de reporte de resumen"""
        # Crear datos de prueba
        client_mapping = ClientMapping.objects.create(
            client_code='TEST_CLIENT',
            mapping_config={'seudo_bd': 'db', 'id_clie': 'id'}
        )
        
        test_file = SimpleUploadedFile(
            "test.csv",
            b"test,data",
            content_type="text/csv"
        )
        
        ClientFile.objects.create(
            client_code='TEST_CLIENT',
            file=test_file,
            original_filename='test.csv',
            file_size=len(b"test,data"),
            status='processed'
        )
        
        report = ReportGenerator.generate_processing_summary()
        
        self.assertIsNotNone(report.id)
        self.assertEqual(report.report_type, 'processing_summary')
        self.assertIn('summary', report.data)
        self.assertEqual(report.data['summary']['total_files'], 1)
        self.assertEqual(report.data['summary']['processed_files'], 1)
