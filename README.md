# Metalload - Servicio de Integración

Servicio de integración desacoplado para procesamiento de archivos de clientes y comunicación con la aplicación principal de mensajería financiera.

## 🏗️ Arquitectura

Este servicio implementa una arquitectura de microservicios que separa la lógica de integración de la aplicación principal:

- **Aplicación Principal**: Maneja la mensajería financiera crítica y estable
- **Servicio de Integración**: Procesa archivos, transforma datos y gestiona reglas por cliente

## 🚀 Características

- ✅ Recepción de archivos en múltiples formatos (CSV, Excel)
- ✅ Transformación y mapeo configurable por cliente
- ✅ Validaciones personalizadas
- ✅ Comunicación con API principal
- ✅ Sistema de reportes
- ✅ Procesamiento asíncrono con Celery
- ✅ Logging completo y trazabilidad
- ✅ Panel administrativo de Django

## 📋 Requisitos

- Python 3.8+
- Django 5.2.11
- Redis (para Celery)
- PostgreSQL o SQLite

## 🛠️ Instalación

1. **Clonar el repositorio**
   ```bash
   git clone <repository-url>
   cd Metalload
   ```

2. **Crear entorno virtual**
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # o
   venv\Scripts\activate  # Windows
   ```

3. **Instalar dependencias**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configurar variables de entorno**
   ```bash
   # settings.py
   MAIN_API_BASE_URL = 'http://url-app-principal:8000'
   MAIN_API_KEY = 'tu-api-key'
   ```

5. **Migraciones**
   ```bash
   python manage.py makemigrations
   python manage.py migrate
   ```

6. **Crear superusuario**
   ```bash
   python manage.py createsuperuser
   ```

7. **Iniciar servicios**
   
   **Servidor Django:**
   ```bash
   python manage.py runserver
   ```
   
   **Celery Worker:**
   ```bash
   celery -A Metalload worker -l info
   ```
   
   **Celery Beat (tareas programadas):**
   ```bash
   celery -A Metalload beat -l info
   ```

## 📚 Uso de la API

### 1. Configurar Mapeo de Cliente

```http
POST /api/v1/integration/mappings/
Content-Type: application/json

{
    "client_code": "CLIENTE_001",
    "mapping_config": {
        "seudo_bd": "database",
        "id_clie": "client_id",
        "nombre": "first_name",
        "surname": "last_name",
        "cc": "id_number",
        "documento": "document"
    },
    "validation_rules": {
        "required_fields": ["seudo_bd", "id_clie", "nombre"]
    },
    "is_active": true
}
```

### 2. Subir Archivo de Cliente

```http
POST /api/v1/integration/files/
Content-Type: multipart/form-data

client_code: CLIENTE_001
file: [archivo.csv]
```

### 3. Procesar Archivo

```http
POST /api/v1/integration/files/{file_id}/process/
```

### 4. Ver Logs de Procesamiento

```http
GET /api/v1/integration/files/{file_id}/logs/
```

### 5. Generar Reportes

```http
POST /api/v1/integration/reports/generate_summary/
Content-Type: application/json

{
    "date_from": "2024-01-01T00:00:00Z",
    "date_to": "2024-01-31T23:59:59Z"
}
```

## 🔧 Configuración de Mapeos

### Mapeo Simple

```json
{
    "seudo_bd": "database",
    "id_clie": "client_id",
    "nombre": "name"
}
```

### Mapeo con Transformaciones

```json
{
    "seudo_bd": "database",
    "id_clie": "client_id",
    "nombre": {
        "source": "first_name",
        "transform": "upper"
    },
    "surname": {
        "source": "last_name", 
        "transform": "strip"
    }
}
```

### Transformaciones Disponibles

- `direct`: Sin transformación
- `upper`: Convertir a mayúsculas
- `lower`: Convertir a minúsculas
- `strip`: Eliminar espacios en blanco

## 📊 Endpoints Principales

| Endpoint | Método | Descripción |
|----------|--------|-------------|
| `/api/v1/integration/files/` | GET/POST | Gestionar archivos |
| `/api/v1/integration/files/{id}/process/` | POST | Procesar archivo |
| `/api/v1/integration/mappings/` | GET/POST | Configurar mapeos |
| `/api/v1/integration/reports/` | GET/POST | Gestionar reportes |
| `/api/v1/integration/logs/` | GET | Ver logs |
| `/api/v1/integration/files/stats/` | GET | Estadísticas |

## 🗂️ Estructura del Proyecto

```
Metalload/
├── Metalload/
│   ├── __init__.py
│   ├── settings.py
│   ├── urls.py
│   ├── celery.py
│   └── wsgi.py
├── integration_service/
│   ├── models.py          # Modelos de datos
│   ├── views.py           # Vistas de API
│   ├── serializers.py     # Serializers DRF
│   ├── services.py        # Lógica de negocio
│   ├── tasks.py           # Tareas Celery
│   ├── urls.py            # URLs del servicio
│   ├── admin.py           # Admin de Django
│   └── tests.py           # Tests unitarios
├── media/                 # Archivos subidos
├── logs/                  # Logs de aplicación
├── requirements.txt       # Dependencias
└── README.md             # Documentación
```

## 🔐 Seguridad

- Autenticación mediante Token de Django REST Framework
- Validación de archivos subidos
- Límites de tamaño de archivo
- Logs de auditoría
- Comunicación segura con API principal

## 📈 Monitoreo

### Logs

Los logs se guardan en `logs/integration_service.log` y incluyen:

- Niveles: INFO, WARNING, ERROR, DEBUG
- Trazas de procesamiento
- Errores de comunicación
- Auditoría de operaciones

### Métricas

- Estadísticas de procesamiento
- Tiempos de respuesta
- Tasa de errores
- Volumen de archivos

## 🧪 Testing

Ejecutar tests:

```bash
python manage.py test integration_service
```

## 🚀 Despliegue

### Docker

```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
EXPOSE 8000

CMD ["gunicorn", "--bind", "0.0.0.0:8000", "Metalload.wsgi:application"]
```

### Variables de Entorno

```bash
DJANGO_SETTINGS_MODULE=Metalload.settings
MAIN_API_BASE_URL=https://api-principal.com
MAIN_API_KEY=secret-key
CELERY_BROKER_URL=redis://redis:6379/0
```

## 🤝 Contribución

1. Fork del proyecto
2. Crear feature branch
3. Commit de cambios
4. Push al branch
5. Pull Request

## 📝 Licencia

[MIT License](LICENSE)

## 🆘 Soporte

Para problemas o preguntas:

- Revisar logs en `logs/integration_service.log`
- Verificar conexión con API principal
- Validar configuración de mapeos
- Revisar estado de tareas Celery
echo # Metalload
