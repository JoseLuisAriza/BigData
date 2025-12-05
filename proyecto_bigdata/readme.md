# Proyecto de BIGDATA para la maestría en Analítica

## Aplicación

**Mini Biblioteca BigData**  
Buscador de una biblioteca digital usando **Elasticsearch** para búsqueda de texto completo y **MongoDB Atlas** para la administración de usuarios y credenciales de acceso al panel de administrador.

La aplicación está desplegada en Render en:

> [https://bigdata-ghvp.onrender.com](https://bigdata-ghvp.onrender.com/)

---

## Autor

- **Nombre:** Jose Luis Ariza  
- **Email:** jarizaa@ucentral.edu.co

---

## Descripción

Este proyecto implementa una mini biblioteca digital como entrega final de la materia de Big Data.  

La solución:

1. Indexa libros en un índice de **Elasticsearch** (título, autor, año y ruta del PDF).
2. Permite realizar búsquedas por:
   - Título.
   - Autor.
3. Muestra resultados paginados con título, autor, año y enlace al archivo PDF.
4. Incluye un **panel de administración protegido** con login, donde el usuario administrador puede:
   - Administrar usuarios (crear, listar, eliminar).
   - Ver y administrar el estado de Elasticsearch.
   - Cargar nuevos archivos PDF, ejecutando el pipeline de PLN para extraer texto y actualizar el índice.

La aplicación está construida con **Flask**, **Elasticsearch (Elastic Cloud)** y **MongoDB Atlas**, y se despliega como *Web Service* en **Render**.

---

## Arquitectura general

- **Frontend**
  - Plantillas HTML con **Jinja2**.
  - Estilos con **Bootstrap 5** + CSS propio.
  - Landing page con imagen de fondo tipo “biblioteca robotizada”.

- **Backend (Flask)**
  - Rutas públicas:
    - `/`               – Landing page.
    - `/buscar`         – Formulario de búsqueda y resultados.
  - Rutas protegidas (requieren login):
    - `/login`          – Formulario de login de administrador.
    - `/admin`          – Panel principal de administración.
    - `/admin/usuarios` – Gestión de usuarios (MongoDB).
    - `/admin/elastic`  – Gestión y estado de Elasticsearch.
    - `/admin/cargar`   – Carga de PDFs y reindexación.

- **Servicios externos**
  - **Elasticsearch Cloud**  
    Índice principal: `libros_bigdata`.
  - **MongoDB Atlas**  
    Colección de usuarios para login y administración.

---

## Estructura de carpetas

```text
proyecto_bigdata/
├─ app.py                         # Aplicación Flask principal
├─ requirements.txt               # Dependencias de Python
├─ helpers/
│   ├─ __init__.py
│   ├─ elastic_helper.py          # Búsquedas y operaciones en Elasticsearch
│   ├─ mongo_helper.py            # Conexión y operaciones con MongoDB
│   ├─ pln.py                     # Lógica de PLN para extraer texto de PDFs
│   └─ funciones.py               # Funciones auxiliares (utilidades)
├─ scritps/
│   ├─ generar_json_libros.py     # Plantilla base con navbar
├─ templates/
│   ├─ base.html                  # Plantilla base con navbar
│   ├─ index.html                 # Landing page (con imagen de fondo)
│   ├─ login.html                 # Login administrador
│   ├─ admin.html                 # Panel admin principal
│   ├─ admin_usuarios.html        # Admin. de usuarios
│   ├─ admin_elastic.html         # Admin. de Elastic
│   ├─ cargar_archivos.html       # Carga de PDFs
│   ├─ buscador.html              # Formulario de búsqueda
│   └─ resultados.html            # Resultados de búsqueda
└─ static/
    ├─ css/
    │   └─ style.css              # Estilos personalizados
    ├─ img/
    │   └─ biblioteca_robot.jpg   # Imagen de fondo landing
    └─ uploads/                   # Carpeta donde se guardan PDFs subidos
