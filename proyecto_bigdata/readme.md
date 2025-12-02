# Proyecto de BIGDATA para la maestría en Analítica

## Aplicación

**Nombre de la aplicación:** Mini Biblioteca BigData  
**URL en producción:** https://bigdata-ghvp.onrender.com  

Aplicación web desarrollada en Flask que implementa un buscador de una biblioteca digital de libros en PDF usando **Elasticsearch** para la búsqueda de texto completo y **MongoDB Atlas** para la gestión de usuarios y administración.

---

## Autor

**Nombre:** Jose Luis Ariza  
**Email:** jarizaa@ucentral.edu.co

---

## Descripción

La aplicación cumple los requisitos del proyecto de Big Data:

1. **Landing Page**  
   Página de presentación de la “Mini Biblioteca BigData”, con navegación hacia el buscador público y el login de administrador.

2. **Página de Login (MongoDB Atlas)**  
   Formulario de inicio de sesión que valida usuario y contraseña contra la colección `usuarios` almacenada en MongoDB Atlas.

3. **Panel de Administración (solo después de login)**  
   Menú de administración con acceso a tres módulos:
   - **Administrar usuarios:** crear, listar y eliminar usuarios de la aplicación.
   - **Administrar Elastic:** ver el estado del índice de libros y ejecutar consultas de prueba.
   - **Cargar archivos a Elastic (PLN):** subir PDFs y procesarlos para indexarlos en Elasticsearch.

4. **Página con buscador público**  
   Buscador de libros que permite filtrar por:
   - Texto libre (título o contenido)
   - Autor
   - Rango de años (desde / hasta)

   Los resultados muestran título, autor, año y un enlace al PDF original (cuando existe).

---

## Tecnologías principales

- Python 3 / Flask
- Elasticsearch (servicio en la nube)
- MongoDB Atlas
- Bootstrap 5
- Render.com (despliegue)
- HTML, CSS y Jinja2

---

## Estructura de carpetas (resumen)

```text
BigData/
└── proyecto_bigdata/
    ├── app.py
    ├── requirements.txt
    ├── Helpers/
    │   ├── __init__.py
    │   ├── elastic_helper.py
    │   └── mongo_helper.py
    ├── static/
    │   ├── css/
    │   │   └── style.css
    │   └── img/
    │       └── biblioteca_robot.jpg
    └── templates/
        ├── base.html
        ├── landing.html
        ├── login.html
        ├── admin.html
        ├── admin_usuarios.html
        ├── admin_elastic.html
        ├── admin_cargar.html
        └── resultados.html
