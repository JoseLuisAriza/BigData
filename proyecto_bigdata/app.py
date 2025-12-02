import os
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    session,
    flash,
)

from Helpers.mongoDB import (
    get_usuarios_collection,
    crear_usuario,
    validar_login,
    asegurar_admin_por_defecto,
)

from Helpers.elastic import (
    contar_documentos,
    buscar_libros,
    crear_indice_si_no_existe,
    cargar_libro,
    get_index_name,
)

from Helpers.funciones import admin_requerido
from Helpers.PLN import resumir_texto


def create_app():
    app = Flask(__name__)

    # Clave de sesión
    app.secret_key = os.environ.get("SECRET_KEY", "cambia-esta-clave")

    # Asegurar admin por defecto y existencia del índice al arrancar
    asegurar_admin_por_defecto()
    crear_indice_si_no_existe()

    # Variables globales para las plantillas
    @app.context_processor
    def inject_globals():
        return {
            "app_nombre": "Mini Biblioteca BigData",
        }

    # ---------------- RUTAS PÚBLICAS ---------------- #

    @app.route("/")
    def index():
        """
        Landing page (página principal).
        """
        return render_template("landing.html")

    @app.route("/buscar", methods=["GET"])
    def buscar():
        """
        Buscador público.
        """
        texto = request.args.get("texto", "").strip() or None
        autor = request.args.get("autor", "").strip() or None
        anio_desde = request.args.get("anio_desde") or None
        anio_hasta = request.args.get("anio_hasta") or None

        resultados = []
        total = 0

        # Solo buscar si hay al menos un filtro
        if any([texto, autor, anio_desde, anio_hasta]):
            try:
                resultados, total = buscar_libros(
                    texto=texto,
                    autor=autor,
                    anio_desde=anio_desde,
                    anio_hasta=anio_hasta,
                )
                # Resumen corto del texto
                for r in resultados:
                    r["resumen"] = resumir_texto(r.get("texto"))
            except Exception as e:
                flash(f"Error al ejecutar la búsqueda: {e}", "danger")

        return render_template(
            "buscador.html",
            resultados=resultados,
            total_resultados=total,
            texto=texto or "",
            autor=autor or "",
            anio_desde=anio_desde or "",
            anio_hasta=anio_hasta or "",
        )

    @app.route("/login", methods=["GET", "POST"])
    def login():
        """
        Login de administrador.
        """
        if request.method == "POST":
            username = request.form.get("username")
            password = request.form.get("password")

            usuario = validar_login(username, password)
            if usuario:
                session["usuario"] = usuario["username"]
                session["es_admin"] = bool(usuario.get("es_admin"))
                flash("Login correcto.", "success")
                return redirect(url_for("admin"))

            flash("Usuario o contraseña incorrectos.", "danger")

        return render_template("login.html")

    @app.route("/logout")
    def logout():
        """
        Cerrar sesión.
        """
        session.clear()
        flash("Sesión cerrada.", "info")
        return redirect(url_for("index"))

    # ---------------- RUTAS ADMIN ---------------- #

    @app.route("/admin")
    @admin_requerido
    def admin():
        """
        Panel principal de administración.
        """
        return render_template("admin.html")

    @app.route("/admin/usuarios", methods=["GET", "POST"])
    @admin_requerido
    def admin_usuarios():
        """
        Gestión de usuarios (crear y listar).
        """
        col = get_usuarios_collection()

        if request.method == "POST":
            username = request.form.get("username")
            password = request.form.get("password")
            es_admin = bool(request.form.get("es_admin"))

            creado = crear_usuario(username, password, es_admin)
            if creado:
                flash("Usuario creado correctamente.", "success")
            else:
                flash("El usuario ya existe.", "warning")

            return redirect(url_for("admin_usuarios"))

        usuarios = list(col.find({}, {"_id": 0, "password": 0}))
        return render_template("admin_usuarios.html", usuarios=usuarios)

    @app.route("/admin/elastic")
    @admin_requerido
    def admin_elastic():
        """
        Panel básico de estado de Elastic.
        """
        total = 0
        error = None
        try:
            total = contar_documentos()
        except Exception as e:
            error = str(e)
            flash(f"Error al conectarse a ElasticSearch: {e}", "danger")

        return render_template(
            "admin_elastic.html",
            indice=get_index_name(),
            total=total,
            error=error,
        )

    @app.route("/admin/cargar", methods=["GET", "POST"])
    @admin_requerido
    def cargar_archivos():
        """
        Formulario para cargar un libro en Elastic.
        """
        if request.method == "POST":
            titulo = request.form.get("titulo")
            autor = request.form.get("autor")
            anio = request.form.get("anio")
            texto = request.form.get("texto")

            try:
                cargar_libro(titulo, autor, anio, texto)
                flash("Libro cargado correctamente en Elastic.", "success")
            except Exception as e:
                flash(f"Error al cargar el libro: {e}", "danger")

            return redirect(url_for("cargar_archivos"))

        return render_template("cargar_archivos.html")

    return app


app = create_app()

if __name__ == "__main__":
    # Para ejecución local
    app.run(debug=True)
