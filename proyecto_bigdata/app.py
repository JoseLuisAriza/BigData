import os
from functools import wraps

from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    session,
)

from Helpers.elastic_helper import (
    buscar_libros,
    contar_documentos,
    agregar_libro,
    INDEX_NAME,
)
from Helpers.mongo_helper import (
    verify_user,
    create_user,
    ensure_admin_user,
    get_users_collection,
)

app = Flask(__name__)

# Clave de sesión para Flask (ya la tienes como SECRET_KEY en Render)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")

# Asegura que exista al menos un admin en Mongo
ensure_admin_user()


# --------- Decoradores de seguridad --------- #

def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if "user" not in session:
            return redirect(url_for("login", next=request.path))
        return view(*args, **kwargs)

    return wrapped


def admin_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if "user" not in session or not session.get("is_admin"):
            flash("Acceso solo para administradores.", "danger")
            return redirect(url_for("login"))
        return view(*args, **kwargs)

    return wrapped


# --------- Páginas públicas --------- #

@app.route("/")
def landing():
    # Landing con info del proyecto
    return render_template("landing.html")

@app.route("/buscar", methods=["GET"])
def buscar():
    from elastic_helper import buscar_libros  # por si no está ya importado arriba

    # Leer filtros desde la query-string (?texto=...&autor=...)
    texto = (request.args.get("texto") or "").strip()
    autor = (request.args.get("autor") or "").strip()
    anio_desde_raw = (request.args.get("anio_desde") or "").strip()
    anio_hasta_raw = (request.args.get("anio_hasta") or "").strip()

    # Convertir años a entero si se puede
    def to_int_or_none(v):
        try:
            return int(v) if v else None
        except ValueError:
            return None

    anio_desde = to_int_or_none(anio_desde_raw)
    anio_hasta = to_int_or_none(anio_hasta_raw)

    # Si no hay filtros, mostramos todo (match_all) hasta 50 libros
    if not (texto or autor or anio_desde or anio_hasta):
        total, libros = buscar_libros(size=50)
    else:
        total, libros = buscar_libros(
            texto=texto or None,
            autor=autor or None,
            anio_desde=anio_desde,
            anio_hasta=anio_hasta,
            size=50,
        )

    return render_template(
        "resultados.html",
        libros=libros,
        total=total,
        texto=texto,
        autor=autor,
        anio_desde=anio_desde_raw,
        anio_hasta=anio_hasta_raw,
    )


# --------- Login / logout --------- #

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        user = verify_user(username, password)
        if user:
            session["user"] = user["username"]
            session["is_admin"] = bool(user.get("is_admin"))
            flash("Login correcto.", "success")
            next_url = request.args.get("next")
            return redirect(next_url or url_for("admin"))
        else:
            flash("Usuario o contraseña incorrectos.", "danger")

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    flash("Sesión cerrada.", "info")
    return redirect(url_for("landing"))


# --------- Panel admin --------- #

@app.route("/admin")
@admin_required
def admin():
    return render_template("admin.html")


@app.route("/admin/usuarios", methods=["GET", "POST"])
@admin_required
def admin_usuarios():
    users_coll = get_users_collection()

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        is_admin = bool(request.form.get("is_admin"))

        if not username or not password:
            flash("Usuario y contraseña son obligatorios.", "danger")
        else:
            ok, msg = create_user(username, password, is_admin=is_admin)
            flash(msg, "success" if ok else "danger")

    usuarios = list(users_coll.find({}, {"password": 0}).sort("username", 1))
    return render_template("admin_usuarios.html", usuarios=usuarios)


@app.route("/admin/elastic")
@admin_required
def admin_elastic():
    total_docs = contar_documentos()
    return render_template(
        "admin_elastic.html",
        total_docs=total_docs,
        index_name=INDEX_NAME,
    )


@app.route("/admin/cargar", methods=["GET", "POST"])
@admin_required
def admin_cargar():
    if request.method == "POST":
        titulo = request.form.get("titulo", "").strip()
        autor = (request.form.get("autor") or "").strip() or "Desconocido"
        anio = request.form.get("anio") or None
        texto = request.form.get("texto") or ""

        if not titulo:
            flash("El título es obligatorio.", "danger")
        else:
            doc = {
                "titulo": titulo,
                "autor": autor,
                "texto": texto,
            }
            if anio:
                try:
                    doc["anio"] = int(anio)
                except ValueError:
                    pass

            agregar_libro(doc)
            flash("Libro cargado en Elasticsearch.", "success")

    return render_template("admin_cargar.html")


# --------- Main local (no se usa en Render, pero sirve en local) --------- #

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
