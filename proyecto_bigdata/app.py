import os
from flask import Flask, render_template, request
from dotenv import load_dotenv
from Helpers.elastic_helper import buscar_libros

# Cargar variables de entorno desde .env (solo en local)
load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "cambia_esta_clave")

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/buscar", methods=["GET", "POST"])
def buscar():
    if request.method == "POST":
        texto = request.form.get("texto") or None
        autor = request.form.get("autor") or None
        anio_desde = request.form.get("anio_desde") or None
        anio_hasta = request.form.get("anio_hasta") or None
    else:
        texto = request.args.get("texto") or None
        autor = request.args.get("autor") or None
        anio_desde = request.args.get("anio_desde") or None
        anio_hasta = request.args.get("anio_hasta") or None

    def to_int(x):
        try:
            return int(x) if x else None
        except ValueError:
            return None

    anio_desde = to_int(anio_desde)
    anio_hasta = to_int(anio_hasta)

    total, docs = buscar_libros(
        texto=texto,
        autor=autor,
        anio_desde=anio_desde,
        anio_hasta=anio_hasta,
        size=30
    )

    return render_template(
        "resultados.html",
        total=total,
        documentos=docs,
        texto=texto or "",
        autor=autor or "",
        anio_desde=anio_desde or "",
        anio_hasta=anio_hasta or ""
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
