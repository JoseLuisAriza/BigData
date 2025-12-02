import os
from elasticsearch import Elasticsearch, exceptions

# -----------------------------------------
#   CLIENTE GLOBAL
# -----------------------------------------
def get_client():
    url = os.getenv("ES_CLOUD_URL")
    api_key = os.getenv("ES_API_KEY")

    if not url or not api_key:
        raise ValueError("Variables ES_CLOUD_URL o ES_API_KEY no definidas.")

    client = Elasticsearch(
        url,
        api_key=api_key,
        verify_certs=False
    )
    return client


# -----------------------------------------
#   VERIFICAR CONEXIÓN
# -----------------------------------------
def ping_elastic(client):
    try:
        return client.ping()
    except:
        return False


# -----------------------------------------
#   BUSCAR LIBROS
# -----------------------------------------
def buscar_libros(client, texto="", autor="", anio_desde="", anio_hasta="", indice="libros"):

    must = []

    if texto:
        must.append({"match": {"descripcion": texto}})
    if autor:
        must.append({"match": {"autor": autor}})
    if anio_desde or anio_hasta:
        rango = {}
        if anio_desde:
            rango["gte"] = anio_desde
        if anio_hasta:
            rango["lte"] = anio_hasta
        must.append({"range": {"anio": rango}})

    if not must:
        query = {"match_all": {}}
    else:
        query = {"bool": {"must": must}}

    try:
        resp = client.search(
            index=indice,
            query=query,
            size=100
        )
        hits = resp.get("hits", {}).get("hits", [])
        resultados = []
        for h in hits:
            src = h["_source"]
            resultados.append({
                "titulo": src.get("titulo"),
                "autor": src.get("autor"),
                "anio": src.get("anio"),
                "descripcion": src.get("descripcion")
            })
        return resultados

    except exceptions.NotFoundError:
        return []
    except Exception as e:
        print("ERROR en buscar_libros:", e)
        return []
