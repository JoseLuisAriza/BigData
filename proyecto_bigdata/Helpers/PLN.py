def resumir_texto(texto: str | None, max_chars: int = 350) -> str:
    """
    Devuelve un resumen sencillo cortando el texto en max_chars.
    No es PLN real, pero sirve para mostrar algo en el proyecto.
    """
    if not texto:
        return ""

    texto = texto.strip()
    if len(texto) <= max_chars:
        return texto

    corte = texto.rfind(" ", 0, max_chars)
    if corte == -1:
        corte = max_chars

    return texto[:corte] + "…"
