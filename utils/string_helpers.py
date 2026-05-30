import difflib

# Coordenadas maestras de los distritos de Tacna para la fase Macro (Alineadas con centros poblados reales)
COORDENADAS_DISTRITOS = {
    "TACNA": {"lat": -18.0146, "lng": -70.2536},
    "ALTO DE LA ALIANZA": {"lat": -17.9922, "lng": -70.2436},
    "CIUDAD NUEVA": {"lat": -17.9790, "lng": -70.2380},
    "CORONEL GREGORIO ALBARRACIN LANCHIPA": {"lat": -18.0463, "lng": -70.2520},
    "POCOLLAY": {"lat": -17.9961, "lng": -70.2185},
    "CALANA": {"lat": -17.9422, "lng": -70.1834},
    "PACHIA": {"lat": -17.8925, "lng": -70.1558},
    "SAMA": {"lat": -17.8441, "lng": -70.6273},
    "LA YARADA LOS PALOS": {"lat": -18.1755, "lng": -70.4735},
}

def limpiar_distrito(nombre):
    """Normaliza nombres de distritos usando coincidencias difusas (fuzzy matching)"""
    if not nombre:
        return "TACNA"
        
    nombre = str(nombre).upper().strip()
    
    trans = str.maketrans("ÁÉÍÓÚÄËÏÖÜ", "AEIOUAEIOU")
    nombre = nombre.translate(trans)
    
    distritos_validos = list(COORDENADAS_DISTRITOS.keys())
    
    if nombre in distritos_validos:
        return nombre
        
    # Atajos manuales
    if "GREGORIO" in nombre or "ALBARRACI" in nombre:
        return "CORONEL GREGORIO ALBARRACIN LANCHIPA"
    if "YARADA" in nombre or "PALOS" in nombre:
        return "LA YARADA LOS PALOS"
    if "ALIANZA" in nombre:
        return "ALTO DE LA ALIANZA"
        
    # Auto-corrector (Fuzzy Matching)
    coincidencias = difflib.get_close_matches(nombre, distritos_validos, n=1, cutoff=0.65)
    if coincidencias:
        return coincidencias[0]
        
    return "TACNA"