import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
MONGO_URL = os.getenv("MONGO_URL")

def descargar_archivo(url, ruta_destino):
    print(f"Descargando {url}...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        respuesta = requests.get(url, headers=headers)
        if respuesta.status_code == 200:
            os.makedirs(os.path.dirname(ruta_destino), exist_ok=True)
            with open(ruta_destino, 'wb') as f:
                f.write(respuesta.content)
            print(f"Descarga completada: {ruta_destino}")
            return True
        else:
            print(f"Error al descargar. Codigo: {respuesta.status_code}")
            return False
    except Exception as e:
        print(f"Excepcion al descargar {url}: {e}")
        return False

def insertar_datos_mongodb(df, nombre_coleccion, mapeo_columnas):
    client = MongoClient(MONGO_URL)
    db = client['geocrimen_tacna']  
    coleccion = db[nombre_coleccion]
    
    columnas_mantener = [c for c in mapeo_columnas.keys() if c in df.columns]
    df = df[columnas_mantener].copy()
    
    df = df.rename(columns=mapeo_columnas)
    
    import numpy as np
    df = df.replace({np.nan: None})
    
    if 'anio' in df.columns and 'mes' in df.columns:
        periodos = df[['anio', 'mes']].drop_duplicates().dropna().to_dict('records')
        for p in periodos:
            mes_insertar = int(p['mes'])
            anio_insertar = int(p['anio'])
            
            mes_anterior = mes_insertar - 1
            anio_anterior = anio_insertar
            if mes_anterior == 0:
                mes_anterior = 12
                anio_anterior -= 1
                
            res_ant = coleccion.delete_many({"anio": anio_anterior, "mes": mes_anterior})
            if res_ant.deleted_count > 0:
                print(f"Limpieza de mes anterior: Se borraron {res_ant.deleted_count} registros viejos de {mes_anterior}/{anio_anterior} en {nombre_coleccion}.")
            
            res_act = coleccion.delete_many({"anio": anio_insertar, "mes": mes_insertar})
            if res_act.deleted_count > 0:
                print(f"Limpieza de reemplazo: Se limpiaron {res_act.deleted_count} registros repetidos de {mes_insertar}/{anio_insertar} en {nombre_coleccion}.")
    
    documentos = df.to_dict('records')
    ahora = datetime.now(timezone.utc)
    
    for doc in documentos:
        doc['importado_en'] = ahora
        for k, v in list(doc.items()):
            if pd.isna(v):
                doc[k] = None
    
    if documentos:
        try:
            coleccion.insert_many(documentos, ordered=False)
            print(f"Exito: Se enviaron {len(documentos)} registros (se omitieron los ya existentes) en {nombre_coleccion}.")
        except Exception as e:
            if 'bulk write error' in str(e).lower() or 'duplicate key error' in str(e).lower():
                print(f"Exito parcial: Algunos de los {len(documentos)} registros insertados en {nombre_coleccion} ya existian.")
            else:
                print(f"Excepcion al insertar en {nombre_coleccion}: {e}")
    else:
        print(f"Advertencia: No hay documentos para insertar en {nombre_coleccion}.")

def obtener_url_flagrancia():
    """Genera el enlace de descarga para el mes anterior en Unidad de Flagrancia."""
    ahora = datetime.now()
    if ahora.month == 1:
        mes_buscar = 12
        anio_buscar = ahora.year - 1
    else:
        mes_buscar = ahora.month - 1
        anio_buscar = ahora.year

    return f"https://csjtacna.exgperu.com/flagrancia/assets/script/descargarIngresos.php?mes={mes_buscar}&anio={anio_buscar}&tipo=xls"

def obtener_url_sidpol():
    """Busca el enlace de descarga mas reciente en la pagina de Mininter (SIDPOL)."""
    url_gob = "https://www.gob.pe/institucion/mininter/informes-publicaciones"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        res = requests.get(url_gob, headers=headers)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        enlaces = soup.find_all('a', href=True)
        for enlace in enlaces:
            href = enlace['href']
            if 'cdn.www.gob.pe' in href and 'sidpol' in href.lower() and '.xlsx' in href:
                return href
            
        for enlace in enlaces:
            if 'base-de-datos' in enlace.get('href', '') and 'sidpol' in enlace.get('href', ''):
                sub_url = f"https://www.gob.pe{enlace['href']}" if enlace['href'].startswith('/') else enlace['href']
                res_sub = requests.get(sub_url, headers=headers)
                soup_sub = BeautifulSoup(res_sub.text, 'html.parser')
                btn_descargar = soup_sub.find('a', string=lambda t: t and 'Descargar' in t)
                if btn_descargar:
                    return btn_descargar['href']
                    
    except Exception as e:
        print(f"Error scrapeando SIDPOL: {e}")
        
    return "https://cdn.www.gob.pe/uploads/document/file/9667704/7912719-base-de-datos-del-sistema-de-denuncias-policiales-sidpol-a-febrero-del-2026.xlsx"


def automatizacion_completa():
    carpeta_datos = "datos_descargados"
    ruta_sidpol = f"{carpeta_datos}/sidpol_actual.xlsx"
    ruta_flagrancia = f"{carpeta_datos}/flagrancia_actual.xls"
    
    url_sidpol = obtener_url_sidpol()
    url_flagrancia = obtener_url_flagrancia()
    
    print(f"URL de SIDPOL detectada: {url_sidpol}")
    print(f"URL de Flagrancia generada: {url_flagrancia}")
    
    descarga_sidpol_ok = descargar_archivo(url_sidpol, ruta_sidpol)
    descarga_flagrancia_ok = descargar_archivo(url_flagrancia, ruta_flagrancia)

    if descarga_sidpol_ok:
        print("\n--- PROCESANDO DATOS DE SIDPOL ---")
        try:
            xls = pd.ExcelFile(ruta_sidpol)
            hoja_objetivo = xls.sheet_names[-1]
            for sheet in reversed(xls.sheet_names):
                df_temp = pd.read_excel(xls, sheet_name=sheet, nrows=0)
                if 'PROV_HECHO' in df_temp.columns and 'TIPO' in df_temp.columns:
                    hoja_objetivo = sheet
                    break
            
            df_sidpol = pd.read_excel(xls, sheet_name=hoja_objetivo)
            
            df_sidpol.columns = df_sidpol.columns.str.strip()
            
            df_sidpol['UBIGEO_HECHO'] = df_sidpol['UBIGEO_HECHO'].astype(str).str.strip()
            
            # Solo extraer el mes anterior para no traer todo el acumulado anual de SIDPOL
            ahora = datetime.now()
            if ahora.month == 1:
                mes_buscar = 12
                anio_buscar = ahora.year - 1
            else:
                mes_buscar = ahora.month - 1
                anio_buscar = ahora.year
                
            df_sidpol = df_sidpol[pd.to_numeric(df_sidpol['ANIO'], errors='coerce') == anio_buscar]
            df_sidpol = df_sidpol[pd.to_numeric(df_sidpol['MES'], errors='coerce') == mes_buscar]
            
            df_sidpol = df_sidpol[df_sidpol['DPTO_HECHO_NEW'].astype(str).str.upper().str.strip() == 'TACNA']
            df_sidpol = df_sidpol[df_sidpol['PROV_HECHO'].astype(str).str.upper().str.strip() == 'TACNA']
            
            df_sidpol = df_sidpol.dropna(subset=['DIST_HECHO'])
            df_sidpol = df_sidpol[df_sidpol['DIST_HECHO'].astype(str).str.strip() != '']
            df_sidpol = df_sidpol[df_sidpol['DIST_HECHO'].astype(str).str.upper().str.strip() != 'TACNA']
            
            palabras_clave = ['ROBO', 'HURTO', 'USURPACIÓN', 'USURPACION']
            patron = '|'.join(palabras_clave)
            df_sidpol = df_sidpol[
                df_sidpol['SUB_TIPO'].astype(str).str.upper().str.contains(patron, na=False) |
                df_sidpol['MODALIDAD'].astype(str).str.upper().str.contains(patron, na=False)
            ]
            
            columnas_agrupar = ['ANIO', 'MES', 'UBIGEO_HECHO', 'DPTO_HECHO_NEW', 'PROV_HECHO', 'DIST_HECHO', 'TIPO', 'SUB_TIPO', 'MODALIDAD']
            df_sidpol = df_sidpol.groupby(columnas_agrupar, as_index=False)['n_dist_ID_DGC'].sum()
            
            mapeo_sidpol = {
                'ANIO': 'anio', 
                'MES': 'mes',
                'UBIGEO_HECHO': 'ubigeo',
                'DPTO_HECHO_NEW': 'departamento',
                'PROV_HECHO': 'provincia',
                'DIST_HECHO': 'distrito',
                'TIPO': 'tipo',
                'SUB_TIPO': 'sub_tipo',
                'MODALIDAD': 'modalidad',
                'n_dist_ID_DGC': 'cantidad'
            }
            
            if not df_sidpol.empty:
                insertar_datos_mongodb(df_sidpol, 'estadisticas_sidpol', mapeo_sidpol)
            else:
                print("No se encontraron registros en SIDPOL tras aplicar filtros.")
                
        except Exception as e:
            print(f"Error procesando SIDPOL: {e}")

    if descarga_flagrancia_ok:
        print("\n--- PROCESANDO DATOS DE UNIDAD DE FLAGRANCIA ---")
        try:
            lista_dfs_flagrancia = pd.read_html(ruta_flagrancia)
            if lista_dfs_flagrancia:
                df_flagrancia = lista_dfs_flagrancia[0]
                df_flagrancia.columns = df_flagrancia.columns.str.strip()
                
                df_flagrancia = df_flagrancia[df_flagrancia['DEPARTAMENTO'].astype(str).str.upper().str.strip() == 'TACNA']
                df_flagrancia = df_flagrancia[df_flagrancia['PROVINCIA'].astype(str).str.upper().str.strip() == 'TACNA']
                
                df_flagrancia = df_flagrancia.dropna(subset=['DISTRITO'])
                df_flagrancia = df_flagrancia[df_flagrancia['DISTRITO'].astype(str).str.strip() != '']
                
                palabras_clave = ['ROBO', 'HURTO', 'USURPACIÓN', 'USURPACION']
                patron = '|'.join(palabras_clave)
                df_flagrancia = df_flagrancia[df_flagrancia['DELITO'].astype(str).str.upper().str.contains(patron, na=False)]
                
                if not df_flagrancia.empty:
                    df_flagrancia['FECHA_PARSED'] = pd.to_datetime(df_flagrancia['FECHA DE INGRESO'], format='%d/%m/%Y', errors='coerce')
                    df_flagrancia['anio'] = df_flagrancia['FECHA_PARSED'].dt.year.fillna(datetime.now().year).astype(int)
                    df_flagrancia['mes'] = df_flagrancia['FECHA_PARSED'].dt.month.fillna(datetime.now().month).astype(int)
                    df_flagrancia['dia'] = df_flagrancia['FECHA_PARSED'].dt.day.astype('Int64')
                    df_flagrancia['cantidad'] = 1 

                    mapeo_flagrancia = {
                        'EXPEDIENTE': 'expediente',
                        'DELITO': 'delito',
                        'DISTRITO': 'distrito',
                        'PROVINCIA': 'provincia',
                        'DEPARTAMENTO': 'departamento',
                        'JUZGADO': 'juzgado',
                        'UNIDAD DE INTERVENCIÓN': 'dependencia_policial',
                        'anio': 'anio',
                        'mes': 'mes',
                        'dia': 'dia',
                        'cantidad': 'cantidad'
                    }
                    insertar_datos_mongodb(df_flagrancia, 'estadisticas_flagrancia', mapeo_flagrancia)
                else:
                    print("No se encontraron registros en FLAGRANCIA tras aplicar filtros.")
            else:
                print("No se encontraron tablas HTML en el archivo")
        except Exception as e:
            print(f"Error procesando Flagrancia: {e}")

if __name__ == "__main__":
    if not MONGO_URL:
        print("Error: Faltan credenciales MONGO_URL en el entorno")
    else:
        print("Iniciando recoleccion mensual de datos...")
        automatizacion_completa()
