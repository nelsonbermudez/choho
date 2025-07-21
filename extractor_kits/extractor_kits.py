import re
import csv
import json
import pandas as pd
import os
from datetime import datetime
#from typing import List, Dict, Optional, Tuple


def procesar_archivos_raw(directorio_salida, archivo_entrada):
    """
    Procesa archivos Excel de datos crudos y genera un archivo CSV unificado.
    """
    excel_dir = 'dataraw'
    #excel_dir = os.path.join(excel_dir, excel_dir)
    excel_files = [f for f in os.listdir(excel_dir) if f.endswith('.xlsx') and not f.startswith('~$')]

    dfs = {}
    for file in excel_files:
        file_path = os.path.join(excel_dir, file)
        try:
            df_excel = pd.read_excel(file_path, sheet_name='DatosParte1')
            dfs[file] = df_excel
        except Exception as e:
            print(f"Error reading {file}: {e}")
    
    print("Procesando Dataframes")
    if not dfs:
        print("Archivos de Excel No validos")
        print("No se encontraron archivos de Excel válidos en el directorio.")
        exit(1)
    
    for file, df in dfs.items():
        print(f"\nCreando dataframe {file}:")
        print(f"Lineas procesadas: {len(df)}")  

    if dfs:
        unified_df = pd.concat(dfs.values(), ignore_index=True)
        print("\nCreando Dataframe Unificado:")
        print(f"Total de lineas procesadas: {len(unified_df)}")
    else:
        unified_df = pd.DataFrame()

    print("\nRemoviendo | de los datos originales")
    unified_df = unified_df.replace('|', '', regex=True)

    detalle_cols = [
        'Descripción de la Mercancía Detallada 1',
        'Descripción de la Mercancía Detallada 2',
        'Descripción de la Mercancía Detallada 3',
        'Descripción de la Mercancía Detallada 4',
        'Descripción de la Mercancía Detallada 5'
    ]

    detalle_cols = [col for col in detalle_cols if col in unified_df.columns]

    print("\nNormalizando Descripciones de Mercancía")
    unified_df['descripcion'] = unified_df[detalle_cols].fillna('').agg(' '.join, axis=1).str.strip()

    final_df = unified_df[['Número de Aceptación', 'descripcion', 'Cantidad', 'Unidad Comercial']].copy()

    final_df = final_df.rename(columns={
        'Número de Aceptación': 'numeroaceptacion',
        'Cantidad': 'cantidad',
        'Unidad Comercial': 'unidades',
        'Descripción de la Mercancía Detallada': 'descripcion'
    })

    print("\nIniciando creacion de Dataframe final")
    directory = directorio_salida
    if not os.path.exists(directory):
        os.makedirs(directory)
        
    output_file = os.path.join(directory, archivo_entrada)
    final_df.to_csv(output_file, index=False, sep="|")
    print(f"\nDataframe generado en archivo: {output_file}")


def cargar_diccionario_kits(archivo_diccionario):
    """
    Carga el diccionario de configuración para kits y cadenas desde un archivo JSON.
    """
    print(f"Entrando a cargar_diccionario_kits")

    try:
        with open(archivo_diccionario, 'r', encoding='utf-8') as archivo:
            diccionario = json.load(archivo)
            return diccionario
    except FileNotFoundError:
        print(f"Error: No se pudo encontrar el archivo de diccionario {archivo_diccionario}")
        return None
    except json.JSONDecodeError:
        print(f"Error: El archivo {archivo_diccionario} no tiene un formato JSON válido")
        return None
    except Exception as e:
        print(f"Error al cargar el diccionario: {e}")
        return None


def cargar_expresiones_regulares_kits(archivo_expresiones):
    """
    Carga las expresiones regulares específicas para kits desde un archivo JSON.
    """
    print(f"Entrando a cargar_expresiones_regulares_kits")

    try:
        with open(archivo_expresiones, 'r', encoding='utf-8') as archivo:
            expresiones = json.load(archivo)
            return expresiones.get("expresiones_regulares", {})
    except FileNotFoundError:
        print(f"Error: No se pudo encontrar el archivo de expresiones regulares {archivo_expresiones}")
        return {}
    except json.JSONDecodeError:
        print(f"Error: El archivo {archivo_expresiones} no tiene un formato JSON válido")
        return {}
    except Exception as e:
        print(f"Error al cargar las expresiones regulares: {e}")
        return {}

def aplicar_expresiones_regulares_ordenadas(texto, expresiones_regulares):
    """
    Aplica todas las expresiones regulares de normalización al texto en orden específico.
    """
    orden_aplicacion = [
        # "eliminar_decimales",
        "normalizar_cantidad_unidad_decimales",
        "normalizar_cantidad_unidad_enteros", 
        "separar_cantidad_producto",
        "normalizar_espacios_antes_producto",
        "normalizar_cantidad_punto_coma",
        "normalizar_cantidad_coma",
        "normalizar_punto_coma_mercancia",
        "limpiar_espacios_multiples",
        "patron_palabra_cantidad",
        "normalizar_cantidad_espacio"
    ]
    
    for nombre_expr in orden_aplicacion:
        if nombre_expr in expresiones_regulares:
            config = expresiones_regulares[nombre_expr]
            patron = config.get("patron", "")
            reemplazo = config.get("reemplazo", "")
            if patron:
                try:
                    texto = re.sub(patron, reemplazo, texto)
                except re.error:
                    continue
    
    return texto.strip()

def limpiar_y_normalizar_texto_kits(texto, expresiones_regulares):
    """
    Aplica transformaciones de limpieza y normalización específicas para kits y cadenas.
    """
    print(f"Entrando a limpiar_y_normalizar_texto_kits")

    # Convertir a mayúsculas y normalizar espacios
    texto = texto.upper()
    texto = re.sub(r'\s+', ' ', texto).strip()
    
    # Reemplazos específicos para kits y cadenas
    texto = texto.replace("|", ":")
    texto = texto.replace("_", ":")
    texto = texto.replace("=", ":")
    texto = texto.replace("(", " ")
    texto = texto.replace(")", " ")
    texto = texto.replace("/", " ")

    # Aplicar expresiones regulares de normalización
    texto = aplicar_expresiones_regulares_ordenadas(texto, expresiones_regulares)

    # Normalizar espacios múltiples
    texto = re.sub(r'\s+', ' ', texto).strip()

    return texto


def aplicar_reemplazos_diccionario_kits(texto, diccionario):
    """
    Aplica reemplazos específicos para kits usando el diccionario.
    """
    # Extraer variantes del diccionario para kits
    segun_variants = diccionario.get("segun_variants", [])
    producto_variants = diccionario.get("producto_variants", [])
    marca_variants = diccionario.get("marca_variants", [])
    referencia_variants = diccionario.get("referencia_variants", [])
    modelo_variants = diccionario.get("modelo_variants", [])
    cantidad_variants = diccionario.get("cantidad_variants", [])
    cadena_variants = diccionario.get("cadena_variants", [])
    kit_variants = diccionario.get("kit_variants", [])
    paso_variants = diccionario.get("paso_variants", [])
    marcas_conocidas = diccionario.get("marcas_conocidas", {})
    partes_variants = diccionario.get("partes_variants", {})
    referencia_modelo_variants = diccionario.get("referencia_modelo_variants", {})
    referencia_segun_variant = diccionario.get("referencia_segun_variant", {})
    unidades_variants = diccionario.get("unidades_medida", [])
    
    #Aplicar reemplazos de "SEGUN"
    for variant in segun_variants:
        if variant in texto:
                texto = texto.replace(variant, "SEGUN")

    # Aplicar reemplazos de productos
    for variant in producto_variants:
        if variant in texto:
            texto = texto.replace(variant, ", PRODUCTO:")
    
     
    # Aplicar reemplazos de marcas
    for variant in marca_variants:
        if variant in texto:
            texto = texto.replace(variant, ", MARCA:")
    
    # Aplicar reemplazos de referencias
    for variant in referencia_variants:
        if variant in texto:
            texto = texto.replace(variant, ", REFERENCIA:")
    
    # Aplicar reemplazos de modelos
    for variant in modelo_variants:
        if variant in texto:
            texto = texto.replace(variant, ", MODELO:")
    
    # Aplicar reemplazos de cantidades
    for variant in cantidad_variants:
        if variant in texto:
            texto = texto.replace(variant, ", CANTIDAD:")
    
    # Aplicar reemplazos de cadenas
    for variant in cadena_variants:
        if variant in texto:
            texto = texto.replace(variant, ", CADENA:")
    
    # Aplicar reemplazos de kits
    for variant in kit_variants:
        if variant in texto:
            texto = texto.replace(variant, ", KIT:")
    
    # Aplicar reemplazos de pasos
    for variant in paso_variants:
        if variant in texto:
            texto = texto.replace(variant, ", PASO:")

    # # Aplicar reemplazos de unidades
    # for variant in unidades_variants:
    #     if variant in texto:
    #         texto = texto.replace(variant, " UNIDADES, ")
   
    
     #Reemplazar diccionario de marcas conocidas
    for variant in marcas_conocidas:
        if variant in texto:
            # reemplazar variante por el valor de reemplazo del diccionario
            reemplazo = diccionario.get("marcas_conocidas", {}).get(variant, "")
            if reemplazo:
                texto = texto.replace(variant, reemplazo)  
    
    # Reemplazar partes variantes
    for variant in partes_variants:
        if variant in texto:
            # reemplazar variante por el valor de reemplazo del diccionario
            reemplazo = diccionario.get("partes_variants", {}).get(variant, "")
            if reemplazo:
                texto = texto.replace(variant, reemplazo)

    # Reemplazar referencias y modelos variantes
    for variant in referencia_modelo_variants:
        if variant in texto:
            # reemplazar variante por el valor de reemplazo del diccionario
            reemplazo = diccionario.get("referencia_modelo_variants", {}).get(variant, "")
            if reemplazo:
                texto = texto.replace(variant, reemplazo)
    
    # Reemplazar referencias según variante
    for variant in referencia_segun_variant:
        if variant in texto:
            # reemplazar variante por el valor de reemplazo del diccionario
            reemplazo = diccionario.get("referencia_segun_variant", {}).get(variant, "")
            if reemplazo:
                texto = texto.replace(variant, reemplazo)

    return texto


def extraer_productos(texto_procesado):
    """
    Extrae productos del texto procesado.
    """
    productos = set()
    
    patrones_producto = [
        r',\s*PRODUCTO:\s*([^,]+?)(?=\s*,\s*(?:MARCA|CANTIDAD|REFERENCIA|MODELO|CADENA|KIT|$))',
        r'PRODUCTO:\s*([^,]+?)(?=\s*,\s*(?:MARCA|CANTIDAD|REFERENCIA|MODELO|CADENA|KIT|$))',
        r',\s*PRODUCTO:\s*([^,\.;:]+?)(?=\s*[,\.;:]|$)'
    ]
    
    for patron in patrones_producto:
        matches = re.findall(patron, texto_procesado, re.IGNORECASE)
        for match in matches:
            producto = match.strip()
            producto = re.sub(r'[,;\.:\s]+$', '', producto)
            if producto and len(producto) > 2 and producto.upper() not in ['NO TIENE', 'NO ESPECIFICADO']:
                productos.add(producto)
    
    return list(productos) if productos else ['NO ESPECIFICADO']


def extraer_marcas_kits(texto_procesado):
    """
    Extrae marcas específicas para kits del texto procesado.
    """
    marcas = set()
    
    patrones_marca = [
        r',\s*MARCA:\s*([^,]+?)(?=\s*,\s*(?:MODELO|REFERENCIA|CANTIDAD|PRODUCTO|CADENA|KIT|$))',
        r'MARCA:\s*([^,]+?)(?=\s*,\s*(?:MODELO|REFERENCIA|CANTIDAD|PRODUCTO|CADENA|KIT|$))',
        r',\s*MARCA:\s*([^,\.;:]+?)(?=\s*[,\.;:]|$)'
    ]
    
    for patron in patrones_marca:
        matches = re.findall(patron, texto_procesado, re.IGNORECASE)
        for match in matches:
            marca = match.strip()
            marca = re.sub(r'[,;\.:\s]+$', '', marca)
            if marca and len(marca) > 1 and marca.upper() not in ['NO TIENE', 'NO ESPECIFICADA']:
                marcas.add(marca)

    return list(marcas) if marcas else ['NO ESPECIFICADA']


def extraer_referencias_kits(texto_procesado):
    """
    Extrae referencias específicas para kits del texto procesado.
    """
    referencias = set()
    
    patrones_referencia = [
        r',\s*REFERENCIA:\s*([^,]+?)(?=\s*,\s*(?:MARCA|CANTIDAD|PRODUCTO|MODELO|CADENA|KIT|$))',
        r'REFERENCIA:\s*([^,]+?)(?=\s*,\s*(?:MARCA|CANTIDAD|PRODUCTO|MODELO|CADENA|KIT|$))',
        r',\s*REFERENCIA:\s*([^,\.;:]+?)(?=\s*[,\.;:]|$)'
    ]
    
    for patron in patrones_referencia:
        matches = re.findall(patron, texto_procesado, re.IGNORECASE)
        for match in matches:
            referencia = match.strip()
            referencia = re.sub(r'[,;\.:\s]+$', '', referencia)
            if referencia and len(referencia) > 1 and referencia.upper() not in ['NO TIENE', 'NO ESPECIFICADA']:
                referencias.add(referencia)
    
    return list(referencias) if referencias else ['NO ESPECIFICADA']


def extraer_modelos(texto_procesado):
    """
    Extrae modelos del texto procesado.
    """
    modelos = set()
    
    patrones_modelo = [
        r',\s*MODELO:\s*([^,]+?)(?=\s*,\s*(?:MARCA|CANTIDAD|PRODUCTO|REFERENCIA|CADENA|KIT|$))',
        r'MODELO:\s*([^,]+?)(?=\s*,\s*(?:MARCA|CANTIDAD|PRODUCTO|REFERENCIA|CADENA|KIT|$))',
        r',\s*MODELO:\s*([^,\.;:]+?)(?=\s*[,\.;:]|$)'
    ]
    
    for patron in patrones_modelo:
        matches = re.findall(patron, texto_procesado, re.IGNORECASE)
        for match in matches:
            modelo = match.strip()
            modelo = re.sub(r'[,;\.:\s]+$', '', modelo)
            if modelo and len(modelo) > 1 and modelo.upper() not in ['NO TIENE', 'NO ESPECIFICADO']:
                modelos.add(modelo)
    
    return list(modelos) if modelos else ['NO ESPECIFICADO']


def extraer_cantidades_kits(texto_procesado):
    """
    Extrae cantidades específicas para kits del texto procesado.
    """
    cantidades = set()
    
    patrones_cantidad = [
        r'(\d+(?:\.\d+)?)\s*(?:UNIDADES?|PIEZA|KILOS|UND)',
        r',\s*CANTIDAD:\s*(\d+)\s*UNIDADES?',
        r',\s*CANTIDAD:\s*(\d+)\s*UND?',
        r',\s*CANTIDAD:\s*\(?(\d+)\)?\s*U(?!NIDAD)',
        r',\s*CANTIDAD:\s*(\d+)',
        r'CANTIDAD:\s*(\d+(?:\.\d+)?)\s*(?:UNIDADES?|UND)',
        r'CANT\s*\(\s*(\d+(?:\.\d+)?)\s*U\s*\)'
    ]
    
    for patron in patrones_cantidad:
        matches = re.findall(patron, texto_procesado, re.IGNORECASE)
        for match in matches:
            try:
                cantidad = float(match)
                if cantidad > 0:
                    cantidades.add(int(cantidad))
            except ValueError:
                continue
    
    return sorted(list(cantidades)) if cantidades else [0]


def detectar_cadenas(texto_procesado):
    """
    Detecta si el producto es una cadena o kit de arrastre.
    """
    patrones_cadena = [
        r'CADENA',
        r'CHAIN',
        r'CADENILLA',
        r'KIT\s+ARRASTRE',
        r'KIT\s+DE\s+ARRASTRE'
    ]
    
    for patron in patrones_cadena:
        if re.search(patron, texto_procesado, re.IGNORECASE):
            return True
    
    return False


def extraer_pasos_medidas(texto_procesado):
    """
    Extrae pasos y medidas de cadenas del texto procesado.
    """
    pasos = []
    
    patrones_paso = [
        r'(\d+H[-\s]*\d*L?)',
        r'(\d+\s*H)',
        r'(\d+\.\d+\s*MM)',
        r'(\d+MM)',
        r'PASO:\s*([^,]+?)(?=\s*,|$)',
        r'MEDIDA:\s*([^,]+?)(?=\s*,|$)'
    ]
    
    for patron in patrones_paso:
        matches = re.findall(patron, texto_procesado, re.IGNORECASE)
        for match in matches:
            paso = match.strip()
            if paso and len(paso) > 1:
                pasos.append(paso)
    
    return pasos if pasos else ['N/A']


def procesar_linea_kits(descripcion, numero_aceptacion, cantidad_original, expresiones_regulares, diccionario):
    """
    Procesa una línea completa del archivo de importación para kits y cadenas.
    """
    print(f"Entrando a procesar_linea_kits")

    # Limpiar y normalizar el texto
    texto_limpio = limpiar_y_normalizar_texto_kits(descripcion, expresiones_regulares)
 
    # Aplicar reemplazos del diccionario
    texto_procesado = aplicar_reemplazos_diccionario_kits(texto_limpio, diccionario)

    # Extraer datos estructurados
    productos = extraer_productos(texto_procesado)
    marcas = extraer_marcas_kits(texto_procesado)
    referencias = extraer_referencias_kits(texto_procesado)
    modelos = extraer_modelos(texto_procesado)
    cantidades = extraer_cantidades_kits(texto_procesado)
    es_cadena = detectar_cadenas(texto_procesado)
    pasos_medidas = extraer_pasos_medidas(texto_procesado)
    
   # Procesar cantidad original
    try:
        cantidad_limpia = re.sub(r"(\d+)[.,\s]00\b", r"\1", str(cantidad_original))
        cantidad_original_int = int(float(cantidad_limpia))
    except (ValueError, TypeError):
        cantidad_original_int = 0
    
    # Si no se encontraron cantidades en el texto, usar la cantidad original
    if not cantidades or cantidades == [0]:
        cantidades = [cantidad_original_int]
    
    #Crear archivo debug 
    debug_file = 'debug_kits.txt'
    with open(debug_file, 'w', encoding='utf-8') as f: 
        for producto in productos:
            f.write(f"Producto: {producto}\n")

    # Crear registros únicos
    registros = []
    max_elementos = max(len(productos), len(marcas), len(referencias), len(modelos), 1)
    
    for i in range(max_elementos):
        producto = productos[min(i, len(productos)-1)] if productos else 'NO ESPECIFICADO'
        marca = marcas[min(i, len(marcas)-1)] if marcas else 'NO ESPECIFICADA'
        referencia = referencias[min(i, len(referencias)-1)] if referencias else 'NO ESPECIFICADA'
        modelo = modelos[min(i, len(modelos)-1)] if modelos else 'NO ESPECIFICADO'
        cantidad = cantidades[min(i, len(cantidades)-1)] if cantidades else cantidad_original_int
        
        registro = {
            'numero_aceptacion': numero_aceptacion,
            'producto': producto,
            'marca': marca,
            'referencia': referencia,
            'modelo': modelo,
            'cantidad': cantidad,
            'unidad': 'UNIDADES',
            'es_cadena': 'SÍ' if es_cadena else 'NO',
            'pasos_medidas': 'N/A', 
            # Verificacion de los pasos de las cadenas
            #'pasos_medidas': ', '.join(pasos_medidas),
            'cantidad_original': cantidad_original,
            'fecha_procesamiento': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        if registro not in registros:
            registros.append(registro)

    return registros


def procesar_archivo_kits(directorio_salida, archivo_entrada, archivo_salida, archivo_diccionario="diccionario_kits.json", archivo_expresiones="expresiones_regulares_kits.json"):
    """
    Función principal que procesa el archivo de importación completo para kits y cadenas.
    """
    
    print(f"Entrando a procesar_archivo_kits")
    
    # Cargar configuraciones
    diccionario = cargar_diccionario_kits(archivo_diccionario)
    if diccionario is None:
        return
        
    expresiones_regulares = cargar_expresiones_regulares_kits(archivo_expresiones)
    
    # Debug expresiones_regulares y mostrar las expresiones cargadas
    if not expresiones_regulares:
        print("No se encontraron expresiones regulares para kits. Asegúrese de que el archivo JSON esté correctamente configurado.")
        return

    # Procesar archivo línea por línea
    resultados = []
    lineas_procesadas = 0
    lineas_con_error = 0

    archivo_salida = os.path.join(directorio_salida, archivo_salida)
    archivo_entrada = os.path.join(directorio_salida, archivo_entrada)
    
    if not os.path.exists(directorio_salida):
        os.makedirs(directorio_salida)
    
    try:
        with open(archivo_entrada, 'r', encoding='utf-8') as archivo:
            for numero_linea, linea in enumerate(archivo, 1):
                linea = linea.strip()
                if not linea:
                    continue
                
                try:
                    partes = linea.split('|')
                    if len(partes) >= 4:
                        numero_aceptacion = partes[0].strip()
                        descripcion = '|'.join(partes[1:-2]).strip()
                        cantidad_original = partes[-2].strip()
                        
                        registros_linea = procesar_linea_kits(
                            descripcion, numero_aceptacion, cantidad_original, 
                            expresiones_regulares, diccionario
                        )
                        
                        resultados.extend(registros_linea)    
                        lineas_procesadas += 1
                        
                    else:
                        lineas_con_error += 1
                        
                except Exception as e:
                    lineas_con_error += 1
                    continue
    
    except FileNotFoundError:
        print(f"Error: No se pudo encontrar el archivo {archivo_entrada}")
        return
    except Exception as e:
        print(f"Error al leer el archivo: {e}")
        return

    # Eliminar duplicados finales
    registros_unicos = []
    registros_vistos = set()
    
    for registro in resultados:
        clave_unica = (
            registro['numero_aceptacion'],
            registro['producto'],
            registro['marca'],
            registro['referencia'],
            registro['modelo'],
            registro['cantidad']
        )
        
        if clave_unica not in registros_vistos:
            registros_vistos.add(clave_unica)
            registros_unicos.append(registro)

    # Escribir archivo de salida
    try:
        with open(archivo_salida, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'numero_aceptacion', 'producto', 'marca', 'referencia', 'modelo', 
                'cantidad', 'unidad', 'es_cadena', 'pasos_medidas', 'cantidad_original', 
                'fecha_procesamiento'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter='|')
            
            writer.writeheader()
            for resultado in registros_unicos:
                writer.writerow(resultado)
        
        # Generar estadísticas
        total_productos = len(registros_unicos)
        productos_cadenas = len([r for r in registros_unicos if r['es_cadena'] == 'SÍ'])
        marcas_unicas = len(set(r['marca'] for r in registros_unicos if r['marca'] != 'NO ESPECIFICADA'))
        
        # Mostrar resultado final
        print(f"Procesamiento completado: {lineas_procesadas} líneas procesadas")
        print(f"Registros únicos generados: {total_productos}")
        print(f"Productos con cadenas: {productos_cadenas}")
        print(f"Marcas únicas identificadas: {marcas_unicas}")
        print(f"Archivo de salida: {archivo_salida}")
        
    except Exception as e:
        print(f"Error al escribir el archivo de salida: {e}")


if __name__ == "__main__":
    directorio_salida = 'data'
    archivo_entrada = "dataraw.csv"
    archivo_salida = "resultado_kits_procesado.csv"
    archivo_diccionario = "diccionario_kits.json"
    archivo_expresiones = "expresiones_regulares_kits.json"
    
    # print(f"Inicio de proceso de archivos de Excel para Kits")
    #procesar_archivos_raw(directorio_salida, archivo_entrada)

    print(f"Inicio de proceso de archivo de Kits y Cadenas")
    procesar_archivo_kits(directorio_salida, archivo_entrada, archivo_salida, archivo_diccionario, archivo_expresiones)