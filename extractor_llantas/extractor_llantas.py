import re
import csv
import json
import pandas as pd
import os


def procesar_archivos_raw(directorio_salida,archivo_entrada):
    excel_dir = 'dataraw'
    excel_dir = os.path.join(excel_dir, excel_dir)
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
    #Remover "|" de los datos originales
    unified_df = unified_df.replace('|', '', regex=True)

    detalle_cols = [
        'Descripción de la Mercancía Detallada 1',
        'Descripción de la Mercancía Detallada 2',
        'Descripción de la Mercancía Detallada 3',
        'Descripción de la Mercancía Detallada 4',
        'Descripción de la Mercancía Detallada 5'
    ]

    # Descargar columnas
    detalle_cols = [col for col in detalle_cols if col in unified_df.columns]

    print("\nNormalizando Descripciones de Mercancía")
    # Concatenando espacios
    unified_df['descripcion'] = unified_df[detalle_cols].fillna('').agg(' '.join, axis=1).str.strip()

    # Creacion del nuevo Dataframe
    final_df = unified_df[['Número de Aceptación', 'descripcion', 'Cantidad', 'Unidad Comercial']].copy()

    final_df = final_df.rename(columns={
        'Número de Aceptación': 'numeroaceptacion',
        'Cantidad': 'cantidad',
        'Unidad Comercial': 'unidades',
        'Descripción de la Mercancía Detallada': 'descripcion'
    })

    print("\nIniciando creacion de Dataframe final")
    # Save the unified DataFrame to a CSV file
    directory = directorio_salida
    if not os.path.exists(directory):
        os.makedirs(directory)
        
    output_file = os.path.join(directory, archivo_entrada)
    final_df.to_csv(output_file, index=False, sep="|")
    print(f"\nDataframe generado en archivo: {output_file}")


def cargar_diccionario(archivo_diccionario):
    """
    Carga el diccionario desde un archivo JSON.
    """
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

def cargar_expresiones_regulares(archivo_expresiones):
    """
    Carga las expresiones regulares desde un archivo JSON.
    """
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
        "normalizar_cantidad_unidad_decimales",
        "normalizar_cantidad_unidad_enteros", 
        "separar_cantidad_producto",
        "normalizar_espacios_antes_producto",
        "normalizar_cantidad_punto_coma",
        "normalizar_cantidad_coma",
        "eliminar_decimales",
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

def limpiar_y_normalizar_texto(texto, expresiones_regulares):
    """
    Aplica las transformaciones completas de limpieza y normalización al texto.
    """
    # Reemplazar símbolos por caracteres estándar
    texto = texto.replace("|", ":")
    texto = texto.replace("_", ":")
    texto = texto.replace("=", ":")
    texto = texto.replace("(", " ")
    texto = texto.replace(")", " ")
    texto = texto.replace("PAIS", ", PAIS")
    texto = texto.replace("P. ORIGEN:", ", PAIS:")
    texto = texto.replace("CANTIDAD DECLARADA: ", ", DECLARADA :")
    texto = texto.replace("CANTIDAD FACTURADA:", ", CANTIDAD: ")
    texto = texto.replace("N O TIENE", "NO TIENE")
    texto = texto.replace("NO TI ENE", "NO TIENE")
    texto = texto.replace("NO TIEN E", "NO TIENE")
    texto = texto.replace("NO TIE NE", "NO TIENE")
    texto = texto.replace(" WC",", WC")
    texto = texto.replace("REFERENCIA: ARANCELARIA",", ARANCEL")
    texto = texto.replace("REFERENCIA: ARANCELARI A",", ARANCEL")
    texto = texto.replace("PARTE NUMERO ", "")
    texto = texto.replace("PA RTE NUMERO ", "")
    texto = texto.replace("UNIDADES:", "UNIDADES.")
    texto = texto.replace("MARCA: IMPORTADOR:", "")
    texto = texto.replace("MODELO",", MODELO")
    texto = texto.replace("ITEM",", ITEM")
    texto = texto.replace(", MARCA: SEGUN FACTURA", ", MARCA: ")
    texto = texto.replace("SERIAL:", ", SERIAL:")
    texto = texto.replace("YMARCA:", "Y, MARCA: ")
    texto = texto.replace("U6224", "QU6224")
    texto = texto.replace("U 6224", "QU6224")
    texto = texto.replace("SEGUN ORDEN DE COMPRA", "")
    texto = texto.replace(". USO", ", USO")
    texto = texto.replace(" USO", ", USO")
    texto = texto.replace(" BATERIA", ", BATERIA")
    texto = texto.replace(";", ",")
    texto = texto.replace("/", ",")
    
     
    # Aplicar expresiones regulares de normalización
    texto = aplicar_expresiones_regulares_ordenadas(texto, expresiones_regulares)
    # Convertir a mayúsculas
    texto = texto.upper()
    
    # Normalizar espacios múltiples
    texto = re.sub(r'\s+', ' ', texto).strip()
    
    return texto

def aplicar_reemplazos_diccionario(texto, diccionario):
    """
    Aplica los reemplazos de palabras clave según el diccionario.
    """
    # Extraer variantes del diccionario
    segun_variants = diccionario.get("segun_variants", [])
    factura_variants = diccionario.get("factura_variants", [])
    referencia_variants = diccionario.get("referencia_variants", [])
    marca_variants = diccionario.get("marca_variants", [])
    cantidad_variants = diccionario.get("cantidad_variants", [])
    codigo_variants = diccionario.get("codigo_variants", [])
    producto_variants = diccionario.get("producto_variants", [])
    marcas_conocidas = diccionario.get("marcas_conocidas", [])
    
    # Aplicar reemplazos
    for variant in segun_variants:
        if variant in texto:
            texto = texto.replace(variant, " SEGUN ")
    
    for variant in factura_variants:
        if variant in texto:
            texto = texto.replace(variant, " FACTURA ")
    
    for variant in referencia_variants:
        if variant in texto:
            texto = texto.replace(variant, ", REFERENCIA: ")
    
    for variant in marca_variants:
        if variant in texto:
            texto = texto.replace(variant, ", MARCA: ")
    
    for variant in cantidad_variants:
       if variant in texto:
            texto = texto.replace(variant, ", CANTIDAD: ")
    
    for variant in codigo_variants:
        if variant in texto:
            texto = texto.replace(variant, ", CODIGO: ")
    
    for variant in producto_variants:
        if variant in texto:
            texto = texto.replace(variant, ", PRODUCTO: ")
    

    #Reemplazar diccionario de marcas conocidas
    for variant in marcas_conocidas:
        if variant in texto:
            # reemplazar variante por el valor de reemplazo del diccionario
            reemplazo = diccionario.get("marcas_conocidas", {}).get(variant, "")
            if reemplazo:
                texto = texto.replace(variant, reemplazo)   
    
    # Reemplazar diccionario de referencias modelo
    for variant in diccionario.get("referencia_modelo_variants", {}):
        if variant in texto:
            # reemplazar variante por el valor de reemplazo del diccionario
            reemplazo = diccionario.get("referencia_modelo_variants", {}).get(variant, "")
            if reemplazo:
                texto = texto.replace(variant, reemplazo)

    return texto

def aplicar_correcciones_referencia(referencia, diccionario):
    """
    Aplica correcciones específicas a las referencias usando el diccionario.
    """
    referencia_modelo_variants = diccionario.get("referencia_modelo_variants", {})
    
    for variante_incorrecta, referencia_correcta in referencia_modelo_variants.items():
        if referencia.upper().strip() == variante_incorrecta.upper().strip():
            return referencia_correcta
    
    referencia_normalizada = re.sub(r'\s*-\s*', '-', referencia)
    referencia_normalizada = re.sub(r'\s+', ' ', referencia_normalizada).strip()
    
    return referencia_normalizada

def extraer_referencias(texto_procesado, diccionario):
    """
    Extrae todas las referencias del texto procesado.
    """
    referencias = set()
    
    patrones_referencia = [
        r',\s*REFERENCIA:\s*([^,]+?)(?=\s*,\s*(?:MARCA|CANTIDAD|REFERENCIA|PRODUCTO|MODELO|CODIGO|SERIAL|$))',
        r'REFERENCIA:\s*([^,]+?)(?=\s*,\s*(?:MARCA|CANTIDAD|REFERENCIA|PRODUCTO|MODELO|CODIGO|SERIAL|$))',
        r',\s*REFERENCIA:\s*([^,\.;:]+?)(?=\s*[,\.;:]|$)'
    ]
    
    for patron in patrones_referencia:
        matches = re.findall(patron, texto_procesado, re.IGNORECASE)
        for match in matches:
            referencia = match.strip()
            referencia = re.sub(r'[,;\.:\s]+$', '', referencia)
            if referencia and len(referencia) > 1 and referencia.upper() not in ['NO TIENE', 'SEGUN FACTURA']:
                referencia_corregida = aplicar_correcciones_referencia(referencia, diccionario)
                referencias.add(referencia_corregida)
    
    return list(referencias) if referencias else ['NO TIENE']

def extraer_marcas(texto_procesado):
    """
    Extrae todas las marcas del texto procesado.
    """
    marcas = set()
    
    patrones_marca = [
        r',\s*MARCA:\s*([^,]+?)(?=\s*,\s*(?:MODELO|REFERENCIA|CANTIDAD|MARCA|PRODUCTO|CODIGO|SERIAL|$))',
        r'MARCA:\s*([^,]+?)(?=\s*,\s*(?:MODELO|REFERENCIA|CANTIDAD|MARCA|PRODUCTO|CODIGO|SERIAL|$))',
        r',\s*MARCA:\s*([^,\.;:]+?)(?=\s*[,\.;:]|$)'
    ]
    
    for patron in patrones_marca:
        matches = re.findall(patron, texto_procesado, re.IGNORECASE)
        for match in matches:
            marca = match.strip()
            marca = re.sub(r'[,;\.:\s]+$', '', marca)
            if marca and len(marca) > 1 and marca.upper() != 'NO TIENE':
                marcas.add(marca)

    return list(marcas) if marcas else ['NO TIENE']

def extraer_cantidades(texto_procesado):
    """
    Extrae todas las cantidades del texto procesado.
    """
    cantidades = set()
    
    patrones_cantidad = [
        r',\s*CANTIDAD:\s*(\d+)\s*UNIDADES?',
        r',\s*CANTIDAD:\s*(\d+)\s*U(?!NIDAD)',
        r',\s*CANTIDAD:\s*\(?(\d+)\)?\s*U(?!NIDAD)',
        r',\s*CANTIDAD:\s*(\d+)',
        r'CANTIDAD:\s*(\d+)\s*UNIDADES?',
        r'CANTIDAD:\s*(\d+)\s*U(?!NIDAD)',
        r'CANTIDAD:\s*\(?(\d+)\)?\s*U(?!NIDAD)',
        r'CANTIDAD:\s*(\d+)'
    ]
    
    for patron in patrones_cantidad:
        matches = re.findall(patron, texto_procesado, re.IGNORECASE)
        for match in matches:
            try:
                cantidad = int(match)
                if cantidad > 0:
                    cantidades.add(cantidad)
            except ValueError:
                continue
    
    return sorted(list(cantidades)) if cantidades else 0

def procesar_linea_importacion(descripcion, numero_aceptacion, cantidad_original, expresiones_regulares, diccionario):
    """
    Procesa una línea completa del archivo de importación.
    """
    # Limpiar y normalizar el texto
    texto_limpio = limpiar_y_normalizar_texto(descripcion, expresiones_regulares)
    
    # Aplicar reemplazos del diccionario
    texto_procesado = aplicar_reemplazos_diccionario(texto_limpio, diccionario)
    
    # Extraer datos estructurados
    referencias = extraer_referencias(texto_procesado, diccionario)
    marcas = extraer_marcas(texto_procesado)
    cantidades = extraer_cantidades(texto_procesado)

    
    # Procesar cantidad original
    try:
        cantidad_limpia = re.sub(r"(\d+)[.,]00\b", r"\1", str(cantidades))
        cantidad_original_int = int(float(cantidad_limpia))
    except (ValueError, TypeError):
        cantidad_original_int = 0
    
    # Si no se encontraron cantidades en el texto, usar la cantidad original
    if not cantidades and cantidades == 0:
        cantidades = [cantidad_original]
    
    # Crear registros únicos
    registros = []
    max_elementos = max(len(referencias), len(marcas), len(cantidades), 1)
    
    for i in range(max_elementos):
        referencia = referencias[min(i, len(referencias)-1)] if referencias else 'NO TIENE'
        marca = marcas[min(i, len(marcas)-1)] if marcas else 'NO TIENE'
        cantidad = cantidades[min(i, len(cantidades)-1)] if cantidades else cantidad_original_int
        
        registro = {
            'numero_aceptacion': numero_aceptacion,
            'referencia': referencia,
            'marca': marca,
            'cantidad': cantidad,
            'cantidad_original': cantidad_original
        }
        
        if registro not in registros:
            registros.append(registro)
    
    return registros

def procesar_archivo_importacion(directorio_salida,archivo_entrada, archivo_salida, archivo_diccionario="diccionario.json", archivo_expresiones="expresiones_regulares.json"):
    """
    Función principal que procesa el archivo de importación completo.
    """
    # Cargar configuraciones
    diccionario = cargar_diccionario(archivo_diccionario)
    if diccionario is None:
        return
    
    expresiones_regulares = cargar_expresiones_regulares(archivo_expresiones)
    
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
                        # unidades = partes[-1].strip()
                        
                        registros_linea = procesar_linea_importacion(
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
            registro['referencia'],
            registro['marca'],
            registro['cantidad']
        )
        
        if clave_unica not in registros_vistos:
            registros_vistos.add(clave_unica)
            registros_unicos.append(registro)

    # Escribir archivo de salida
    try:
        with open(archivo_salida, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['numero_aceptacion', 'referencia', 'marca', 'cantidad', 'cantidad_original']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for resultado in registros_unicos:
                writer.writerow(resultado)
        
        # Mostrar únicamente resultado final
        print(f"Procesamiento completado: {lineas_procesadas} líneas procesadas, {len(registros_unicos)} registros únicos generados")
        print(f"Archivo de salida: {archivo_salida}")
        
    except Exception as e:
        print(f"Error al escribir el archivo de salida: {e}")

if __name__ == "__main__":
    directorio_salida = 'extractor_general/data'
    archivo_entrada = "dataraw.csv"
    archivo_salida = "resultado_procesado.csv"
    archivo_diccionario = "diccionario.json"
    archivo_expresiones = "expresiones_regulares.json"
    
    print(f"Inicio de proceso de archivos de Excel")
    procesar_archivos_raw(directorio_salida,archivo_entrada)

    print(f"Inicio de proceso de archivo de Baterias")
    procesar_archivo_importacion(directorio_salida,archivo_entrada, archivo_salida, archivo_diccionario, archivo_expresiones)