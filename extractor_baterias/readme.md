# Extractor de Baterías - Sistema de Procesamiento de Datos de Importación

## Descripción General

Este sistema está diseñado para procesar y normalizar datos de importación comercial, específicamente enfocado en la extracción y estructuración de información sobre baterías y componentes electrónicos. El programa toma archivos Excel con datos no estructurados y los convierte en información estructurada y normalizada.

## Características Principales

- **Procesamiento de múltiples archivos Excel**: Unifica datos de varios archivos en un único conjunto de datos
- **Normalización de texto**: Limpia y estandariza descripciones de mercancías
- **Extracción de entidades**: Identifica automáticamente referencias, marcas y cantidades
- **Sistema de diccionarios configurable**: Utiliza archivos JSON para personalizar la normalización
- **Expresiones regulares avanzadas**: Implementa patrones complejos para la limpieza de datos
- **Eliminación de duplicados**: Garantiza la unicidad de los registros procesados

## Arquitectura del Sistema

### Manejo de entorno virtual para Python

1. **Lógica de creacion del entorno**
Se crea usando la libreria venv (no restrictivo a Conda) y correr el comando
`python3 -m venv nombre_entorno`
Activar el entorno virtual usando el comando `source nombre_entorno/bin/activate`
Para desactivar el entorno se usa el comando `deactivate`
Por recomendación se debe evitar instalar las librerias fuera del entorno virtual.

2. **Uso de instalacion**
2.1. Se activa el entorno virtual `source nombre_entorno/bin/activate`
2.2. Se instalan las liberias dependientes `pip install -r requirements.txt`
2.3. En caso que no se tenga actualizado el PIP, usar el comando `pip install --upgrade pip` y luego ejecutar el comando 2.2.


### Componentes Principales

1. **Procesador de Archivos Excel** (`procesar_archivos_raw`)
2. **Motor de Normalización de Texto** (`limpiar_y_normalizar_texto`)
3. **Sistema de Extracción de Entidades** (`extraer_referencias`, `extraer_marcas`, `extraer_cantidades`)
4. **Procesador Principal** (`procesar_archivo_importacion`)

### Estructura de Directorios

```
proyecto/
├── extractor_baterias/
│   ├── dataraw/          # Archivos Excel de entrada
│   └── data/             # Archivos procesados de salida
├── diccionario.json      # Configuración de normalización
├── expresiones_regulares.json  # Patrones de texto
└── extractor_baterias.py # Código principal
```

## Dependencias del Sistema

```python
import re               # Expresiones regulares
import csv              # Manejo de archivos CSV
import json             # Procesamiento de configuración JSON
import pandas as pd     # Manipulación de datos estructurados
import os               # Operaciones del sistema de archivos
```

## Funciones Principales

### `procesar_archivos_raw(directorio_salida, archivo_entrada)`

**Propósito**: Unifica múltiples archivos Excel en un único archivo CSV estructurado.

**Parámetros**:
- `directorio_salida`: Directorio donde se guardará el archivo unificado
- `archivo_entrada`: Nombre del archivo CSV resultante

**Proceso**:
1. Busca archivos Excel en `extractor_baterias/dataraw/`
2. Lee la hoja 'DatosParte1' de cada archivo
3. Concatena todos los DataFrames en uno unificado
4. Normaliza las descripciones combinando múltiples columnas de detalle
5. Elimina caracteres especiales (`|`) que interfieren con el procesamiento
6. Exporta el resultado como CSV delimitado por `|`

**Columnas procesadas**:
- `Número de Aceptación` → `numeroaceptacion`
- `Descripción de la Mercancía Detallada 1-5` → `descripcion`
- `Cantidad` → `cantidad`
- `Unidad Comercial` → `unidades`

### `cargar_diccionario(archivo_diccionario)`

**Propósito**: Carga configuraciones de normalización desde archivo JSON.

**Estructura del diccionario**:
```json
{
  "segun_variants": ["SEGUN", "SEG UN", "SE GUN"],
  "factura_variants": ["FACTURA", "FACT URA", "FAC TURA"],
  "marca_variants": ["MARCA:", "MAR CA:", "M ARCA:"],
  "marcas_conocidas": {
    "DURACELL": "DURACELL",
    "ENERGIZER": "ENERGIZER"
  },
  "referencia_modelo_variants": {
    "AA-1.5V": "AA-1.5V-ALCALINA"
  }
}
```

### `cargar_expresiones_regulares(archivo_expresiones)`

**Propósito**: Carga patrones de expresiones regulares para normalización avanzada.

**Estructura de expresiones**:
```json
{
  "expresiones_regulares": {
    "normalizar_cantidad_unidad_decimales": {
      "patron": "\\b(\\d+),00\\s*(UNIDADES?)\\b",
      "reemplazo": ", CANTIDAD: \\1"
    },
    "separar_cantidad_producto": {
      "patron": "(\\d+)([A-Z]+)",
      "reemplazo": "\\1 \\2"
    }
  }
}
```

### `limpiar_y_normalizar_texto(texto, expresiones_regulares)`

**Propósito**: Aplica transformaciones comprehensivas al texto de entrada.

**Transformaciones aplicadas**:

1. **Reemplazo de símbolos especiales**:
   ```python
   texto = texto.replace("|", ":")
   texto = texto.replace("_", ":")
   texto = texto.replace("=", ":")
   ```

2. **Normalización de separadores**:
   ```python
   texto = texto.replace("(", " ")
   texto = texto.replace(")", " ")
   texto = texto.replace(";", ",")
   ```

3. **Corrección de términos específicos**:
   ```python
   texto = texto.replace("PAIS", ", PAIS")
   texto = texto.replace("P. ORIGEN:", ", PAIS:")
   texto = texto.replace("N O TIENE", "NO TIENE")
   ```

4. **Aplicación de expresiones regulares ordenadas**
5. **Conversión a mayúsculas**
6. **Normalización de espacios múltiples**

### `aplicar_expresiones_regulares_ordenadas(texto, expresiones_regulares)`

**Propósito**: Aplica patrones de normalización en secuencia específica.

**Orden de aplicación**:
1. `normalizar_cantidad_unidad_decimales`
2. `normalizar_cantidad_unidad_enteros`
3. `separar_cantidad_producto`
4. `normalizar_espacios_antes_producto`
5. `normalizar_cantidad_punto_coma`
6. `normalizar_cantidad_coma`
7. `eliminar_decimales`
8. `normalizar_punto_coma_mercancia`
9. `limpiar_espacios_multiples`
10. `patron_palabra_cantidad`
11. `normalizar_cantidad_espacio`

### `extraer_referencias(texto_procesado, diccionario)`

**Propósito**: Extrae códigos de referencia de productos usando múltiples patrones.

**Patrones utilizados**:
```python
patrones_referencia = [
    r',\s*REFERENCIA:\s*([^,]+?)(?=\s*,\s*(?:MARCA|CANTIDAD|REFERENCIA|PRODUCTO|MODELO|CODIGO|SERIAL|$))',
    r'REFERENCIA:\s*([^,]+?)(?=\s*,\s*(?:MARCA|CANTIDAD|REFERENCIA|PRODUCTO|MODELO|CODIGO|SERIAL|$))',
    r',\s*REFERENCIA:\s*([^,\.;:]+?)(?=\s*[,\.;:]|$)'
]
```

**Proceso**:
1. Busca patrones de referencia en el texto
2. Aplica correcciones usando el diccionario
3. Filtra referencias inválidas ('NO TIENE', 'SEGUN FACTURA')
4. Normaliza formato eliminando caracteres finales

### `extraer_marcas(texto_procesado)`

**Propósito**: Identifica nombres de marcas comerciales.

**Patrones de búsqueda**:
```python
patrones_marca = [
    r',\s*MARCA:\s*([^,]+?)(?=\s*,\s*(?:MODELO|REFERENCIA|CANTIDAD|MARCA|PRODUCTO|CODIGO|SERIAL|$))',
    r'MARCA:\s*([^,]+?)(?=\s*,\s*(?:MODELO|REFERENCIA|CANTIDAD|MARCA|PRODUCTO|CODIGO|SERIAL|$))',
    r',\s*MARCA:\s*([^,\.;:]+?)(?=\s*[,\.;:]|$)'
]
```

### `extraer_cantidades(texto_procesado)`

**Propósito**: Extrae valores numéricos de cantidad.

**Patrones soportados**:
```python
patrones_cantidad = [
    r',\s*CANTIDAD:\s*(\d+)\s*UNIDADES?',    # "CANTIDAD: 10 UNIDADES"
    r',\s*CANTIDAD:\s*(\d+)\s*U(?!NIDAD)',   # "CANTIDAD: 10 U"
    r',\s*CANTIDAD:\s*\(?(\d+)\)?\s*U(?!NIDAD)',  # "CANTIDAD: (10) U"
    r',\s*CANTIDAD:\s*(\d+)'                 # "CANTIDAD: 10"
]
```

### `procesar_linea_importacion(descripcion, numero_aceptacion, cantidad_original, expresiones_regulares, diccionario)`

**Propósito**: Procesa una línea completa del archivo de importación.

**Flujo de procesamiento**:
1. **Limpieza y normalización**: Aplica todas las transformaciones de texto
2. **Aplicación de diccionario**: Reemplaza términos según configuración
3. **Extracción de entidades**: Obtiene referencias, marcas y cantidades
4. **Creación de registros únicos**: Genera combinaciones válidas de datos
5. **Validación de cantidad**: Usa cantidad original si no se encuentra en texto

### `procesar_archivo_importacion(directorio_salida, archivo_entrada, archivo_salida, archivo_diccionario, archivo_expresiones)`

**Propósito**: Función principal que orquesta todo el procesamiento.

**Flujo completo**:
1. **Carga de configuraciones**: Diccionario y expresiones regulares
2. **Procesamiento línea por línea**: Lee archivo CSV de entrada
3. **Análisis de estructura**: Separa campos usando delimitador `|`
4. **Aplicación de procesamiento**: Para cada línea válida
5. **Eliminación de duplicados**: Garantiza unicidad de registros
6. **Exportación de resultados**: Guarda archivo CSV estructurado

**Formato de salida**:
```csv
numero_aceptacion,referencia,marca,cantidad,cantidad_original
2023001,AA-1.5V-ALCALINA,DURACELL,4,4
2023002,CR2032,ENERGIZER,10,10
```

## Uso del Sistema

### Ejecución Principal

```python
if __name__ == "__main__":
    directorio_salida = 'extractor_baterias/data'
    archivo_entrada = "dataraw.csv"
    archivo_salida = "resultado_procesado.csv"
    archivo_diccionario = "diccionario.json"
    archivo_expresiones = "expresiones_regulares.json"
    
    # Fase 1: Procesamiento de archivos Excel
    print(f"Inicio de proceso de archivos de Excel")
    procesar_archivos_raw(directorio_salida, archivo_entrada)

    # Fase 2: Extracción y normalización
    print(f"Inicio de proceso de archivo de Baterias")
    procesar_archivo_importacion(directorio_salida, archivo_entrada, 
                                archivo_salida, archivo_diccionario, 
                                archivo_expresiones)
```

### Requisitos de Archivos de Entrada

1. **Archivos Excel**: Deben estar en `extractor_baterias/dataraw/`
2. **Hoja requerida**: 'DatosParte1'
3. **Columnas necesarias**:
   - Número de Aceptación
   - Descripción de la Mercancía Detallada 1-5
   - Cantidad
   - Unidad Comercial

### Configuración del Sistema

#### Archivo `diccionario.json`
Define variantes de términos y correcciones específicas para normalización.

#### Archivo `expresiones_regulares.json`
Contiene patrones regex para transformaciones avanzadas de texto.

## Casos de Uso Típicos

### Entrada Típica
```
"BATERIA AA 1.5V MARCA: DURACELL CANTIDAD: 4 UNIDADES REFERENCIA: AA-1.5V"
```

### Salida Estructurada
```csv
numero_aceptacion,referencia,marca,cantidad,cantidad_original
2023001,AA-1.5V,DURACELL,4,4
```

## Manejo de Errores

- **Archivos no encontrados**: Mensajes informativos y terminación controlada
- **Formato JSON inválido**: Validación de estructura de configuración
- **Líneas malformadas**: Contador de errores y continuación del procesamiento
- **Expresiones regulares inválidas**: Captura de excepciones y omisión del patrón

## Consideraciones de Rendimiento

- **Procesamiento en lotes**: Maneja múltiples archivos Excel simultáneamente
- **Eliminación eficiente de duplicados**: Usa conjuntos (sets) para optimización
- **Lectura línea por línea**: Minimiza uso de memoria para archivos grandes
- **Compilación de regex**: Reutiliza patrones compilados implícitamente

## Extensibilidad

El sistema es altamente configurable mediante:
- **Diccionarios personalizables**: Agregar nuevas variantes de términos
- **Expresiones regulares modulares**: Añadir nuevos patrones de normalización
- **Orden de aplicación configurable**: Modificar secuencia de transformaciones
- **Patrones de extracción extensibles**: Crear nuevos métodos de extracción

## Salida del Sistema

El programa genera dos archivos principales:
1. **`dataraw.csv`**: Datos unificados de archivos Excel
2. **`resultado_procesado.csv`**: Datos estructurados y normalizados

Además, proporciona información de progreso detallada durante la ejecución, incluyendo contadores de líneas procesadas y registros únicos generados.