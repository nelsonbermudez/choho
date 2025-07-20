#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analizador de Registros de Importación
======================================

Este script procesa registros de importación desde un archivo de texto,
extrae información estructurada de productos, marcas, referencias y 
especificaciones técnicas, y genera reportes en CSV y Excel.

Autor: Sistema de Análisis
Fecha: 2025
"""

import re
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
import argparse
import logging
from pathlib import Path
import json
from datetime import datetime


class ImportacionAnalyzer:
    """
    Clase principal para analizar registros de importación.
    
    Attributes:
        data (List[Dict]): Lista de registros procesados
        logger (logging.Logger): Logger para el seguimiento de operaciones
    """
    
    def __init__(self, log_level: str = "INFO"):
        """
        Inicializa el analizador con configuración de logging.
        
        Args:
            log_level (str): Nivel de logging (DEBUG, INFO, WARNING, ERROR)
        """
        self.data = []
        self.setup_logging(log_level)
        
        # Patrones de expresiones regulares para extracción
        self.patterns = {
            'producto': [
                r'PRODUCTO\s*[=:]\s*([^,;|]+?)(?:,|;|USO\s*=)',
                r'Nombre Comercial:\s*([^,]+)'
            ],
            'marca': [
                r'MARCA\s*[=:]\s*([^,;|]+?)(?:,|;|\s)',
                r'Marca\s+C:\s*([^,]+)'
            ],
            'referencia': [
                r'REFERENCIA[^=]*=\s*([^,;|]+?)(?:,|;|\s)',
                r'Ref:\s*([^;|,]+)'
            ],
            'modelo': [
                r'MODELO\s*[=:]\s*([^,;|]+?)(?:,|;|\s)'
            ],
            'cantidad': [
                r'(\d+(?:\.\d+)?)\s*(?:Unidad|Pieza|Kilos)',
                r'CANTIDAD\s+(\d+(?:\.\d+)?)\s+UNIDADES',
                r'CANT\s*\(\s*(\d+(?:\.\d+)?)\s*U\s*\)',
                r'(\d+(?:\.\d+)?)\s*UND'
            ],
            'unidad': r'(Unidad|Pieza|Kilos|UND)',
            'cadena': r'CADENA|CHAIN|CADENILLA|KIT\s+ARRASTRE',
            'paso': r'(\d+H[-\s]*\d*L?|\d+\s*H|\d+\.\d+\s*MM|\d+MM)'
        }
    
    def setup_logging(self, level: str) -> None:
        """Configura el sistema de logging."""
        logging.basicConfig(
            level=getattr(logging, level.upper()),
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('importaciones_analysis.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def load_file(self, file_path: str) -> str:
        """
        Carga el archivo de texto con los registros de importación.
        
        Args:
            file_path (str): Ruta al archivo de texto
            
        Returns:
            str: Contenido del archivo
            
        Raises:
            FileNotFoundError: Si el archivo no existe
            UnicodeDecodeError: Si hay problemas de codificación
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()
            self.logger.info(f"Archivo cargado exitosamente: {file_path}")
            return content
        except FileNotFoundError:
            self.logger.error(f"Archivo no encontrado: {file_path}")
            raise
        except UnicodeDecodeError:
            # Intentar con diferentes codificaciones
            encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            for encoding in encodings:
                try:
                    with open(file_path, 'r', encoding=encoding) as file:
                        content = file.read()
                    self.logger.info(f"Archivo cargado con codificación {encoding}: {file_path}")
                    return content
                except UnicodeDecodeError:
                    continue
            raise UnicodeDecodeError("No se pudo decodificar el archivo con ninguna codificación")
    
    def extract_with_pattern(self, content: str, patterns: List[str]) -> List[str]:
        """
        Extrae información usando múltiples patrones de regex.
        
        Args:
            content (str): Contenido a analizar
            patterns (List[str]): Lista de patrones regex
            
        Returns:
            List[str]: Lista de coincidencias encontradas
        """
        matches = []
        for pattern in patterns:
            found = re.findall(pattern, content, re.IGNORECASE)
            if found:
                # Limpiar y procesar las coincidencias
                cleaned = [match.strip() for match in found if match.strip()]
                matches.extend(cleaned)
        
        # Eliminar duplicados manteniendo el orden
        unique_matches = []
        for match in matches:
            if match not in unique_matches:
                unique_matches.append(match)
        
        return unique_matches
    
    def extract_quantities_and_units(self, content: str) -> List[Tuple[float, str]]:
        """
        Extrae cantidades y unidades del contenido.
        
        Args:
            content (str): Contenido a analizar
            
        Returns:
            List[Tuple[float, str]]: Lista de tuplas (cantidad, unidad)
        """
        quantities_units = []
        
        # Buscar patrones que incluyan cantidad y unidad juntos
        patterns = [
            r'(\d+(?:\.\d+)?)\s*(Unidad|Pieza|Kilos|UND)',
            r'CANTIDAD\s+(\d+(?:\.\d+)?)\s+(UNIDADES)',
            r'CANT\s*\(\s*(\d+(?:\.\d+)?)\s*(U)\s*\)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                try:
                    quantity = float(match[0])
                    unit = match[1] if match[1].upper() != 'U' else 'Unidad'
                    if quantity > 0:
                        quantities_units.append((quantity, unit))
                except (ValueError, IndexError):
                    continue
        
        return quantities_units
    
    def extract_products_with_quantities(self, content: str) -> List[Dict]:
        """
        Extrae productos con sus cantidades asociadas.
        
        Args:
            content (str): Contenido a analizar
            
        Returns:
            List[Dict]: Lista de productos con cantidades
        """
        products_with_qty = []
        
        # Buscar patrones que incluyan cantidad y producto juntos
        product_qty_pattern = r'(\d+(?:\.\d+)?)\s*(Unidad|Pieza|Kilos)\s+PRODUCTO\s*[=:]\s*([^,;|]+?)(?:,|;|USO\s*=)'
        
        matches = re.findall(product_qty_pattern, content, re.IGNORECASE)
        for match in matches:
            try:
                quantity = float(match[0])
                unit = match[1]
                product = match[2].strip()
                if quantity > 0 and product:
                    products_with_qty.append({
                        'cantidad': quantity,
                        'unidad': unit,
                        'producto': product
                    })
            except (ValueError, IndexError):
                continue
        
        # Si no encontramos productos con cantidades específicas, usar patrones separados
        if not products_with_qty:
            productos = self.extract_with_pattern(content, self.patterns['producto'])
            productos = self.clean_extracted_data(productos)
            
            cantidades_unidades = self.extract_quantities_and_units(content)
            
            # Asociar cantidades con productos
            for i, producto in enumerate(productos):
                if i < len(cantidades_unidades):
                    cantidad, unidad = cantidades_unidades[i]
                elif cantidades_unidades:
                    cantidad, unidad = cantidades_unidades[0]
                else:
                    cantidad, unidad = 1, 'Unidad'
                
                products_with_qty.append({
                    'cantidad': cantidad,
                    'unidad': unidad,
                    'producto': producto
                })
        
        return products_with_qty
    
    def clean_extracted_data(self, data: List[str]) -> List[str]:
        """
        Limpia y normaliza los datos extraídos.
        
        Args:
            data (List[str]): Lista de datos sin procesar
            
        Returns:
            List[str]: Lista de datos limpiados
        """
        cleaned = []
        for item in data:
            # Remover caracteres de control y espacios extra
            clean_item = re.sub(r'[^\w\s\-/().]', ' ', item)
            clean_item = re.sub(r'\s+', ' ', clean_item).strip()
            
            # Filtrar items muy cortos o vacíos
            if len(clean_item) > 2:
                cleaned.append(clean_item)
        
        return cleaned
    
    def extract_record_data(self, record: str) -> Dict:
        """
        Extrae información estructurada de un registro individual.
        
        Args:
            record (str): Registro de importación completo
            
        Returns:
            Dict: Diccionario con la información extraída
        """
        parts = record.split('|')
        if len(parts) < 2:
            return None
        
        numero_aceptacion = parts[0].strip()
        content = parts[1]
        
        # Extraer productos con cantidades
        productos_con_cantidad = self.extract_products_with_quantities(content)
        
        # Extraer marcas
        marcas = self.extract_with_pattern(content, self.patterns['marca'])
        marcas = self.clean_extracted_data(marcas)
        
        # Extraer referencias
        referencias = self.extract_with_pattern(content, self.patterns['referencia'])
        referencias = self.clean_extracted_data(referencias)
        
        # Extraer modelos
        modelos = self.extract_with_pattern(content, self.patterns['modelo'])
        modelos = self.clean_extracted_data(modelos)
        
        # Detectar cadenas
        es_cadena = bool(re.search(self.patterns['cadena'], content, re.IGNORECASE))
        
        # Extraer pasos si es cadena
        pasos = []
        if es_cadena:
            pasos = re.findall(self.patterns['paso'], content, re.IGNORECASE)
            pasos = [paso.strip() for paso in pasos if paso.strip()]
        
        # Calcular totales
        cantidad_total = sum(item['cantidad'] for item in productos_con_cantidad)
        
        return {
            'numero_aceptacion': numero_aceptacion,
            'productos_con_cantidad': productos_con_cantidad,
            'marcas': marcas,
            'referencias': referencias,
            'modelos': modelos,
            'es_cadena': es_cadena,
            'pasos': pasos,
            'cantidad_total': cantidad_total,
            'total_productos': len(productos_con_cantidad),
            'total_marcas': len(marcas),
            'total_referencias': len(referencias)
        }
    
    def process_file(self, file_path: str) -> List[Dict]:
        """
        Procesa el archivo completo y extrae todos los registros.
        
        Args:
            file_path (str): Ruta al archivo
            
        Returns:
            List[Dict]: Lista de registros procesados
        """
        self.logger.info("Iniciando procesamiento del archivo...")
        
        content = self.load_file(file_path)
        records = content.strip().split('\n')
        records = [record.strip() for record in records if record.strip()]
        
        self.logger.info(f"Encontrados {len(records)} registros para procesar")
        
        processed_records = []
        for i, record in enumerate(records, 1):
            try:
                data = self.extract_record_data(record)
                if data:
                    data['registro_numero'] = i
                    processed_records.append(data)
                    self.logger.debug(f"Registro {i} procesado exitosamente")
            except Exception as e:
                self.logger.warning(f"Error procesando registro {i}: {str(e)}")
        
        self.data = processed_records
        self.logger.info(f"Procesamiento completado: {len(processed_records)} registros válidos")
        return processed_records
    
    def create_detailed_dataframe(self) -> pd.DataFrame:
        """
        Crea un DataFrame detallado con un producto por fila.
        
        Returns:
            pd.DataFrame: DataFrame con información detallada
        """
        detailed_data = []
        
        for record in self.data:
            numero_aceptacion = record['numero_aceptacion']
            productos_con_cantidad = record['productos_con_cantidad']
            marcas = record['marcas']
            referencias = record['referencias']
            modelos = record['modelos']
            es_cadena = record['es_cadena']
            pasos = record['pasos']
            
            # Crear una fila por producto con cantidad
            if productos_con_cantidad:
                for i, item in enumerate(productos_con_cantidad):
                    row = {
                        'numero_aceptacion': numero_aceptacion,
                        'registro_numero': record['registro_numero'],
                        'cantidad': item['cantidad'],
                        'unidad': item['unidad'],
                        'producto': item['producto'],
                        'marca': marcas[i] if i < len(marcas) else marcas[0] if marcas else 'NO ESPECIFICADA',
                        'referencia': referencias[i] if i < len(referencias) else referencias[0] if referencias else 'NO ESPECIFICADA',
                        'modelo': modelos[i] if i < len(modelos) else modelos[0] if modelos else 'NO ESPECIFICADO',
                        'es_cadena': 'SÍ' if es_cadena else 'NO',
                        'paso_medida': ', '.join(pasos) if pasos else 'N/A',
                        'total_productos_registro': len(productos_con_cantidad),
                        'cantidad_total_registro': record['cantidad_total'],
                        'fecha_procesamiento': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    detailed_data.append(row)
            else:
                # Si no hay productos específicos, crear una fila con información general
                row = {
                    'numero_aceptacion': numero_aceptacion,
                    'registro_numero': record['registro_numero'],
                    'cantidad': 0,
                    'unidad': 'N/A',
                    'producto': 'INFORMACIÓN NO ESPECÍFICA',
                    'marca': marcas[0] if marcas else 'NO ESPECIFICADA',
                    'referencia': referencias[0] if referencias else 'NO ESPECIFICADA',
                    'modelo': modelos[0] if modelos else 'NO ESPECIFICADO',
                    'es_cadena': 'SÍ' if es_cadena else 'NO',
                    'paso_medida': ', '.join(pasos) if pasos else 'N/A',
                    'total_productos_registro': 0,
                    'cantidad_total_registro': 0,
                    'fecha_procesamiento': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                detailed_data.append(row)
        
        return pd.DataFrame(detailed_data)
    
    def create_summary_dataframe(self) -> pd.DataFrame:
        """
        Crea un DataFrame resumen con un registro por número de aceptación.
        
        Returns:
            pd.DataFrame: DataFrame resumen
        """
        summary_data = []
        
        for record in self.data:
            row = {
                'numero_aceptacion': record['numero_aceptacion'],
                'total_productos': record['total_productos'],
                'cantidad_total': record['cantidad_total'],
                'total_marcas': record['total_marcas'],
                'total_referencias': record['total_referencias'],
                'marcas_principales': ', '.join(record['marcas'][:3]) if record['marcas'] else 'NO ESPECIFICADA',
                'contiene_cadenas': 'SÍ' if record['es_cadena'] else 'NO',
                'pasos_identificados': ', '.join(record['pasos']) if record['pasos'] else 'N/A',
                'productos_muestra': ', '.join([item['producto'] for item in record['productos_con_cantidad'][:2]]) if record['productos_con_cantidad'] else 'NO ESPECIFICADOS'
            }
            summary_data.append(row)
        
        return pd.DataFrame(summary_data)
    
    def generate_statistics(self) -> Dict:
        """
        Genera estadísticas del análisis.
        
        Returns:
            Dict: Diccionario con estadísticas
        """
        df_detailed = self.create_detailed_dataframe()
        
        stats = {
            'total_registros': len(self.data),
            'total_productos': df_detailed.shape[0],
            'total_unidades': df_detailed['cantidad'].sum(),
            'productos_con_cadenas': len(df_detailed[df_detailed['es_cadena'] == 'SÍ']),
            'marcas_unicas': df_detailed['marca'].nunique(),
            'lista_marcas': sorted(df_detailed['marca'].unique().tolist()),
            'registros_con_cadenas': sum(1 for record in self.data if record['es_cadena']),
            'promedio_productos_por_registro': np.mean([record['total_productos'] for record in self.data]),
            'promedio_unidades_por_producto': df_detailed['cantidad'].mean(),
            'registro_con_mas_productos': max(self.data, key=lambda x: x['total_productos'])['numero_aceptacion'],
            'registro_con_mas_unidades': max(self.data, key=lambda x: x['cantidad_total'])['numero_aceptacion'],
            'max_productos_por_registro': max(record['total_productos'] for record in self.data),
            'max_unidades_por_registro': max(record['cantidad_total'] for record in self.data)
        }
        
        return stats
    
    def export_to_csv(self, output_path: str = "importaciones_detallado.csv") -> None:
        """
        Exporta los datos detallados a CSV.
        
        Args:
            output_path (str): Ruta del archivo de salida
        """
        df = self.create_detailed_dataframe()
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        self.logger.info(f"Datos detallados exportados a: {output_path}")
    
    def export_to_excel(self, output_path: str = "importaciones_completo.xlsx") -> None:
        """
        Exporta los datos a Excel con múltiples hojas.
        
        Args:
            output_path (str): Ruta del archivo de salida
        """
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Hoja detallada
            df_detailed = self.create_detailed_dataframe()
            df_detailed.to_excel(writer, sheet_name='Detallado', index=False)
            
            # Hoja resumen
            df_summary = self.create_summary_dataframe()
            df_summary.to_excel(writer, sheet_name='Resumen', index=False)
            
            # Hoja de estadísticas
            stats = self.generate_statistics()
            df_stats = pd.DataFrame(list(stats.items()), columns=['Métrica', 'Valor'])
            df_stats.to_excel(writer, sheet_name='Estadísticas', index=False)
        
        self.logger.info(f"Datos exportados a Excel: {output_path}")
    
    def export_json_report(self, output_path: str = "importaciones_reporte.json") -> None:
        """
        Exporta un reporte completo en formato JSON.
        
        Args:
            output_path (str): Ruta del archivo de salida
        """
        report = {
            'metadata': {
                'fecha_procesamiento': datetime.now().isoformat(),
                'total_registros': len(self.data),
                'version': '1.0'
            },
            'estadisticas': self.generate_statistics(),
            'registros': self.data
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"Reporte JSON exportado a: {output_path}")
    
    def print_summary_report(self) -> None:
        """Imprime un reporte resumen en consola."""
        stats = self.generate_statistics()
        
        print("\n" + "="*60)
        print("REPORTE DE ANÁLISIS DE IMPORTACIONES")
        print("="*60)
        print(f"Total de registros procesados: {stats['total_registros']}")
        print(f"Total de productos identificados: {stats['total_productos']}")
        print(f"Productos con cadenas: {stats['productos_con_cadenas']}")
        print(f"Marcas únicas: {stats['marcas_unicas']}")
        print(f"Promedio de productos por registro: {stats['promedio_productos_por_registro']:.1f}")
        print(f"Registro con más productos: {stats['registro_con_mas_productos']} ({stats['max_productos_por_registro']} productos)")
        
        print(f"\nMarcas identificadas:")
        for marca in stats['lista_marcas']:
            print(f"  • {marca}")
        
        print(f"\nRegistros con cadenas: {stats['registros_con_cadenas']}/{stats['total_registros']}")
        print("="*60)


def main():
    """Función principal del programa."""
    parser = argparse.ArgumentParser(description='Analizador de Registros de Importación')
    parser.add_argument('file_path', help='Ruta al archivo de texto con los registros')
    parser.add_argument('--output-csv', default='importaciones_detallado.csv', 
                       help='Nombre del archivo CSV de salida')
    parser.add_argument('--output-excel', default='importaciones_completo.xlsx', 
                       help='Nombre del archivo Excel de salida')
    parser.add_argument('--output-json', default='importaciones_reporte.json', 
                       help='Nombre del archivo JSON de salida')
    parser.add_argument('--log-level', default='INFO', 
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Nivel de logging')
    parser.add_argument('--no-excel', action='store_true', 
                       help='No generar archivo Excel')
    parser.add_argument('--no-json', action='store_true', 
                       help='No generar archivo JSON')
    
    args = parser.parse_args()
    
    # Verificar que el archivo existe
    if not Path(args.file_path).exists():
        print(f"Error: El archivo {args.file_path} no existe.")
        return 1
    
    try:
        # Crear analizador
        analyzer = ImportacionAnalyzer(log_level=args.log_level)
        
        # Procesar archivo
        analyzer.process_file(args.file_path)
        
        # Generar reportes
        analyzer.export_to_csv(args.output_csv)
        
        if not args.no_excel:
            analyzer.export_to_excel(args.output_excel)
        
        if not args.no_json:
            analyzer.export_json_report(args.output_json)
        
        # Mostrar reporte en consola
        analyzer.print_summary_report()
        
        print(f"\n✅ Procesamiento completado exitosamente!")
        print(f"📄 CSV generado: {args.output_csv}")
        if not args.no_excel:
            print(f"📊 Excel generado: {args.output_excel}")
        if not args.no_json:
            print(f"📋 JSON generado: {args.output_json}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error durante el procesamiento: {str(e)}")
        return 1


if __name__ == "__main__":
    exit(main())


# Ejemplo de uso como módulo
"""
# Uso básico:
analyzer = ImportacionAnalyzer()
analyzer.process_file('paste.txt')
analyzer.export_to_csv('resultados.csv')
analyzer.export_to_excel('resultados.xlsx')
analyzer.print_summary_report()

# Uso avanzado con configuración:
analyzer = ImportacionAnalyzer(log_level="DEBUG")
data = analyzer.process_file('mi_archivo.txt')
stats = analyzer.generate_statistics()
df_detailed = analyzer.create_detailed_dataframe()

# Filtrar solo productos con cadenas:
df_cadenas = df_detailed[df_detailed['es_cadena'] == 'SÍ']
print(f"Productos con cadenas: {len(df_cadenas)}")
"""