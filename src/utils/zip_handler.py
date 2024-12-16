"""
Módulo para manejar la extracción y procesamiento de archivos ZIP.
"""

import os
import logging
import zipfile
from typing import List, Optional
import shutil
from datetime import datetime

logger = logging.getLogger(__name__)

class ZipHandler:
    """Clase para manejar operaciones con archivos ZIP"""
    
    def __init__(self, raw_path: str, processed_path: str, archive_path: str):
        """
        Inicializa el manejador de archivos ZIP.

        Args:
            raw_path (str): Ruta donde se encuentran los archivos ZIP originales
            processed_path (str): Ruta donde se extraerán los archivos
            archive_path (str): Ruta donde se archivarán los ZIP procesados
        """
        self.raw_path = raw_path
        self.processed_path = processed_path
        self.archive_path = archive_path

    def list_zip_files(self, periodo: str) -> List[str]:
        """
        Lista todos los archivos ZIP de un periodo específico.

        Args:
            periodo (str): Periodo a procesar (ej: '202409')

        Returns:
            List[str]: Lista de rutas de archivos ZIP encontrados
        """
        periodo_path = os.path.join(self.raw_path, periodo)
        if not os.path.exists(periodo_path):
            logger.error(f"No se encontró el directorio del periodo: {periodo_path}")
            return []

        return [f for f in os.listdir(periodo_path) if f.endswith('.zip')]

    def extract_zip(self, periodo: str, zip_name: str, specific_files: Optional[List[str]] = None) -> bool:
        """
        Extrae archivos de un ZIP específico.

        Args:
            periodo (str): Periodo del archivo
            zip_name (str): Nombre del archivo ZIP
            specific_files (List[str], optional): Lista de archivos específicos a extraer

        Returns:
            bool: True si la extracción fue exitosa, False en caso contrario
        """
        zip_path = os.path.join(self.raw_path, periodo, zip_name)
        extract_path = os.path.join(self.processed_path, periodo)
        
        try:
            # Crear directorio si no existe
            os.makedirs(extract_path, exist_ok=True)

            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Si se especifican archivos, extraer solo esos
                if specific_files:
                    for file in specific_files:
                        if file in zip_ref.namelist():
                            zip_ref.extract(file, extract_path)
                else:
                    zip_ref.extractall(extract_path)
            
            logger.info(f"Archivo ZIP extraído exitosamente: {zip_name}")
            return True
            
        except zipfile.BadZipFile:
            logger.error(f"Archivo ZIP corrupto o inválido: {zip_path}")
            return False
        except Exception as e:
            logger.error(f"Error extrayendo archivo ZIP {zip_path}: {str(e)}")
            return False

    def archive_zip(self, periodo: str, zip_name: str) -> bool:
        """
        Mueve un archivo ZIP procesado al archivo.

        Args:
            periodo (str): Periodo del archivo
            zip_name (str): Nombre del archivo ZIP

        Returns:
            bool: True si el archivo fue movido exitosamente
        """
        source_path = os.path.join(self.raw_path, periodo, zip_name)
        archive_dir = os.path.join(self.archive_path, periodo)
        archive_path = os.path.join(archive_dir, zip_name)

        try:
            # Crear directorio de archivo si no existe
            os.makedirs(archive_dir, exist_ok=True)
            
            # Agregar timestamp al nombre del archivo
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name, ext = os.path.splitext(zip_name)
            archive_name = f"{base_name}_{timestamp}{ext}"
            archive_path = os.path.join(archive_dir, archive_name)
            
            # Mover el archivo
            shutil.move(source_path, archive_path)
            logger.info(f"Archivo ZIP archivado exitosamente: {archive_path}")
            return True

        except Exception as e:
            logger.error(f"Error archivando ZIP {source_path}: {str(e)}")
            return False

    def process_period_zips(self, periodo: str, specific_files: Optional[List[str]] = None) -> bool:
        """
        Procesa todos los archivos ZIP de un periodo.

        Args:
            periodo (str): Periodo a procesar
            specific_files (List[str], optional): Lista de archivos específicos a extraer

        Returns:
            bool: True si todos los archivos fueron procesados exitosamente
        """
        zip_files = self.list_zip_files(periodo)
        if not zip_files:
            logger.warning(f"No se encontraron archivos ZIP para el periodo {periodo}")
            return False

        success = True
        for zip_file in zip_files:
            if self.extract_zip(periodo, zip_file, specific_files):
                if not self.archive_zip(periodo, zip_file):
                    success = False
            else:
                success = False

        return success