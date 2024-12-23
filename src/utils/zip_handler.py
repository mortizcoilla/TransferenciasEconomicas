"""
Manejador para la extracción y archivado de archivos ZIP.
Este módulo se encarga de extraer los archivos ZIP originales a una carpeta temporal
'unzipped' y archivar los ZIP originales. Los archivos extraídos serán posteriormente
organizados por el FileOrganizer.
"""

import os
import logging
import zipfile
from typing import List, Optional
import shutil
from datetime import datetime
import subprocess

logger = logging.getLogger(__name__)

# Ruta al ejecutable de 7-Zip
SEVEN_ZIP_PATH = r"C:\Program Files\7-Zip\7z.exe"


class ZipHandler:
    """
    Maneja la extracción de archivos ZIP y su archivado.

    Esta clase se encarga de:
    1. Extraer los ZIP originales a una carpeta temporal 'unzipped'
    2. Archivar los ZIP originales en 'archive'
    3. Extraer cualquier ZIP anidado dentro de los archivos extraídos
    """

    def __init__(self, raw_path: str, unzipped_path: str, archive_path: str):
        """
        Inicializa el manejador de ZIPs.

        Args:
            raw_path: Ruta donde se encuentran los ZIP originales
            unzipped_path: Ruta donde se extraerán temporalmente todos los archivos
            archive_path: Ruta donde se archivarán los ZIP originales
        """
        self.raw_path = raw_path
        self.unzipped_path = unzipped_path  # Cambiado de processed_path a unzipped_path
        self.archive_path = archive_path

    def _is_sscc_balance_file(self, filename: str) -> bool:
        """
        Verifica si un archivo corresponde al patrón de Balance SSCC.
        """
        import re

        pattern = r"^Balance_SSCC_\d{4}_(ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)_def\.zip$"
        return bool(re.match(pattern, filename))

    def _get_sscc_extract_folder(self, zip_name: str) -> str:
        """
        Obtiene el nombre de la carpeta de destino para un archivo Balance SSCC.
        """
        return os.path.splitext(zip_name)[0]

    def normalize_path(self, path: str) -> str:
        """
        Normaliza una ruta del sistema de archivos para manejar mejor los espacios y caracteres especiales.

        Args:
            path: Ruta a normalizar

        Returns:
            str: Ruta normalizada y absoluta
        """
        return os.path.abspath(path)

    def extract_with_7zip(self, zip_path: str, extract_path: str) -> bool:
        """
        Intenta extraer un archivo usando 7-Zip cuando zipfile falla.

        Args:
            zip_path: Ruta al archivo ZIP a extraer
            extract_path: Ruta donde extraer los archivos

        Returns:
            bool: True si la extracción fue exitosa
        """
        try:
            # Aseguramos que las rutas sean absolutas
            zip_path = self.normalize_path(zip_path)
            extract_path = self.normalize_path(extract_path)

            # Construimos el comando con las rutas entre comillas para manejar espacios
            cmd = f'"{SEVEN_ZIP_PATH}" x -y "-o{extract_path}" "{zip_path}"'

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                shell=True,  # Necesario para manejar rutas con espacios
            )

            if result.returncode == 0:
                logger.info(f"Archivo extraído con 7z: {zip_path}")
                return True
            else:
                logger.error(f"Error en 7z: {result.stderr}")
                return False

        except Exception as e:
            logger.error(f"Error usando 7z en {zip_path}: {str(e)}")
            return False

    def extract_zip(
        self, periodo: str, zip_name: str, specific_files: Optional[List[str]] = None
    ) -> bool:
        """
        Extrae un archivo ZIP al directorio unzipped.

        Args:
            periodo: Periodo que se está procesando (YYYYMM)
            zip_name: Nombre del archivo ZIP a extraer
            specific_files: Lista opcional de archivos específicos a extraer

        Returns:
            bool: True si la extracción fue exitosa
        """
        zip_path = os.path.join(self.raw_path, periodo, zip_name)
        extract_base_path = os.path.join(self.unzipped_path, periodo)

        try:
            # Crear directorio base
            os.makedirs(extract_base_path, exist_ok=True)

            # Determinar la ruta de extracción basada en el tipo de archivo
            if self._is_sscc_balance_file(zip_name):
                folder_name = self._get_sscc_extract_folder(zip_name)
                extract_path = os.path.join(extract_base_path, folder_name)
                logger.info(
                    f"Creando directorio específico para Balance SSCC: {folder_name}"
                )
                os.makedirs(extract_path, exist_ok=True)
            else:
                # Para otros archivos, extraer al directorio base del periodo
                extract_path = extract_base_path

            # Normalizar rutas para manejar espacios y caracteres especiales
            zip_path = self.normalize_path(zip_path)
            extract_path = self.normalize_path(extract_path)

            # Primer intento: usar zipfile
            try:
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    # Listar contenido para logging
                    file_list = zip_ref.namelist()
                    logger.debug(
                        f"Contenido del ZIP {zip_name}: {len(file_list)} archivos"
                    )

                    if specific_files:
                        # Extraer solo archivos específicos
                        for file in specific_files:
                            if file in file_list:
                                zip_ref.extract(file, extract_path)
                                logger.debug(f"Extraído archivo específico: {file}")
                    else:
                        # Extraer todos los archivos
                        zip_ref.extractall(extract_path)

                    logger.info(f"Archivo ZIP extraído exitosamente: {zip_name}")
                    return True

            except (zipfile.BadZipFile, NotImplementedError) as e:
                logger.warning(
                    f"No se pudo extraer {zip_name} con zipfile, intentando con 7z: {str(e)}"
                )
                # Segundo intento: usar 7-Zip como respaldo
                return self.extract_with_7zip(zip_path, extract_path)

        except Exception as e:
            logger.error(f"Error extrayendo {zip_path}: {str(e)}")
            return False

    def extract_all_nested_zips(self, base_path: str) -> bool:
        """
        Extrae todos los archivos ZIP encontrados dentro del directorio base.

        Args:
            base_path: Ruta base donde buscar ZIPs anidados

        Returns:
            bool: True si todas las extracciones fueron exitosas
        """
        try:
            overall_success = True
            processed_files = set()  # Evita procesar el mismo archivo múltiples veces

            for root, _, files in os.walk(base_path):
                for file in files:
                    if not file.lower().endswith(".zip"):
                        continue

                    zip_path = os.path.join(root, file)
                    if zip_path in processed_files:
                        continue

                    extract_path = os.path.dirname(os.path.abspath(zip_path))

                    try:
                        # Primer intento con zipfile
                        try:
                            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                                zip_ref.extractall(extract_path)
                            logger.info(f"ZIP anidado extraído: {zip_path}")
                            processed_files.add(zip_path)
                        except (zipfile.BadZipFile, NotImplementedError):
                            # Segundo intento con 7z
                            if self.extract_with_7zip(zip_path, extract_path):
                                processed_files.add(zip_path)
                            else:
                                overall_success = False

                    except Exception as e:
                        logger.error(f"Error procesando ZIP {zip_path}: {str(e)}")
                        overall_success = False

            return overall_success

        except Exception as e:
            logger.error(f"Error en extracción anidada: {str(e)}")
            return False

    def archive_zip(self, periodo: str, zip_name: str) -> bool:
        """
        Mueve un archivo ZIP procesado al directorio de archivo.

        Args:
            periodo: Periodo del archivo (YYYYMM)
            zip_name: Nombre del archivo ZIP a archivar

        Returns:
            bool: True si el archivo fue movido exitosamente
        """
        source_path = os.path.join(self.raw_path, periodo, zip_name)
        archive_dir = os.path.join(self.archive_path, periodo)

        try:
            os.makedirs(archive_dir, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name, ext = os.path.splitext(zip_name)
            archive_name = f"{base_name}_{timestamp}{ext}"
            archive_path = os.path.join(archive_dir, archive_name)

            shutil.move(source_path, archive_path)
            logger.info(f"ZIP archivado: {archive_path}")
            return True

        except Exception as e:
            logger.error(f"Error archivando {source_path}: {str(e)}")
            return False

    def list_zip_files(self, periodo: str) -> List[str]:
        """
        Lista todos los archivos ZIP en el directorio del periodo.

        Args:
            periodo: Periodo a listar (YYYYMM)

        Returns:
            List[str]: Lista de nombres de archivos ZIP
        """
        periodo_path = os.path.join(self.raw_path, periodo)
        if not os.path.exists(periodo_path):
            logger.error(f"No se encontró el directorio: {periodo_path}")
            return []
        return [f for f in os.listdir(periodo_path) if f.endswith(".zip")]

    def process_period_zips(
        self, periodo: str, specific_files: Optional[List[str]] = None
    ) -> bool:
        """
        Procesa todos los archivos ZIP de un periodo.

        Este método:
        1. Lista todos los ZIP del periodo
        2. Extrae cada ZIP al directorio unzipped
        3. Archiva los ZIP originales

        Args:
            periodo: Periodo a procesar (YYYYMM)
            specific_files: Lista opcional de archivos específicos a extraer

        Returns:
            bool: True si el proceso general fue exitoso
        """
        zip_files = self.list_zip_files(periodo)
        if not zip_files:
            logger.warning(f"No hay archivos ZIP para periodo {periodo}")
            return False

        overall_success = True

        for zip_file in zip_files:
            success = self.extract_zip(periodo, zip_file, specific_files)
            if success:
                if not self.archive_zip(periodo, zip_file):
                    overall_success = False
            else:
                overall_success = False

        if not overall_success:
            logger.warning("Algunos archivos no se pudieron procesar correctamente")

        return True  # Continuamos el proceso incluso si hay algunos errores
