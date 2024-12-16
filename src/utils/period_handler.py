"""
Módulo para manejar la selección y validación de periodos.
"""

import os
import re
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

class PeriodHandler:
    """Clase para manejar la selección y validación de periodos."""

    @staticmethod
    def validate_period_format(period: str) -> bool:
        """
        Valida que el periodo tenga el formato correcto YYYYMM.

        Args:
            period (str): Periodo a validar

        Returns:
            bool: True si el formato es válido, False en caso contrario
        """
        if not re.match(r'^\d{6}$', period):
            return False
        
        try:
            year = int(period[:4])
            month = int(period[4:])
            
            # Validar año y mes
            current_year = datetime.now().year
            if not (2000 <= year <= current_year + 1):  # Permite hasta un año futuro
                return False
            if not (1 <= month <= 12):
                return False
                
            return True
        except ValueError:
            return False

    @staticmethod
    def list_available_periods(raw_path: str) -> list:
        """
        Lista todos los periodos disponibles en el directorio raw.

        Args:
            raw_path (str): Ruta al directorio raw

        Returns:
            list: Lista de periodos disponibles ordenados descendentemente
        """
        try:
            # Obtener directorios que coincidan con el patrón YYYYMM
            periods = [d for d in os.listdir(raw_path) 
                      if os.path.isdir(os.path.join(raw_path, d)) and 
                      re.match(r'^\d{6}$', d)]
            
            # Ordenar periodos de más reciente a más antiguo
            return sorted(periods, reverse=True)
        except Exception as e:
            logger.error(f"Error listando periodos disponibles: {str(e)}")
            return []

    @classmethod
    def get_period_input(cls, raw_path: str) -> Optional[str]:
        """
        Solicita y valida el input del periodo a procesar.

        Args:
            raw_path (str): Ruta al directorio raw

        Returns:
            Optional[str]: Periodo seleccionado o None si hubo error
        """
        available_periods = cls.list_available_periods(raw_path)
        
        if not available_periods:
            logger.error("No se encontraron periodos disponibles en el directorio raw")
            return None

        print("\nPeriodos disponibles:")
        for i, period in enumerate(available_periods, 1):
            year = period[:4]
            month = period[4:]
            print(f"{i}. {year}/{month}")

        while True:
            try:
                option = input("\nSeleccione una opción (número) o ingrese el periodo (YYYYMM): ").strip()
                
                # Si ingresó un número
                if option.isdigit() and 1 <= int(option) <= len(available_periods):
                    selected_period = available_periods[int(option) - 1]
                    logger.info(f"Periodo seleccionado: {selected_period}")
                    return selected_period
                
                # Si ingresó un periodo en formato YYYYMM
                elif cls.validate_period_format(option):
                    if option in available_periods:
                        logger.info(f"Periodo seleccionado: {option}")
                        return option
                    else:
                        print(f"Error: El periodo {option} no está disponible en el directorio raw")
                else:
                    print("Error: Entrada inválida. Ingrese un número de opción o un periodo en formato YYYYMM")
            
            except ValueError:
                print("Error: Por favor ingrese un valor válido")
            except Exception as e:
                logger.error(f"Error en la selección del periodo: {str(e)}")
                return None