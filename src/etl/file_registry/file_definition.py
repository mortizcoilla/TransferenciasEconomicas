"""
Módulo para la definición de archivos y sus características básicas.
Este módulo solo define estructuras de datos, sin realizar validaciones.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class FileValidation:
    """
    Estructura de datos que almacena las reglas de validación.
    Estas reglas serán utilizadas posteriormente por el validador.
    """

    required_columns: List[str] = field(default_factory=list)
    data_types: Dict[str, str] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)
    required_sheets: List[str] = field(default_factory=list)


@dataclass
class SheetDefinition:
    """
    Estructura de datos que define una hoja de cálculo.
    """

    name: str
    required: bool = True


@dataclass
class FileDefinition:
    """
    Estructura de datos que define las características de un archivo.
    No realiza validaciones, solo almacena información.
    """

    file_type: str
    required: bool
    format: str
    locations: List[Dict]
    validation: FileValidation = field(default_factory=FileValidation)
    requires_sheets: bool = False
    encoding: str = "utf-8"
    sheets: List[SheetDefinition] = field(default_factory=list)

    def __post_init__(self):
        """
        Normaliza el formato a minúsculas después de la inicialización.
        """
        self.format = self.format.lower()
