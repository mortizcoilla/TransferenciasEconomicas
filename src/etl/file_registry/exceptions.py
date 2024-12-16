"""
Módulo de excepciones personalizadas para el sistema de registro de archivos.
"""

class FileRegistryError(Exception):
    """Excepción base para errores del registro de archivos."""
    pass

class FileDefinitionError(FileRegistryError):
    """Error en la definición de un archivo."""
    pass

class FileLocationError(FileRegistryError):
    """Error en la ubicación de un archivo."""
    pass

class FileValidationError(FileRegistryError):
    """Error en la validación de un archivo."""
    pass

class FileNotFoundError(FileRegistryError):
    """Archivo no encontrado en las ubicaciones especificadas."""
    pass

class ConfigurationError(FileRegistryError):
    """Error en la configuración del registro."""
    pass