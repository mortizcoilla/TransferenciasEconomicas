import bcchapi
import yaml


def cargar_configuracion(ruta_config):
    """
    Carga la configuración desde un archivo YAML.

    Args:
        ruta_config (str): Ruta del archivo de configuración.

    Returns:
        dict: Configuración cargada.
    """
    with open(ruta_config, "r", encoding="utf-8") as archivo:
        return yaml.safe_load(archivo)


def obtener_valores_dolar(config_path, desde, hasta):
    """
    Obtiene los valores del dólar desde la API del Banco Central de Chile.

    Args:
        config_path (str): Ruta al archivo de configuración.
        desde (str): Fecha de inicio en formato 'YYYY-MM-DD'.
        hasta (str): Fecha de fin en formato 'YYYY-MM-DD'.

    Returns:
        pd.DataFrame: DataFrame con los valores del dólar en el rango de fechas especificado.
    """
    try:
        # Cargar configuración
        config = cargar_configuracion(config_path)
        usuario = config["api"]["usuario"]
        contraseña = config["api"]["contraseña"]

        # Inicializar la conexión con la API
        siete = bcchapi.Siete(usuario, contraseña)

        # Selección directa de la serie de dólar observado
        serie_dolar = "F073.TCO.PRE.Z.D"  # Dólar Observado

        # Consultar los valores del dólar en el rango especificado
        cuadro = siete.cuadro(
            series=[serie_dolar],
            nombres=["dolar"],
            desde=desde,
            hasta=hasta,
        )

        return cuadro

    except Exception as e:
        print(f"Error al obtener los valores del dólar: {e}")
        return None


# Uso del método
if __name__ == "__main__":
    config_path = "config/config.yml"  # Ruta al archivo de configuración
    desde = "2023-01-01"
    hasta = "2023-12-31"

    valores_dolar = obtener_valores_dolar(config_path, desde, hasta)
    if valores_dolar is not None:
        print(valores_dolar)
