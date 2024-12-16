# Transferencias Economicas - Extractor ETL

## Descripcion

Proyecto de automatizacion para el procesamiento mensual de datos de Transferencias Economicas del Coordinador Electrico Nacional de Chile. El sistema extrae, transforma y carga (ETL) datos desde multiples fuentes hacia una base de datos MariaDB.

## Estructura del Proyecto

    TransferenciasEconomicas/
    ├── config/          # Archivos de configuración 
    ├── data/            # Datos crudos
    ├── logs/            # Registros de ejecución
    ├── scripts/         # Scripts auxiliares  
    ├── src/             # Código fuente
    │   ├── database/    # Módulos de conexión a BD
    │   ├── etl/         # Módulos de extracción y transformación
    │   └── utils/       # Utilidades comunes
    ├── tests/           # Pruebas unitarias
    └── main.py          # Punto de entrada principal

## Origen de los Datos

Los datos provienen del Coordinador Electrico Nacional y se organizan mensualmente en la siguiente estructura:

* Balance Valorizado (15 minutos)
* Contratos entre Generadoras
* Sobrecostos
* Precio Estabilizado
* Costos Variables
* Datos de Transmisión
* Servicios Complementarios (SSCC)
* Fuente: [Coordinador Eléctrico Nacional](https://www.coordinador.cl/mercados/documentos/transferencias-economicas/antecedentes-de-calculo-para-las-transferencias-economicas/)

## Objetivo

Automatizar el proceso mensual de extracción y procesamiento de datos de transferencias económicas, garantizando:

* Procesamiento eficiente de múltiples fuentes de datos
* Validación de integridad y consistencia
* Carga estructurada en base de datos
* Trazabilidad del proceso
* Reducción de errores manuales

## Instalación

1. Clonar el repositorio
    * git clone [url_repositorio]

2. Crear y activar entorno virtual
    * python -m venv venv
    * venv\Scripts\activate

3. Instalar dependencias
    * pip install -r requirements.txt

4. Configurar variables de entorno
    * cp .env.example .env

## Uso

1. Configurar rutas y credenciales en config/config.yml
2. Ejecutar el procesamiento: python main.py

## Estructura del Proceso

1. Etapa de Descarga de Datos
   * Descarga de archivos desde el Coordinador Eléctrico Nacional

2. Etapa de Balance y Validación
   * Importación del Balance Valorizado Bruto
   * Validaciones de estructura y datos
   * Carga de Generación y Retiro

3. Etapa de Factores y CMG
   * Procesamiento de RetiroReguladorFactor
   * Cálculo de factores técnicos
   * Manejo de datos CMG

4. Etapa de Sobrecostos
   * Extracción y procesamiento de sobrecostos
   * Validaciones específicas
   * Carga en base de datos

5. Etapa de Vertimiento y Transmisión
   * Procesamiento de datos de vertimiento
   * Manejo de información de transmisión

6. Etapa de SSCC y Precios
   * Extracción de datos SSCC
   * Procesamiento de precios estabilizados

7. Etapa de Costos Variables y Generación
   * Procesamiento de centrales marginales
   * Cálculo de costos variables
   * Análisis de generación

8. Etapa Final de Contratos y Factores
   * Procesamiento de contratos de generadoras
   * Cálculos finales
   * Proceso de desacople
