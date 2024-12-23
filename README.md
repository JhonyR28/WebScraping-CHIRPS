Este proyecto automatiza la descarga, procesamiento y combinación de datos meteorológicos en formato NetCDF provenientes del conjunto de datos CHIRPS (Climate Hazards group InfraRed Precipitation with Stations). El código permite filtrar los datos por coordenadas geográficas específicas, combinarlos a lo largo de una dimensión temporal y generar un archivo NetCDF optimizado para análisis climáticos regionales.

## Funcionalidades
1. Descarga automática de archivos NetCDF desde el servidor de CHIRPS.
2. Filtrado de datos de precipitación en función de límites geográficos definidos por el usuario.
3. Combinación de múltiples archivos NetCDF en un solo archivo optimizado.
4. Generación de un nuevo archivo NetCDF que incluye metadatos, atributos y datos filtrados.
