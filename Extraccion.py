# 1. Configuración del entorno
# Instalación de bibliotecas necesarias (ejecutar solo si no están instaladas)
!pip install xarray
!pip install netcdf4 h5netcdf
!pip install --upgrade xarray netcdf4 h5netcdf

# Importación de bibliotecas
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import xarray as xr
import netCDF4 as nc
import numpy as np
import h5netcdf

# Verificación de versiones
print("xarray versión:", xr.__version__)
print("netCDF4 versión:", nc.__version__)
print("h5netcdf versión:", h5netcdf.__version__)

# 2. Descarga de archivos NetCDF
# URL de la página que contiene los archivos NetCDF
base_url = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p05/"

# Carpeta donde se descargarán los archivos
download_folder = "CHIRPS_NetCDF_Files"
combined_folder = "CHIRPS_Combined"

# Crear las carpetas de descarga y destino si no existen
os.makedirs(download_folder, exist_ok=True)
os.makedirs(combined_folder, exist_ok=True)

# Encabezados para imitar un navegador
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# Obtener y analizar la página web
try:
    response = requests.get(base_url, headers=headers)
    response.raise_for_status()
except requests.exceptions.RequestException as e:
    print(f"Error al acceder a la página: {e}")
    exit(1)

# Parsear el contenido HTML con BeautifulSoup
soup = BeautifulSoup(response.text, 'html.parser')

# Encontrar todos los enlaces que terminan con .nc o .nc.gz
file_links = soup.find_all('a', href=True)
netcdf_files = [urljoin(base_url, link['href']) for link in file_links if link['href'].endswith(('.nc', '.nc.gz'))]

print(f"Se encontraron {len(netcdf_files)} archivos NetCDF.")

# Descargar los archivos
for file_url in netcdf_files:
    file_name = os.path.basename(file_url)
    local_path = os.path.join(download_folder, file_name)

    if not os.path.exists(local_path):
        try:
            print(f"Descargando {file_name}...")
            with requests.get(file_url, headers=headers, stream=True) as r:
                r.raise_for_status()
                with open(local_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            print(f"{file_name} descargado correctamente.")
        except requests.exceptions.RequestException as e:
            print(f"Error al descargar {file_name}: {e}")
    else:
        print(f"{file_name} ya existe. Se omite la descarga.")

print("Proceso de descarga completado.")

# 3. Procesamiento y combinación de archivos NetCDF
# Parámetros de extracción
lat_min = -16.0  # 16°S
lat_max = -13.0  # 13°S
lon_min = -71.0  # 71°W
lon_max = -69.0  # 69°W

# Inicializar listas para almacenar datos combinados
combined_times = []
combined_precip = []

# Variables para almacenar las coordenadas una vez
latitudes = None
longitudes = None

# Lista de archivos NetCDF en la carpeta de descarga
netcdf_files = sorted([f for f in os.listdir(download_folder) if f.endswith('.nc')])

print(f"Procesando {len(netcdf_files)} archivos NetCDF...")

for idx, file_name in enumerate(netcdf_files):
    file_path = os.path.join(download_folder, file_name)
    print(f"Abriendo archivo {idx+1}/{len(netcdf_files)}: {file_name}")

    # Abrir el archivo NetCDF
    ds = nc.Dataset(file_path, 'r')

    # Leer latitudes y longitudes una sola vez
    if latitudes is None or longitudes is None:
        latitudes = ds.variables['latitude'][:]
        longitudes = ds.variables['longitude'][:]

        # Encontrar los índices que cumplen con los criterios
        lat_indices = np.where((latitudes >= lat_min) & (latitudes <= lat_max))[0]
        lon_indices = np.where((longitudes >= lon_min) & (longitudes <= lon_max))[0]

        # Extraer las coordenadas correspondientes
        subset_latitudes = latitudes[lat_indices]
        subset_longitudes = longitudes[lon_indices]

        print(f"Latitudes seleccionadas: {subset_latitudes[0]} a {subset_latitudes[-1]}")
        print(f"Longitudes seleccionadas: {subset_longitudes[0]} a {subset_longitudes[-1]}")

    # Leer el tiempo
    time = ds.variables['time'][:]
    combined_times.append(time)

    # Leer la precipitación y seleccionar el subset
    precip = ds.variables['precip'][:, lat_indices, lon_indices]
    combined_precip.append(precip)

    # Cerrar el dataset actual
    ds.close()

# Concatenar los datos a lo largo de la dimensión temporal
print("Concatenando datos a lo largo del tiempo...")
combined_times = np.concatenate(combined_times)
combined_precip = np.concatenate(combined_precip, axis=0)

print(f"Datos combinados: {combined_precip.shape} (time, lat, lon)")

# Crear el nuevo archivo NetCDF combinado y recortado
combined_file_name = "CHIRPS_Combined_Subset.nc"
combined_file_path = os.path.join(combined_folder, combined_file_name)

print(f"Creando archivo combinado: {combined_file_name}")

# Abrir un nuevo dataset para escribir
with nc.Dataset(combined_file_path, 'w', format='NETCDF4') as ds_out:
    # Definir las dimensiones
    ds_out.createDimension('time', None)
    ds_out.createDimension('latitude', subset_latitudes.size)
    ds_out.createDimension('longitude', subset_longitudes.size)

    # Crear las variables
    time_var = ds_out.createVariable('time', 'f4', ('time',))
    lat_var = ds_out.createVariable('latitude', 'f4', ('latitude',))
    lon_var = ds_out.createVariable('longitude', 'f4', ('longitude',))
    precip_var = ds_out.createVariable('precip', 'f4', ('time', 'latitude', 'longitude'),
                                       fill_value=-9999.0)

    # Asignar atributos a las variables
    time_var.units = 'days since 1980-1-1 0:0:0'
    time_var.standard_name = 'time'
    time_var.calendar = 'gregorian'
    time_var.axis = 'T'

    lat_var.units = 'degrees_north'
    lat_var.standard_name = 'latitude'
    lat_var.long_name = 'latitude'
    lat_var.axis = 'Y'

    lon_var.units = 'degrees_east'
    lon_var.standard_name = 'longitude'
    lon_var.long_name = 'longitude'
    lon_var.axis = 'X'

    precip_var.units = 'mm/day'
    precip_var.standard_name = 'convective_precipitation_rate'
    precip_var.long_name = 'Climate Hazards group InfraRed Precipitation with Stations'
    precip_var.time_step = 'day'
    precip_var.missing_value = -9999.0

    precip_var.geostatial_lat_min = lat_min
    precip_var.geostatial_lat_max = lat_max
    precip_var.geostatial_lon_min = lon_min
    precip_var.geostatial_lon_max = lon_max

    # Asignar las coordenadas
    lat_var[:] = subset_latitudes
    lon_var[:] = subset_longitudes
    time_var[:] = combined_times

    # Asignar los datos de precipitación
    precip_var[:] = combined_precip

print(f"Archivo combinado y recortado guardado en: {combined_file_path}")
print("Proceso completado.")
