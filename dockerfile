# Usar una imagen base oficial de Python
FROM python:3.12-slim

# Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiar el archivo requirements.txt dentro del contenedor
COPY requirements.txt /app/requirements.txt

# Instalar las dependencias especificadas en requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copiar el código de la aplicación al contenedor
COPY . /app

# Crear un directorio para guardar archivos temporales
RUN mkdir -p /mnt/output

# Establecer el comando CMD para ejecutar el script principal
CMD ["python", "/app/haka.py"]