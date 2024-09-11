import pandas as pd 
import numpy as np
import os
import random
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from sqlalchemy import create_engine
from datetime import datetime

def Obtener_Datos(batch_size=100000):
    db_user = os.environ.get('DB_USER')
    db_password = os.environ.get('DB_PASSWORD')
    db_host = os.environ.get('DB_HOST')
    db_port = os.environ.get('DB_PORT', '5432')  # Valor por defecto 5432
    db_name = os.environ.get('DB_NAME')

    # Crear una conexión usando SQLAlchemy
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    query = 'SELECT * FROM haka_entry WHERE EXTRACT(month FROM "eventDate") = 6 AND EXTRACT(week FROM "eventDate") != 22'

    # Procesar los datos en lotes
    dfs = []
    for chunk in pd.read_sql_query(query, engine, chunksize=batch_size):
        dfs.append(chunk)

    df = pd.concat(dfs, ignore_index=True)
    df['eventDate'] = df['eventDate'].dt.tz_convert('America/La_Paz')
    return df

def filtrados(df):
    # Crear la máscara booleana
    mask = df['camera'] == 'Mz40-Piso 30'
    # Aplicar la máscara para filtrar el DataFrame
    df_filtrado = df
    df_filtrado['eventDate']=pd.to_datetime(df['eventDate'])
    # Agregar columnas day_of_week y hour
    df_filtrado['week'] = df['eventDate'].dt.isocalendar().week
    df_filtrado['day_of_week'] = df['eventDate'].dt.day_name()
    df_filtrado['hour'] = df['eventDate'].dt.hour
    df_filtrado['min']=df['eventDate'].dt.minute
    df_filtrado['seg']=df['eventDate'].dt.second
    return df_filtrado

def Estadisticas(df_filtrado):
    # Agrupar por week, day_of_week, hour y zone, y contar las apariciones
    # Filtrar los datos para excluir la primera semana de junio
    df_filtrado1 = df_filtrado[(df_filtrado['week'] != 22)]
    conteo_semanal = df_filtrado1.groupby(['week', 'day_of_week', 'hour','min', 'zone','camera']).size().reset_index(name='count')
    conteo_semanal

    #conteo_semanal=conteo_semanal[(conteo_semanal['camera'] == 'Mz40-Piso 30')&(conteo_semanal['zone'] == 'Coches 4to anillo hacia Roca y coronado')&(conteo_semanal['hour'] == 15)&(conteo_semanal['min'] == 15)]
    # Calcular la desviación estándar de los conteos por day_of_week, hour y zone
    std_devs = conteo_semanal.groupby(['day_of_week', 'hour','min', 'zone','camera']).agg(mean=('count', 'mean'), std=('count', 'std')).reset_index()
    std_devs

    # Agrupar por day_of_week, hour y zone y contar las apariciones en todo el mes
    conteo_mensual = df_filtrado.groupby(['day_of_week', 'hour','min', 'zone','camera']).size().reset_index(name='count')
    conteo_mensual
    # Combinar los conteos y las desviaciones estándar
    result = pd.merge(conteo_mensual, std_devs, on=['day_of_week', 'hour','min', 'zone','camera'])
    return result


def extraer_filas_unicas(df, columnas):
    # Verificamos que las columnas especificadas existen en el DataFrame
    for columna in columnas:
        if columna not in df.columns:
           raise ValueError(f"La columna '{columna}' no existe en el DataFrame")
    # Extraemos las filas únicas basadas en las columnas especificadas
    df_unicas = df.drop_duplicates(subset=columnas)
    return df_unicas


def devolver_base_random(df_stats, fecha_str, hora_inicio, hora_fin, df_unicas):
    # Convertir la cadena de fecha a un objeto datetime
    fecha_datetime = pd.to_datetime(fecha_str, format='%d,%m,%Y')

    dias_semana = {
        0: 'Monday',
        1: 'Tuesday',
        2: 'Wednesday',
        3: 'Thursday',
        4: 'Friday',
        5: 'Saturday',
        6: 'Sunday'
    }
    
    # Obtener el día de la semana como número (0=Monday, ..., 6=Sunday)
    dia_semana_num = fecha_datetime.weekday()
    
    # Convertir el número del día de la semana al nombre en inglés
    dia_semana = dias_semana[dia_semana_num]
    
    # Filtrar el DataFrame para el día de la semana correspondiente
    stats_dia = df_stats[df_stats['day_of_week'] == dia_semana]
    
    if stats_dia.empty:
        raise ValueError(f"No statistics found for day: {dia_semana}")
    
    # Ajustar la división de la hora para manejar HH:MM:SS
    h_inicio, m_inicio, _ = map(int, hora_inicio.split(':'))
    h_fin, m_fin, _ = map(int, hora_fin.split(':'))

    # Filtrar para el rango de horas y minutos especificado
    stats_dia = stats_dia[(stats_dia['hour'] > h_inicio) | 
                          ((stats_dia['hour'] == h_inicio) & (stats_dia['min'] > m_inicio))]
    stats_dia = stats_dia[(stats_dia['hour'] < h_fin) | 
                          ((stats_dia['hour'] == h_fin) & (stats_dia['min'] < m_fin))]

    #print(f"Number of rows in stats_dia: {len(stats_dia)}")
    
    resultados = []
    
    # Recorrer cada hora y zona para generar valores aleatorios
    for _, row in stats_dia.iterrows():
        media = row['mean']
        std_dev = row['std']
        zona = row['zone']
        camera = row['camera']
        hour = row['hour']
        minute = row['min']
                    
        # Manejar el caso donde la desviación estándar es NaN
        if pd.isna(std_dev):
            std_dev = 0
               
        # Generar un valor aleatorio siguiendo una distribución normal
        valor_aleatorio = np.random.normal(media, std_dev)
        #print(f"Generated value: {valor_aleatorio}")
        # Redondear el valor a enteros (ya que estamos tratando con conteos)
        valor_aleatorio = round(valor_aleatorio)
        seg = random.randint(0, 59)
        # Crear el valor de fecha y tiempo en el formato deseado
        fecha_evento = f"{fecha_datetime.strftime('%Y-%m-%d')} {hour:02}:{minute:02}:{seg:02}"

        # Buscar las filas equivalentes en el segundo DataFrame
        matching_rows = df_unicas[(df_unicas['zone'] == row['zone']) & 
                                  (df_unicas['camera'] == row['camera'])]
        if matching_rows.shape[0] != 1:
            raise ValueError(f"Expected one matching row, found {matching_rows.shape[0]}")
        match_row = matching_rows.iloc[0]
        #print(f"Number of matching rows: {len(matching_rows)}")                       
        for _ in range(valor_aleatorio):
            resultados.append({
                'eventType': match_row['eventType'],
                'zone': zona,
                'camera': camera,
                'objectClass': match_row['objectClass'],
                'eventDate': fecha_evento,
                'impact': match_row['impact']
            })    
    # Crear un DataFrame con los resultados
    df_resultado = pd.DataFrame(resultados)
    return df_resultado
    

def guardar_como_csv(df, ruta,fecha_str):
    fecha = datetime.strptime(fecha_str, '%d,%m,%Y')
    date = fecha.strftime('%d-%m-%Y')
    file_name= f"output{date}.csv"
    file_path = os.path.join(ruta,file_name)
    df.to_csv(file_path, index=False)
    return file_path

def enviar_email_con_smtp(to_email, file_path):
    smtp_server = os.environ['SMTP_SERVER']
    smtp_port = int(os.environ['SMTP_PORT'])
    smtp_user = os.environ['SMTP_USER']
    smtp_password = os.environ['SMTP_PASSWORD']  # Esta es la contraseña de aplicación

    from_email = smtp_user

    # Crear el mensaje de correo electrónico
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = "Tu archivo CSV generado por Lambda"

    # Adjuntar el archivo CSV
    with open(file_path, 'rb') as file:
        attachment = MIMEApplication(file.read(), _subtype="csv")
        attachment.add_header('Content-Disposition', 'attachment', filename="output.csv")
        msg.attach(attachment)

    msg.attach(MIMEText("Adjunto encontrarás el archivo CSV generado."))

    # Conectar al servidor SMTP y enviar el correo
    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(from_email, to_email, msg.as_string())
        server.quit()
        print(f"Correo enviado a {to_email}")
    except Exception as e:
        print(f"Error al enviar correo: {str(e)}")


# Lambda handler
def main():
    fecha_str = os.environ.get('FECHA', '26,07,2024')
    hora_inicio = os.environ.get('HORA_INICIO', '00:00:00')
    hora_fin = os.environ.get('HORA_FIN', '06:52:00')
    to_email = os.environ.get('TO_EMAIL', 'destinatario@example.com')

    print("Variables de entorno cargadas correctamente")  

    ruta_csv = '/mnt/output'  # Usar siempre /tmp en Lambda

    df = Obtener_Datos()
    df_filtrado = filtrados(df)
    df_stats = Estadisticas(df_filtrado)
    df_unicas = extraer_filas_unicas(df_filtrado, ['eventType', 'zone', 'camera', 'objectClass', 'impact'])
    
    base = devolver_base_random(df_stats, fecha_str, hora_inicio, hora_fin, df_unicas)
    
    # Guardar como CSV
    csv_path = guardar_como_csv(base, ruta_csv,fecha_str)
    print(f"Archivo CSV guardado en: {csv_path}")
    # Enviar el archivo CSV por correo electrónico
    enviar_email_con_smtp(to_email, csv_path)
    print("Correo enviado correctamente") 
    return {
        'statusCode': 200,
        'body': f"CSV generated and sent to {to_email}"
    }

if __name__ == "__main__":
    main()