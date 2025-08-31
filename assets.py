from io import StringIO
import pandas as pd
import requests
from dagster import job, op
from datetime import datetime

# -------------------- Paso 2: Lectura de datos y chequeos --------------------
@op
def leer_datos() -> pd.DataFrame:
    # Descargamos el CSV oficial de Our World in Data
    # y lo leemos directamente en un DataFrame sin modificarlo
    url = "https://catalog.ourworldindata.org/garden/covid/latest/compact/compact.csv"
    resp = requests.get(url, timeout=10)
    if resp.status_code != 200:
        # Si hay un error en la descarga, devolvemos un DataFrame vacío
        print(f"No se pudo descargar el CSV, status_code={resp.status_code}")
        return pd.DataFrame()
    # Convertimos el contenido descargado a DataFrame
    df = pd.read_csv(StringIO(resp.text))
    print(f"CSV descargado correctamente, filas={len(df)}")
    return df

@op
def chequear_datos(df: pd.DataFrame) -> pd.DataFrame:
    # Convertimos la columna de fechas a datetime
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    hoy = pd.Timestamp(datetime.today().date())

    # Verificamos que no haya fechas futuras
    fechas_invalidas = df[df["date"] > hoy]
    if len(fechas_invalidas) > 0:
        print(f"Alerta: {len(fechas_invalidas)} filas tienen fecha mayor a hoy")
    else:
        print("Todas las fechas son válidas")

    # Revisamos que las columnas importantes no tengan valores nulos
    columnas_clave = ["country", "date", "population"]
    nulos = df[columnas_clave].isna().sum().sum()
    if nulos > 0:
        print(f"Alerta: se encontraron {nulos} valores nulos en {columnas_clave}")
    else:
        print("Todas las columnas clave contienen datos")

    # Comprobamos que no existan filas duplicadas por país y fecha
    duplicados = df.duplicated(subset=["country", "date"]).sum()
    if duplicados > 0:
        print(f"Alerta: se encontraron {duplicados} filas duplicadas por (country, date)")
    else:
        print("No hay duplicados por país y fecha")

    return df

# -------------------- Paso 3: Procesamiento de Datos --------------------
@op
def datos_procesados(df: pd.DataFrame) -> pd.DataFrame:
    # Eliminamos filas con valores nulos en columnas importantes
    # y duplicados, luego filtramos solo los países que nos interesan
    df_proc = df.dropna(subset=["new_cases", "people_vaccinated"]).drop_duplicates()
    df_proc = df_proc[df_proc["country"].isin(["Ecuador", "Argentina"])].copy()
    columnas = ["country", "date", "new_cases", "people_vaccinated", "population"]
    print(f"Datos procesados, filas después de limpieza={len(df_proc)}")
    return df_proc[columnas]

# -------------------- Paso 4: Cálculo de Métricas --------------------
@op
def metrica_incidencia_7d(df: pd.DataFrame) -> pd.DataFrame:
    # Calculamos la incidencia diaria por cada 100k habitantes
    # y luego hacemos una media móvil de 7 días
    df["date"] = pd.to_datetime(df["date"])
    df["incidencia_diaria"] = (df["new_cases"] / df["population"]) * 100000
    df["incidencia_7d"] = df.groupby("country")["incidencia_diaria"].transform(
        lambda x: x.rolling(7, min_periods=1).mean()
    )
    # Retornamos solo las columnas necesarias renombrando para claridad
    return df[["date", "country", "incidencia_7d"]].rename(
        columns={"date": "fecha", "country": "país"}
    )

@op
def metrica_factor_crec_7d(df: pd.DataFrame) -> pd.DataFrame:
    # Calculamos la suma de casos por semana
    # Luego obtenemos la semana anterior para calcular el factor de crecimiento
    df["date"] = pd.to_datetime(df["date"])
    df["casos_semana_actual"] = df.groupby("country")["new_cases"].transform(
        lambda x: x.rolling(7, min_periods=7).sum()
    )
    df["casos_semana_prev"] = df.groupby("country")["casos_semana_actual"].shift(7)
    df["factor_crec_7d"] = df["casos_semana_actual"] / df["casos_semana_prev"]
    return df[["date", "country", "casos_semana_actual", "factor_crec_7d"]].rename(
        columns={"date": "semana_fin", "country": "país", "casos_semana_actual": "casos_semana"}
    )

# -------------------- Paso 5: Chequeos de Salida --------------------
@op
def chequeo_incidencia(incidencia: pd.DataFrame):
    # Verificamos que los valores de incidencia estén dentro de un rango razonable
    min_val = incidencia["incidencia_7d"].min()
    max_val = incidencia["incidencia_7d"].max()
    if 0 <= min_val <= 2000 and 0 <= max_val <= 2000:
        print(f"Incidencia 7d dentro del rango esperado: min={min_val}, max={max_val}")
    else:
        print(f"Incidencia fuera del rango esperado: min={min_val}, max={max_val}")

# -------------------- Paso 6: Exportación de Resultados --------------------
@op
def reporte_excel_covid(df_proc: pd.DataFrame, incidencia: pd.DataFrame, factor_crec: pd.DataFrame) -> str:
    # Creamos un archivo Excel con varias hojas: datos procesados, incidencia y factor de crecimiento
    output_file = "reporte_covid.xlsx"
    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
        df_proc.to_excel(writer, sheet_name="datos_procesados", index=False)
        incidencia.to_excel(writer, sheet_name="incidencia_7d", index=False)
        factor_crec.to_excel(writer, sheet_name="factor_crec_7d", index=False)
    print(f"Archivo Excel generado: {output_file}")
    return output_file

# -------------------- Job completo --------------------
@job
def pipeline_covid():
    # Leemos los datos crudos desde la fuente oficial
    df_raw = leer_datos()
    # Hacemos chequeos iniciales para asegurar que los datos están correctos
    df_checkeado = chequear_datos(df_raw)
    # Limpiamos y filtramos los datos según nuestros criterios
    df_proc = datos_procesados(df_checkeado)
    # Calculamos métricas clave: incidencia y factor de crecimiento
    incidencia = metrica_incidencia_7d(df_proc)
    factor_crec = metrica_factor_crec_7d(df_proc)
    # Revisamos que la incidencia esté dentro de rangos normales
    chequeo_incidencia(incidencia)
    # Exportamos todo a un archivo Excel listo para análisis
    reporte_excel_covid(df_proc, incidencia, factor_crec)