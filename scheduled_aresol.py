import requests
import pytz
import schedule
import os
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from smtp import smtp_send
from datetime import date, timedelta
from SQLConnections import *

from ddbb_utils import *
DEBUG = os.getenv('DEBUG', 'False') in ('True', 'true', 't', 'yes', '1')
PAST_DAYS = os.getenv('PAST_DAYS', 15)
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


DDBB_INFO = {
    "user": os.getenv('POSTGRES_USER', ""),
    "password": os.getenv('POSTGRES_PASSWORD', ""),
    "host": os.getenv('POSTGRES_HOST', ""),
    "port": os.getenv('POSTGRES_PORT', ""),
    "database": os.getenv('POSTGRES_DB', "")
}

s = requests.Session()
retries = Retry(
    total=3,
    backoff_factor=0.1,
    status_forcelist=[408, 400],
    allowed_methods={'GET'},
)
s.mount('https://', HTTPAdapter(max_retries=retries))
def insert_meteo_data(plant, position_data, dicc_meteo, historic: bool,past: int, db, t):

    position = position_data.get(plant)
    lat, lon = position

    id_meteo = db.execute_query(f"select id from openiot.meteo where id_planta='{plant}' and label in('Open Meteo')")['id'][0]

    logger.info(f'Datos de la planta {plant},{lat=}, {lon=},{id_meteo=} ')

    data_url = ''
    if historic:
        data_url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}1&start_date=2022-01-01&end_date={str(date.today() - timedelta(days=1))}&hourly=temperature_2m,windspeed_10m,winddirection_10m,shortwave_radiation,direct_radiation,diffuse_radiation,direct_normal_irradiance"
        #data_url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}1&start_date=2022-01-01&end_date=2023-08-13&hourly=temperature_2m,windspeed_10m,winddirection_10m,shortwave_radiation,direct_radiation,diffuse_radiation,direct_normal_irradiance"
    else:
        data_url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m,windspeed_10m,winddirection_10m,shortwave_radiation,direct_radiation,diffuse_radiation,direct_normal_irradiance&past_days={past}"

    logging.info(f"Requesting data from API")
    #req = requests.get(data_url)
    #logging.info(f"req: {req}")
    #res = req.json()
    time.sleep(5)
    req = s.get(data_url)
    res = req.json()

    timestamps_str = res["hourly"]["time"]
    timezone = res["timezone"]

    shortwave = res["hourly"]["shortwave_radiation"]
    direct = res["hourly"]["direct_radiation"]
    dni = res["hourly"]["direct_normal_irradiance"]
    temperature = res["hourly"]["temperature_2m"]
    winddirection = res["hourly"]["winddirection_10m"]
    windspeed = res["hourly"]['windspeed_10m']

    tz = pytz.timezone(timezone)
    timestamps = [datetime.strptime(time, "%Y-%m-%dT%H:%M") for time in timestamps_str]
    timestamps_loc = [tz.localize(time) for time in timestamps]

    columns = ["datetime", "radiant_line", "horiz_radiant_total", "horiz_radiant_line",
                                                "temperature", "wind_direction", "wind_speed"
]
    zipped = list(zip(timestamps_loc, dni, shortwave, direct, temperature, winddirection, windspeed))


    registry_df = pd.DataFrame(data=zipped,
                               columns=columns)

    registry_df.insert(0, "id", id_meteo)
    registry_df["radiant_total"] = None
    registry_df["pv_temperature"] = None

    inicio_borrado = timestamps_loc[0]
    fin_borrado = tz.localize(datetime.strptime(t, "%Y-%m-%d"))

    daily_df = registry_df.loc[registry_df["datetime"] < fin_borrado]

    del_query_reg = f"""DELETE
                        FROM openiot.meteo_measurements
                        WHERE datetime >= '{inicio_borrado}' 
                        AND   datetime < '{fin_borrado}' 
                        AND   id = '{id_meteo}';"""

    logger.info(f"Se borrará desde {inicio_borrado} hasta {fin_borrado} del id_meteo {id_meteo}")

    res = db.upsert_df_to_database(del_query_reg, daily_df, 'openiot', 'meteo_measurements')
    return res

def get_section(db, plant:int)->int:
    query_id_section = f"""select s.id 
                          from openiot."section" s
                          where s.id_planta = '{plant}'"""
    data = db.execute_query(query_id_section)
    if data.size !=0:
        return data["id"][0]
    else:
        return 'Not Found'

def main_function():
    error_inserts = []

    db = DataBaseManager(f"postgresql+psycopg2://{DDBB_INFO['user']}:{DDBB_INFO['password']}@{DDBB_INFO['host']}:{DDBB_INFO['port']}/{DDBB_INFO['database']}")

    t = time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(time.time()))
    t = t.split('T')[0]
    logging.info(f"Database manager succesfully connected: {db}")
    # Buscamos datos de las plantas que no pertenecen a estación meteorológicas y son de openmeteo
    # Search for plants which don't have a meteo-station, and get their data from openmeteo

    position_data = metadatos_plantas(db)

    # Buscamos los primeros registros de las plantas que ya están añadidas, con esto sabemos que plantas están
    # registradas

    # Search for the first regs in the measurements table, so we can know which plants dont need historical insert and
    # only update

    first_data = primer_registro(db)

    # Get the plants that only have Open Meteo

    dicc_meteo = metadatos_meteo(db)

    # Analyze the plants, if there is already any data, update from last days, else then add the plant meteo sensor
    # to the database add a first measurement and then get historical data from it (2022).

    for plant in position_data.keys():

        if plant in first_data.keys():
            logging.info(f"Planta: {plant} dentro de registros de meteo anteriores, actualizando los últimos {PAST_DAYS} días")
            res = insert_meteo_data(plant, position_data, dicc_meteo, False, PAST_DAYS, db, t)
            logging.info(res)
            if res == 'Bad insert':
                error_inserts.append(plant)
            # Días pasados de los que necesito los datos, se meterán 15 días para en caso de pérdida de datos, exista recovery

        else:
            logging.info(f"Planta: {plant} no está dentro de registros de meteo anteriores,creando meteo sensor y primer registro")
            section = get_section(db, plant)
            if section == 'Not Found':
                logging.info(
                    f"Planta: {plant} existe pero no tiene asociada ninguna sección,por tanto no se creará un meteo_sensor")
                continue
            else:
                insert_new_meteo_sensor = f"""insert into openiot.meteo (id_section,id_api,model,software_version,name_api,label,esn,fabricante, id_planta)
                                              select {section},' ',' ',' ',' ','Open Meteo',' ',' ','{plant}'
                                              where not exists(
                                              select id_section,id_api,model,software_version,name_api,label,esn,fabricante, id_planta
                                              from openiot.meteo
                                              where id_planta='{plant}' and label in('Open Meteo'))"""
                db.execute_query(insert_new_meteo_sensor)

                meteo_id = db.execute_query(f"select id from openiot.meteo where id_planta='{plant}' and label in('Open Meteo')")
                if meteo_id.empty is True:#Send an alarm if a meteo sensor couldn't be added to the database
                    error_inserts.append(plant)
                else:
                    add_first_measurement = f"""insert into openiot.meteo_measurements (id,datetime,radiant_line,horiz_radiant_total,horiz_radiant_line,temperature,wind_direction,wind_speed,radiant_total,pv_temperature)
                                               values({meteo_id['id'][0]},'2022-01-01 01:00:00.000 +0100',0,0,0,0,0,0,0,0)"""
                    db.execute_query(add_first_measurement)

                    logger.info(f"La planta {plant} se ha añadido en meteo_sensors_measurements, insertando datos desde 2022")

                    #Insert historic data with True
                    res = insert_meteo_data(plant, position_data, dicc_meteo, True, 3, db, t)
                    if res == 'Bad insert':
                        error_inserts.append(plant)

    if error_inserts != []:
        bad_plants = ';'.join(error_inserts)
        smtp_send(f'Meteo sensor measurements for plants: {bad_plants} could not be added to database reason: bad insert',
                  'Meteo problem', ['josedaniel.sosa@bosonit.com', 'alvaro.magallanes@elliotcloud.com'])

    logger.info("Tarea finalizada")

if __name__ == "__main__":
    if DEBUG:
        main_function()
    else:
        # Update data every day at 4 am, add plants if available
        schedule.every().day.at("00:15").do(main_function)

    while True:
        schedule.run_pending()
        time.sleep(5)


