import time
import pandas as pd
from datetime import datetime

def metadatos_plantas(db):
    query_plantas = f"""	select iwl.id as plantId ,ms.id as meteoId,latitude,longitude
                                from(
                                    select id,latitude,longitude
                                    from  openiot.plant p
                                    where latitude != 1 and longitude !=1
                                    )iwl left join openiot.meteo ms on iwl.id = ms.id_planta
                                    where iwl.id is not null and ms.id is null
                                union 
                                select iwl.id as plantId,ms.id as meteoId,latitude,longitude
                                from(
                                    select id,latitude,longitude
                                    from openiot.plant p
                                    where latitude != 1 and longitude !=1
                                    )iwl left join openiot.meteo ms on iwl.id = ms.id_planta
                                    where iwl.id is not null"""
    result1 = db.fetch_data(query_plantas)
    plantas = list(result1["plantid"].values)
    latitudes = list(result1["latitude"].values)
    longitudes = list(result1["longitude"].values)
    meteoId = list(result1["meteoid"].values)
    coordenadas = [(lat, lon) for lat, lon in zip(latitudes, longitudes)]
    position_data = {readi_id: zona for readi_id, zona, meteoId in zip(plantas, coordenadas, meteoId)}


    return position_data


def metadatos_meteo(db):
    query_meteo = f"""SELECT id, id_planta FROM openiot.meteo WHERE label = 'Open Meteo';"""
    result3 = db.fetch_data(query_meteo)
    ids_met = list(result3["id"].values)
    ids_planta = list(result3["id_planta"].values)
    dicc_meteo = {id_planta: id_met for id_planta, id_met in zip(ids_planta, ids_met)}


    return dicc_meteo


def primer_registro(db):
    query_time = f"""SELECT ms.id_planta , min(msm.datetime) 
                     FROM openiot.meteo_measurements msm
                     join openiot.meteo ms on ms.id = msm.id
                     where ms.label in ('Open Meteo')
                     GROUP BY ms.id"""
    result2 = db.fetch_data(query_time)
    plantas = list(result2["id_planta"].values)
    minimos = list(result2["min"])
    minimos = [item.strftime("%Y-%m-%d") for item in minimos]
    first_data = {id_planta: minimo for id_planta, minimo in zip(plantas, minimos)}


    return first_data