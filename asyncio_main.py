import requests
import psycopg2
import pandas as pd
import os
import psycopg2.extras
from files_writer import *
from files_creator import *
import threading

from datetime import datetime
import time


import sys

def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn

import psycopg2.extras
def execute_values(conn, df, table):
    """
    Using psycopg2.extras.execute_values() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES %%s ON CONFLICT (event_id) DO NOTHING" % (table, cols)
    cursor = conn.cursor()
    try:
        psycopg2.extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()

def create_table(conn, table, query):
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
        print("create_table() done")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()

    cursor.close()

def list_split(listA, n):
    for x in range(0, len(listA), n):
        every_chunk = listA[x: n+x]

        if len(every_chunk) < n:
            every_chunk = every_chunk + \
                [None for y in range(n-len(every_chunk))]
        yield every_chunk

def writer(dates):
    for date in dates:
        start_time = datetime.now()

        print(date)
        url = f'https://api.sofascore.com/api/v1/sport/{sport_name}/scheduled-events/{date}'

        response = requests.get(url)
        sheduled_events = response.json()

        past_events_create_creator(date)
        player_incidents_cards_creator(date)
        player_incidents_substitutions_creator(date)
        player_incidents_goals_creator(date)
        player_incidents_inGamePenaltyies_creator(date)
        player_incidents_penaltyShootouts_creator(date)

        event_ids = []
        for event in range(len(sheduled_events['events'])):
            if sheduled_events['events'][event]['status']['type'] == 'finished':
                try:
                    sport = sheduled_events['events'][event]['tournament']['uniqueTournament']['category']['sport']['name']

                    country = sheduled_events['events'][event]['tournament']['uniqueTournament']['category']['name']
                    tournament = sheduled_events['events'][event]['tournament']['uniqueTournament']['name']

                    home_team_name = sheduled_events['events'][event]['homeTeam']['name']
                    away_team_name = sheduled_events['events'][event]['awayTeam']['name']

                    try:
                        round_info = sheduled_events['events'][event]['roundInfo']['name']
                    except:
                        round_info = 'None'

                    event_id = sheduled_events['events'][event]['id']
                    event_ids.append(event_id)

                    _type = sheduled_events['events'][event]['status']['description']

                    home_score = sheduled_events['events'][event]['homeScore']['current']
                    away_score = sheduled_events['events'][event]['awayScore']['current']

                    try:
                        event_json = requests.get(f'https://api.sofascore.com/api/v1/event/{event_id}').json()
                        try:
                            season_name = event_json['season']['name']
                        except:
                            season_name = 'None'

                        try:
                            city_name = event_json['event']['venue']['city']['name']
                        except:
                            city_name = 'None'
                        try:
                            stadium_name = event_json['event']['venue']['stadium']['name']
                        except:
                            stadium_name = 'None'
                        try:
                            referee_name = event_json['event']['venue']['referee']['name']
                        except:
                            referee_name = 'None'

                        past_events_create_writer(
                            date, sport, country, tournament,
                            home_team_name, away_team_name,
                            round_info, event_id, _type,
                            home_score, away_score, season_name,
                            city_name,stadium_name, referee_name)
                    except:
                        continue
                except:
                    continue

        for event_id in event_ids:
            try:
                incident_event = requests.get(f'https://api.sofascore.com/api/v1/event/{event_id}/incidents').json()
                for incident_number in range(len(incident_event['incidents'])):
                    incident_type = incident_event['incidents'][incident_number]['incidentType']

                    if incident_type == 'card':
                        try:
                            player_name = incident_event['incidents'][incident_number]['playerName']
                            reason = incident_event['incidents'][incident_number]['reason']
                            time = incident_event['incidents'][incident_number]['time']
                            is_home = incident_event['incidents'][incident_number]['isHome']
                            incident_class = incident_event['incidents'][incident_number]['incidentClass']
                            incident_type = incident_type

                            player_incidents_cards_writer(date, event_id,
                                                          player_name, reason, time, is_home,
                                                          incident_class, incident_type)
                        except:
                            continue

                    if incident_type == 'substitution':
                        try:
                            player_in_name = incident_event['incidents'][incident_number]['playerIn']['name']
                            player_out_name = incident_event['incidents'][incident_number]['playerOut']['name']
                            time = incident_event['incidents'][incident_number]['time']
                            is_home = incident_event['incidents'][incident_number]['isHome']
                            incident_type = incident_type

                            player_incidents_substitutions_writer(date, event_id,
                                                                  player_in_name, player_out_name, time,
                                                                  is_home, incident_type)
                        except:
                            continue

                    if incident_type == 'goal':
                        try:
                            home_score = incident_event['incidents'][incident_number]['homeScore']
                            away_score = incident_event['incidents'][incident_number]['awayScore']

                            player_name = incident_event['incidents'][incident_number]['player']['name']

                            try:
                                assist1_name = incident_event['incidents'][incident_number]['assist1']['name']
                            except:
                                assist1_name = 'None'

                            time = incident_event['incidents'][incident_number]['time']
                            is_home = incident_event['incidents'][incident_number]['isHome']
                            incident_type = incident_type

                            player_incidents_goals_writer(date, event_id,
                                                          home_score, away_score, player_name,
                                                          assist1_name, time, is_home, incident_type)

                        except:
                            continue

                    if incident_type == 'inGamePenalty':
                        try:
                            name = incident_event['incidents'][incident_number]['player']['name']
                            description = incident_event['incidents'][incident_number]['description']

                            time = incident_event['incidents'][incident_number]['time']
                            is_home = incident_event['incidents'][incident_number]['isHome']

                            incident_class = incident_event['incidents'][incident_number]['incidentClass']
                            incident_type = incident_type

                            player_incidents_inGamePenaltyies_writer(date, event_id,
                                                                     name, description, time, is_home,
                                                                     incident_class, incident_type)

                        except:
                            continue

                    if incident_type == 'penaltyShootout':
                        try:
                            name = incident_event['incidents'][incident_number]['player']['name']

                            home_score = incident_event['incidents'][incident_number]['homeScore']

                            away_score = incident_event['incidents'][incident_number]['awayScore']

                            sequence = incident_event['incidents'][incident_number]['sequence']

                            description = incident_event['incidents'][incident_number]['description']

                            is_home = incident_event['incidents'][incident_number]['isHome']

                            incident_class = incident_event['incidents'][incident_number]['incidentClass']

                            incident_type = incident_type

                            player_incidents_penaltyShootouts_(date, event_id,
                                                               name, home_score, away_score,
                                                               sequence, description, is_home,
                                                               incident_class, incident_type)
                        except:
                            continue
            except:
                continue

        print(f'[INFO] Информация за дату {date} успешно спарсена!...')

        df_past_events = pd.read_csv(f"past-events_{date}.csv", sep = ',')
        player_incidents_cards = pd.read_csv(f"player_incidents_cards_{date}.csv", sep = ',')
        player_incidents_substitutions = pd.read_csv(f"player_incidents_substitutions_{date}.csv", sep = ',')
        player_incidents_goals = pd.read_csv(f"player_incidents_goals_{date}.csv", sep = ',')
        player_incidents_inGamePenaltyies = pd.read_csv(f"player_incidents_inGamePenaltyies_{date}.csv", sep = ',')
        player_incidents_penaltyShootouts = pd.read_csv(f"player_incidents_penaltyShootouts_{date}.csv", sep = ',')

        param_dic = {
            "host"      : "127.0.0.1",
            "database"  : "sofascore_db",
            "user"      : "postgres",
            "password"  : "puma2015"
        }

        conn = connect(param_dic)

        execute_values(conn, df_past_events, 'past_events')
        execute_values(conn, player_incidents_cards, 'player_incidents_cards')
        execute_values(conn, player_incidents_substitutions, 'player_incidents_substitutions')
        execute_values(conn,player_incidents_goals,'player_incidents_goals')
        execute_values(conn, player_incidents_penaltyShootouts, 'player_incidents_penaltyShootouts')
        execute_values(conn, player_incidents_inGamePenaltyies, 'player_incidents_inGamePenaltyies')

        print(f'[INFO] Информация за дату {date} успешно сохранена в БД!...')

        files = os.listdir()
        for i in files:
            if i.endswith(f'{date}.csv'):
                os.remove(i)
                print('удалено успешно')

sport_name = 'football'


# init threads

start_date = datetime(2001, 1, 1)
end_date = datetime(2022, 9, 3)

res = pd.date_range(
    min(start_date, end_date),
    max(start_date, end_date)
).strftime('%Y-%m-%d').tolist()

import numpy as np
dates_split_array = np.array_split(res,4)
print(len(dates_split_array))

for date_split in dates_split_array:
    t = threading.Thread(target=writer, args= (date_split,))

    t.start()