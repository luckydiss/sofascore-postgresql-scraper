import csv

def past_events_create_writer(
                date, sport, country,tournament,
                home_team_name, away_team_name,
                round_info, event_id, _type,
                home_score, away_score, season_name,
                city_name,stadium_name,referee_name):

    with open(f"past-events_{date}.csv", "a", encoding='utf-8', newline='') as file:
        writer = csv.writer(file, delimiter=',')
        writer.writerow(
            (
                date,
                sport,
                country,
                tournament,
                home_team_name,
                away_team_name,
                round_info,
                event_id,
                _type,
                home_score,
                away_score,
                season_name,
                city_name,
                stadium_name,
                referee_name
            )
        )


def player_incidents_cards_writer(date, event_id,
                player_name, reason, time,is_home,
                incident_class, incident_type):
    with open(f"player_incidents_cards_{date}.csv", "a", encoding='utf-8', newline='') as file:
        writer = csv.writer(file, delimiter=',')
        writer.writerow(
            (
                event_id,
                player_name,
                reason,
                time,
                is_home,
                incident_class,
                incident_type
            )
        )


def player_incidents_substitutions_writer(date,event_id,
                player_in_name, player_out_name,time,
                is_home, incident_type):

    with open(f"player_incidents_substitutions_{date}.csv", "a", encoding='utf-8', newline='') as file:
        writer = csv.writer(file, delimiter=',')
        writer.writerow(
            (
                event_id,
                player_in_name,
                player_out_name,
                time,
                is_home,
                incident_type
            )
        )

def player_incidents_goals_writer(date, event_id,
                home_score,away_score, player_name,
                assist1_name, time, is_home, incident_type):
    with open(f"player_incidents_goals_{date}.csv", "a", encoding='utf-8', newline='') as file:
        writer = csv.writer(file, delimiter=',')
        writer.writerow(
            (
                event_id,
                home_score,
                away_score,
                player_name,
                assist1_name,
                time,
                is_home,
                incident_type

            )
        )

def player_incidents_inGamePenaltyies_writer(date,event_id,
                name, description, time, is_home,
                incident_class, incident_type):
    with open(f"player_incidents_inGamePenaltyies_{date}.csv", "a", encoding='utf-8', newline='') as file:
        writer = csv.writer(file, delimiter=',')
        writer.writerow(
            (
                event_id,
                name,
                description,
                time,
                is_home,  # id
                incident_class,
                incident_type

            )
        )

def player_incidents_penaltyShootouts_(date,event_id,
                name, home_score, away_score,
                sequence, description, is_home,
                incident_class,incident_type):
    with open(f"player_incidents_penaltyShootouts_{date}.csv", "a", encoding='utf-8', newline='') as file:
        writer = csv.writer(file, delimiter=',')
        writer.writerow(
            (
                event_id,
                name,
                home_score,
                away_score,
                sequence,
                description,
                is_home,
                incident_class,
                incident_type

            )
        )




