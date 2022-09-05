import csv

def past_events_create_creator(date):
    with open(f"past-events_{date}.csv", "w", encoding='utf-8', newline='') as file:
        writer = csv.writer(file, delimiter=',')

        writer.writerow(
            (
                "date",
                "sport",  # sport name
                "country",  # country (category-name)
                "tournament",  # tournament name
                "home_team_name",
                "away_team_name",
                "round_info",  # Roundinfo - name
                "event_id",  # id
                "type",  # finished
                "home_score",
                "away_score",
                "season_name",
                "city_name",
                "stadium_name",
                "referee_name"
            )
        )

def player_incidents_cards_creator(date):
    with open(f"player_incidents_cards_{date}.csv", "w", encoding='utf-8', newline='') as file:
        writer = csv.writer(file, delimiter=',')

        writer.writerow(
            (
                "event_id",  # country (category-name)
                "player_name",  # tournament name
                "reason",  # Roundinfo - name
                "time",  # id
                "is_home",
                "incident_class",
                'incident_type'
            )
        )

def player_incidents_substitutions_creator(date):
    with open(f"player_incidents_substitutions_{date}.csv", "w", encoding='utf-8', newline='') as file:
        writer = csv.writer(file, delimiter=',')

        writer.writerow(
            (
                "event_id",  # country (category-name)
                "player_in_name",  # tournament name
                "player_out_name",  # Roundinfo - name
                "time",  # id
                "is_home",
                'incident_type',
            )
        )

def player_incidents_goals_creator(date):
    with open(f"player_incidents_goals_{date}.csv", "w", encoding='utf-8', newline='') as file:
        writer = csv.writer(file, delimiter=',')

        writer.writerow(
            (
                "event_id",  # country (category-name)
                "home_score",
                "away_score",
                "player_name",  # tournament name
                "assist1_name",  # Roundinfo - name
                "time",  # id
                "isHome",
                'incident_type',
            )
        )

def player_incidents_inGamePenaltyies_creator(date):
    with open(f"player_incidents_inGamePenaltyies_{date}.csv", "w", encoding='utf-8', newline='') as file:
        writer = csv.writer(file, delimiter=',')

        writer.writerow(
            (
                "event_id",  # country (category-name)
                "name",
                "description",
                "time",  # id
                "isHome",
                "incident_class",
                'incident_type',
            )
        )

def player_incidents_penaltyShootouts_creator(date):
    with open(f"player_incidents_penaltyShootouts_{date}.csv", "w", encoding='utf-8', newline='') as file:
        writer = csv.writer(file, delimiter=',')

        writer.writerow(
            (
                "event_id",  # country (category-name)
                "name",
                "home_score",
                "away_score",
                "sequence",
                "description",
                "isHome",
                "incident_class",
                'incident_type',
            )
        )