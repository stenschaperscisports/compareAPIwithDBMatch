# compare_match.py
import datetime
import json
import requests
import pandas as pd
from urllib.parse import urlencode
import pyodbc

def compare_match(environment, match_id, csv_filename):
    # Load the configuration file based on the environment

    if environment == 'prod':
        api_config_file = "../properties/configapi_prod.json"
        db_config_file = "../properties/configdb_prod.json"
    else:
        api_config_file = "../properties/configapi.json"
        db_config_file = "../properties/configdb.json"

    with open(api_config_file) as f:
        api_config = json.load(f)

    with open(db_config_file) as f:
        db_config = json.load(f)



    # AccessToken class and related functions
    class AccessToken:
        access_token = ""

        def __init__(self, properties_file_path, token_url):
            with open(properties_file_path) as f:
                prop = json.load(f)
            grant_type = prop.get("grant_type")
            username = prop.get("username")
            password = prop.get("password")
            client_id = prop.get("client_id")
            client_secret = prop.get("client_secret")
            scope = prop.get("scope")

            # send the request
            url = token_url
            print("URL:", url)
            data = {
                "grant_type": grant_type,
                "username": username,
                "password": password,
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": scope
            }
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            print("Data:", data)
            response = requests.post(url, data=data, headers=headers)
            print(response)
            response.raise_for_status()
            json_data = json.loads(response.text)
            AccessToken.access_token = json_data["access_token"]

            # Save the access token to a variable and print it
            self.saved_access_token = AccessToken.access_token
            # print("Access token:", self.saved_access_token)
            response.close()

        def get_access_token(self):
            return AccessToken.access_token



    def get_token(config):
        if environment == 'prod':
            access_token = AccessToken("../properties/api_credentials_prod.json", "https://identity.scisports.app/connect/token")
        else:
            access_token = AccessToken("../properties/api_credentials.json", "https://identity-test.scisports.app/connect/token")
        print(str(access_token))
        token = access_token.get_access_token()
        if token is None:
            print("Error getting access token")
            return None
        return token


    def get_api_match_and_players(config, match_id):
        print(config)
        token = get_token(config)
        if token is None:
            return
        endpoint = f"/api/v1/wyscout/matches/{match_id}"
        params = {}
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        url = f"{config['api']['base_url']}{endpoint}{urlencode(params)}"
        response = requests.get(url, headers=headers)
        if response.status_code != 200 or response.headers.get('content-type', '').lower() != 'application/json; charset=utf-8':
            print(f"Error getting data from API. Status code: {response.status_code}")
            return None

        json_data = response.json()
        response.close()
        # print("API data:", json_data)
        return json_data


    # Load the configuration file
    with open(api_config_file) as f:
        config = json.load(f)
        print(str(config))


    # Step 1: Fetch data from the API
    api_data = get_api_match_and_players(config, match_id)

    def get_db_config(file_path):
        with open(file_path, 'r') as f:
            db_config = json.load(f)
        return db_config

    def compare_values(value1, value2):
        if isinstance(value1, (int, float)) and isinstance(value2, (int, float)):
            return float(value1) == float(value2)
        else:
            return str(value1) == str(value2)

    def get_db_data(config, match_id, query):
        conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={config["db"]["server"]};Database={config["db"]["database"]};UID={config["db"]["username"]};PWD={config["db"]["password"]}'
        with pyodbc.connect(conn_str) as conn:
            df = pd.read_sql_query(query, conn, params=[match_id])
        return df

    # Load DB configuration
    db_config = get_db_config(db_config_file)

    # Define your query
    query = """SELECT M.*, S.START_DATE, S.END_DATE, S.NAME as SEASON_NAME, L.GENDER, L.AREA_ID, L.NAME as LEAGUE_NAME, MTP.PLAYER_ID AS PLAYER_ID, MTP.GOALS, MTP.OWN_GOALS, MTP.RED_CARDS, MTP.SHIRT_NUMBER, MTP.YELLOW_CARDS, MTP.MINUTES_PLAYED, MTP.STARTING, MTP.POSITION_1,
        CASE
            WHEN MTP.TEAM_ID = M.HOME_TEAM_ID THEN 1
            ELSE 0
        END as IS_HOME,
        TH.NAME as HOME_TEAM_NAME,
        TA.NAME as AWAY_TEAM_NAME
    FROM MATCHES M
    JOIN MATCH_TEAM_PLAYERS MTP ON M.MATCH_ID = MTP.MATCH_ID
    JOIN SEASONS S ON M.SEASON_ID = S.SEASON_ID
    JOIN LEAGUES L ON M.LEAGUE_ID = L.LEAGUE_ID
    JOIN MATCH_TEAMS MTH ON M.MATCH_ID = MTH.MATCH_ID AND MTH.SIDE = 'home'
    JOIN MATCH_TEAMS MTA ON M.MATCH_ID = MTA.MATCH_ID AND MTA.SIDE = 'away'
    JOIN TEAMS TH ON M.HOME_TEAM_ID = TH.TEAM_ID
    JOIN TEAMS TA ON M.AWAY_TEAM_ID = TA.TEAM_ID
    WHERE M.MATCH_ID = ?;
    """


    # Get data from DB
    # print("print query "+query)
    db_df = get_db_data(db_config, match_id, query) # match_id via parameter in the main
    # print("db_df"+str(db_df))
    # db_df.to_csv("comparison_results_5.csv", index=False)


    # create a db frame
    db_data = {
        "START_DATE": "2020-01-01T00:00:00",
        "END_DATE": "2020-12-31T00:00:00",
        "SEASON_NAME": "Premier League 2020/2021",
        "GENDER": "M",
        "AREA_ID": 2072,
        "LEAGUE_NAME": "Premier League",
        "HOME_TEAM_ID": 6698,
        "AWAY_TEAM_ID": 4687,
        "PLAYER_ID": 123456,
        "GOALS": 0,
        "OWN_GOALS": 0,
        "RED_CARDS": 0,
        "SHIRT_NUMBER": 7,
        "YELLOW_CARDS": 0,
        "MINUTES_PLAYED": 90,
        "STARTING": True,
        "POSITION_1": "FW",
        "IS_HOME": 1,
        "HOME_TEAM_NAME": "Manchester United",
        "AWAY_TEAM_NAME": "Liverpool",
        "KICKOFF_DATE": "2021-06-20 16:00:00.00"
    }

    db_df = pd.DataFrame([db_data])
    # db_df['STARTING'] = db_df['STARTING'].astype(bool)
    db_df['STARTING'] = db_df['STARTING'].replace({True: 1, False: 0}).astype(bool)



    # Get data from DB
    db_df = get_db_data(db_config, match_id, query)
    db_df['START_DATE'] = pd.to_datetime(db_df['START_DATE']).dt.strftime('%Y-%m-%dT%H:%M:%S')
    db_df['END_DATE'] = pd.to_datetime(db_df['END_DATE']).dt.strftime('%Y-%m-%dT%H:%M:%S')
    db_df['KICKOFF_DATE'] = pd.to_datetime(db_df['KICKOFF_DATE']).dt.strftime('%Y-%m-%dT%H:%M:%S')

    # Update db_data dictionary with the values from the database
    db_data = {
        "START_DATE": db_df.loc[0, "START_DATE"],
        "END_DATE": db_df.loc[0, "END_DATE"],
        "SEASON_NAME": db_df.loc[0, "SEASON_NAME"],
        "GENDER": db_df.loc[0, "GENDER"],
        "AREA_ID": db_df.loc[0, "AREA_ID"],
        "LEAGUE_NAME": db_df.loc[0, "LEAGUE_NAME"],
        "HOME_TEAM_ID": db_df.loc[0, "HOME_TEAM_ID"],
        "AWAY_TEAM_ID": db_df.loc[0, "AWAY_TEAM_ID"],
        "PLAYER_ID": db_df.loc[0, "PLAYER_ID"],
        "GOALS": db_df.loc[0, "GOALS"],
        "OWN_GOALS": db_df.loc[0, "OWN_GOALS"],
        "RED_CARDS": db_df.loc[0, "RED_CARDS"],
        "SHIRT_NUMBER": db_df.loc[0, "SHIRT_NUMBER"],
        "YELLOW_CARDS": db_df.loc[0, "YELLOW_CARDS"],
        "MINUTES_PLAYED": db_df.loc[0, "MINUTES_PLAYED"],
        "STARTING": db_df.loc[0, "STARTING"],
        "POSITION_1": db_df.loc[0, "POSITION_1"],
        "IS_HOME": db_df.loc[0, "IS_HOME"],
        "HOME_TEAM_NAME": db_df.loc[0, "HOME_TEAM_NAME"],
        "AWAY_TEAM_NAME": db_df.loc[0, "AWAY_TEAM_NAME"],
        "KICKOFF_DATE": db_df.loc[0, "KICKOFF_DATE"]
    }

    # Step 3: Compare the values
    def get_nested_value(data, key_path):
        keys = key_path.split(".")
        value = data
        for key in keys:
            if "[" in key and "]" in key:
                key, index = key.split("[")[0], int(key.split("[")[1].split("]")[0])
                value = value[key][index]
            else:
                value = value[key]
        return value



# Match mappings
    mappings = [
        ("START_DATE", "season.startDate"),
        ("END_DATE", "season.endDate"),
        ("SEASON_NAME", "season.name"),
        ("GENDER", "league.gender"),
        ("AREA_ID", "league.nation"),
        ("LEAGUE_NAME", "league.name"),
        ("KICKOFF_DATE", "kickOffDate"),
        ("HOME_TEAM_ID", "homeTeam.sourceReferences[0].sourceValue"),
        ("AWAY_TEAM_ID", "awayTeam.sourceReferences[0].sourceValue"),
        ("HOME_TEAM_NAME", "homeTeam.name"),
        ("AWAY_TEAM_NAME", "awayTeam.name"),
        ("KICKOFF_DATE", "kickOffDate"),
    ]

    # Create an empty DataFrame to store the comparison results
    comparison_df = pd.DataFrame(columns=['DB Column Name', 'API Name', 'DB Value', 'API Value', 'Match'])

    # Loop through the mappings and compare the values
    for mapping in mappings:
        # Get the column name from the DB DataFrame and the API data
        db_column_name, api_name = mapping


            # Get the value from the DB DataFrame
        db_value = db_df.loc[0, db_column_name]
        # Convert gender code to string
        if db_column_name == "GENDER":
            db_value = "Male" if db_value == 1 else "Female"
        if isinstance(db_value, str):
            try:
                db_value = pd.to_datetime(db_value).strftime('%Y-%m-%dT%H:%M:%S')
            except ValueError:
                pass
        db_df[db_column_name] = db_value

        def normalize_date_format(date_string):
            date_string = '2020-01-01T00:00:00'
            date_string = date_string.replace('T', ' ')
            datetime_obj = datetime.datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
            return datetime_obj.isoformat()

        # Get the value from the API data
        api_value = get_nested_value(api_data, api_name)
        # print("api "+str(api_value))

        # Compare the values
        match = compare_values(db_value, api_value)


        comparison_df = pd.concat([comparison_df, pd.DataFrame([{
            'DB Column Name': db_column_name,
            'API Name': api_name,
            'DB Value': db_value,
            'API Value': api_value,
            'Match': bool(match)
        }]).astype({'Match': bool})], ignore_index=True)

    # Compare player data
    player_mappings = [
        ("PLAYER_ID", "sourceReferences[0].sourceValue"),
        ("SHIRT_NUMBER", "shirtNumber"),
        ("MINUTES_PLAYED", "minutesPlayed"),
        ("STARTING", "starting"),
        ("POSITION_1", "position"),
        # Add more mappings as needed
    ]

    for team_type in ["homeTeam", "awayTeam"]:
        is_home = 1 if team_type == "homeTeam" else 0

        for i, db_player in db_df[db_df['IS_HOME'] == is_home].iterrows():
            db_shirt_number = db_player['SHIRT_NUMBER']
            matching_api_player = None

            for api_player in api_data[team_type]["players"]:
                if api_player["shirtNumber"] == db_shirt_number:
                    matching_api_player = api_player
                    break

            if not matching_api_player:
                print(f"No matching API player found for DB shirt number {db_shirt_number} of match {match_id}.")
                continue

            for mapping in player_mappings:
                db_column_name, api_name = mapping

                # Get the value from the DB DataFrame
                db_value = db_player[db_column_name]

                # Get the value from the API player data
                api_value = get_nested_value(matching_api_player, api_name)
                match = compare_values(db_value, api_value)

                # # Add the comparison result to the comparison_df
                # comparison_df = pd.concat([comparison_df, pd.DataFrame([{
                #     'DB Column Name': db_column_name,
                #     'API Name': f"{team_type}.players[{i}].{api_name}",
                #     'DB Value': db_value,
                #     'API Value': api_value,
                #     'Match': bool(match)
                # }], index=[0])], ignore_index=True)
                # # Add the comparison result to the comparison_df
                comparison_df = pd.concat([comparison_df, pd.DataFrame([{
                    'DB Column Name': db_column_name,
                    'API Name': f"{team_type}.players[{i}].{api_name}",
                    'DB Value': db_value,
                    'API Value': api_value,
                    'Match': bool(match)
                }], index=[0])], ignore_index=True)

                # Cast 'Match' column to bool dtype
                comparison_df['Match'] = comparison_df['Match'].astype(bool)

    # Print the comparison DataFrame
    print(comparison_df)
    comparison_df.to_csv(csv_filename, index=False)
