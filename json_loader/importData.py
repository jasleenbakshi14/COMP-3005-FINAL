import json
import os
import psycopg2 as psycopg

# Connection
directory_path = '/Users/queenieli/Downloads/open-data-master/data'
root_database_name = "project_database"
query_database_name = "query_database"
db_username = 'postgres'
db_password = '8320'
db_host = 'localhost'
db_port = '5432'

# Connect to PostgreSQL
def connect_to_db():
    conn = psycopg.connect(
        dbname=query_database_name, 
        user=db_username, 
        password=db_password, 
        host=db_host, 
        port=db_port)
    cursor = conn.cursor()
    return conn, cursor

def create_tables(cursor):
    competitions_table = """
        CREATE TABLE IF NOT EXISTS competitions (
            competition_id INT PRIMARY KEY,
            competition_name VARCHAR(255),
            country_name VARCHAR(255),
            season_id INTEGER,
            competition_international BOOLEAN,
            season_name VARCHAR(255)
        );
    """
    
    players_table = """
        CREATE TABLE IF NOT EXISTS players (
            player_id INTEGER PRIMARY KEY,
            player_name VARCHAR(255),
            player_nickname VARCHAR(255),
            player_number INTEGER
        );
    """
    
    teams_table = """
        CREATE TABLE IF NOT EXISTS teams (
            match_id INTEGER,
            team_id INTEGER,
            team_name VARCHAR(255),
            PRIMARY KEY (match_id, team_id)
        );
    """
    
    shots_table = """
        CREATE TABLE IF NOT EXISTS shots (
            match_id INTEGER,
            statsbomb_xg DECIMAL,
            player_id INTEGER,
            player_name VARCHAR(255),
            team_id INTEGER,
            team_name VARCHAR(255),
            first_time BOOLEAN,
            PRIMARY KEY (match_id, player_id)
        );
    """
    
    passes_table = """
        CREATE TABLE IF NOT EXISTS passes (
            player_id INTEGER,
            player_name VARCHAR(255),
            team_id INTEGER,
            team_name VARCHAR(255),
            through_ball BOOLEAN,
            PRIMARY KEY (player_id, team_id)
        );
    """
    
    dribbles_table = """
        CREATE TABLE IF NOT EXISTS dribbles (
            player_id INTEGER,
            player_name VARCHAR(255),
            outcome VARCHAR(255),
            complete BOOLEAN,
            PRIMARY KEY (player_id, outcome)
        );
    """
    
    cursor.execute(competitions_table)
    cursor.execute(players_table)
    cursor.execute(teams_table)
    cursor.execute(shots_table)
    cursor.execute(passes_table)
    cursor.execute(dribbles_table)
    
    cursor.connection.commit()


def insert_competitions_data(cursor, competitions_data):
    for comp in competitions_data:
        insert_query = """
        INSERT INTO competitions (competition_id, competition_name, country_name, season_id, 
                                  competition_international, season_name)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            comp["competition_id"], comp["competition_name"], comp["country_name"], comp["season_id"],
            comp["competition_international"], comp["season_name"]
        ))
    
    cursor.connection.commit()

def insert_players_data(cursor, players_data):
    for player in players_data:
        insert_query = """
        INSERT INTO players (player_id, player_name, player_nickname, player_number)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            player["player_id"], player.get("player_name"), player.get("player_nickname"), player.get("player_number")
        ))
    
    cursor.connection.commit()

if __name__ == "__main__":
    conn, cursor = connect_to_db()
    #competitions_data = load_json_data('/Users/queenieli/Downloads/open-data-master/data')
    #insert_competitions_data(cursor, competitions_data)
    # Create Tables
    create_tables(cursor)
    #insert_competitions_data(cursor, conn)
    conn.commit() 
    
    
    # Close connection
    cursor.close()
    conn.close()
