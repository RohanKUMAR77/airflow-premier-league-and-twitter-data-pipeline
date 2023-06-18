from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType
import os
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from pyspark.sql.functions import col, udf, unix_timestamp, round
import re
import requests
import json
from pyspark.sql import functions as F
from time import sleep
from selenium.common.exceptions import NoSuchElementException
from pyspark.sql.functions import udf
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pandas as pd


default_args = {
    'start_date': datetime(2023, 1, 1),
}

def fetch_data_1():
    def connexion_twitter():
        HOME = os.path.expanduser('~')
        DATALAKE_ROOT_FOLDER = HOME + "/airflow/Data/"
        JSON_LOG_FILE = DATALAKE_ROOT_FOLDER + 'Logins/LogPass.json'
        #Opening the JSON file, You should create yours with your information
        with open(JSON_LOG_FILE) as file:
            log_file = json.load(file)

        # Create variables from JSON data
        twitter_username = log_file['username']
        twitter_password = log_file['password']
        from pyvirtualdisplay import Display
        display = Display(visible=0, size=(800, 800))
        display.start()
        driver = webdriver.Chrome()
        sleep(2)
        driver.get("https://www.twitter.com/login")
        sleep(2)
        username = driver.find_element("xpath", '//input[@name="text"]')
        username.send_keys(twitter_username)
        username.send_keys(Keys.RETURN)
        sleep(2)
        password = driver.find_element("xpath", '//input[@name="password"]')
        my_password = twitter_password
        password.send_keys(my_password)
        password.send_keys(Keys.RETURN)
        sleep(2)
        print("Connexion à Twitter réussie")
        return driver

    def get_tweetsData(url, club, scroll_numbers, driver):
        driver.get(url)
        sleep(2)
        print("Collecting tweets from : ", club)
        tweets = driver.find_elements("xpath", '//article[@data-testid="tweet"]')
        start = driver.find_element("xpath", '//div[@class="css-1dbjc4n r-13awgt0 r-18u37iz r-1w6e6rj"]')
        nb_followers = \
        start.find_elements("xpath", './/span[@class="css-901oao css-16my406 r-poiln3 r-bcqeeo r-qvutc0"]')[2].text

        lines = []
        texts = []
        for tweet in tweets:
            now = datetime.now()
            line = [club, nb_followers, now]
            try:
                text = tweet.find_element("xpath", './/div[@data-testid="tweetText"]').text
                time = tweet.find_element("xpath", './/time').get_attribute('datetime')
                retweet = tweet.find_element("xpath", './/div[@data-testid="retweet"]').text
                like = tweet.find_element("xpath", './/div[@data-testid="like"]').text
                reply = tweet.find_element("xpath", './/div[@data-testid="reply"]').text
                views = tweet.find_element("xpath", '//div[@class="css-1dbjc4n r-18u37iz r-1h0z5md"]//a').get_attribute(
                    'aria-label')
                texts.append(text)
                line.extend([time, text, retweet, like, reply, views])
                lines.append(line)
            except NoSuchElementException:
                # Handle the error, e.g., you can choose to skip this tweet
                pass

        for k in range(scroll_numbers):
            driver.execute_script("window.scrollTo(0, window.scrollY + 1500);")
            now = datetime.now()
            sleep(1.5)
            card = driver.find_elements("xpath", '//article[@data-testid="tweet"]')
            sleep(2)
            for j in card:
                try:
                    Tweet = j.find_element("xpath", './/div[@data-testid="tweetText"]').text
                    if Tweet not in texts:
                        texts.append(Tweet)
                        line = [club, nb_followers, now]
                        text = j.find_element("xpath", './/div[@data-testid="tweetText"]').text
                        time = j.find_element("xpath", './/time').get_attribute('datetime')
                        retweet = j.find_element("xpath", './/div[@data-testid="retweet"]').text
                        like = j.find_element("xpath", './/div[@data-testid="like"]').text
                        reply = j.find_element("xpath", './/div[@data-testid="reply"]').text
                        views = j.find_element("xpath",
                                               '//div[@class="css-1dbjc4n r-18u37iz r-1h0z5md"]//a').get_attribute(
                            'aria-label')
                        line.extend([time, text, retweet, like, reply, views])
                        lines.append(line)
                except NoSuchElementException:
                    # Handle the error, e.g., you can choose to skip this tweet
                    pass
        return lines
    print("Starting to scrapp from TWITTER !!!!!!!!")
    today = date.today()

    clubs_url = [
        "ArsenalAcademy",
        "AVFCOfficial",
        "afcbournemouth",
        "BrentfordFC",
        "OfficialBHAFC",
        "ChelseaFC",
        "CPFC",
        "Everton",
        "FulhamFC",
        "LUFC",
        "LCFC",
        "LFC",
        "ManCity",
        "ManUtd",
        "NUFC",
        "NFFC",
        "SouthamptonFC",
        "SpursOfficial",
        "WestHam",
        "Wolves"
    ]
    club_names = [
        "Arsenal",
        "Aston Villa",
        "Bournemouth",
        "Brendford",
        "Brighton",
        "Chelsea",
        "Crystal Palace",
        "Everton",
        "Fulham",
        "Leeds",
        "Leicester",
        "Liverpool",
        "Manchester City",
        "Manchester United",
        "Newcastle",
        "Nottingham Forest",
        "Southampton",
        "Tottenham",
        "West Ham",
        "Wolves"
    ]
    clubs_url = ["https://twitter.com/" + k for k in clubs_url]
    Lines = []
    driver = connexion_twitter()
    for k in range(20):
        club_lines = get_tweetsData(clubs_url[k], club_names[k], 2, driver)
        for i in club_lines:
            Lines.append(i)
    driver.close()
    columns = ["Club", "Followers", "Request time", "Posting time", "Tweet", "Retweet", "Likes", "Replies", "Views"]
    ####Mettre un parquet spark
    struct_fields = [
        StructField("Club", StringType(), True),
        StructField("Followers", StringType(), True),
        StructField("Request time", TimestampType(), True),
        StructField("Posting time", StringType(), True),
        StructField("Tweet", StringType(), True),
        StructField("Retweet", StringType(), True),
        StructField("Likes", StringType(), True),
        StructField("Replies", StringType(), True),
        StructField("Views", StringType(), True)
    ]
    # Define the schema using the list of StructFields
    schema = StructType(struct_fields)
    # Create an empty list to store rows
    rows = []
    # Iterate over the lines and construct rows
    for line in Lines:
        rows.append(tuple(line))
    spark = SparkSession.builder.appName("Test").getOrCreate()
    df = spark.createDataFrame(rows, schema)
    HOME = os.path.expanduser('~')
    DATALAKE_ROOT_FOLDER = HOME + "/airflow/Data/"
    RAW_PATH = DATALAKE_ROOT_FOLDER + "Tweets/Raw/clubTweets-{}.parquet".format(today)
    df.write.parquet(RAW_PATH, mode="overwrite")
    print("Les tweets des clubs ont été ajoutés avec succés.")


def fetch_data_2():
    # Access the sensitive information using environment variables
    HOME = os.path.expanduser('~')
    DATALAKE_ROOT_FOLDER = HOME + "/airflow/Data/"
    JSON_LOG_FILE = DATALAKE_ROOT_FOLDER + 'Logins/LogPass.json'
    # Same here Opening the JSON file, You should create yours JSON file with your information
    with open(JSON_LOG_FILE) as file:
        log_file = json.load(file)
    api_key = log_file['api_key']
    api_host = log_file['api_host']
    print("Starting to fetch the Data 2")
    today = date.today()
    HOME = os.path.expanduser('~')
    DATALAKE_ROOT_FOLDER = HOME + "/airflow/Data/"
    RAW_PATH = DATALAKE_ROOT_FOLDER + "Players/Raw/Players-{}.parquet".format(today)
    if os.path.exists(RAW_PATH):
        print("Le fichier existe déjà.")
    else:
        print("Le fichier n'existe pas.")
        # Create SparkSession

        values = []
        clubs = [
            "Arsenal",
            "Aston Villa",
            "Bournemouth",
            "Brendford",
            "Brighton",
            "Chelsea",
            "Crystal Palace",
            "Everton",
            "Fulham",
            "Leeds",
            "Leicester",
            "Liverpool",
            "Manchester City",
            "Manchester United",
            "Newcastle",
            "Nottingham Forest",
            "Southampton",
            "Tottenham",
            "West Ham",
            "Wolves"
        ]

        #Number of Pages can be increased
        start_page = 2
        end_page = 47

        for k in range(start_page, end_page + 1):
            url = "https://api-football-beta.p.rapidapi.com/players"
            headers = {
                "X-RapidAPI-Key": api_key,
                "X-RapidAPI-Host": api_host
            }

            querystring = {"season": "2022", "league": "39", "page": str(k)}
            response = requests.get(url, headers=headers, params=querystring)
            json_result = response.json()
            # réponse pour la page k
            filename = DATALAKE_ROOT_FOLDER + "Players/Raw/JSON/random_player{}.json".format(k)
            # On supprime le contenu si il existe déjà

            if os.path.exists(filename):
                open(filename, "w").close()  # Supprime le contenu du fichier existant

            with open(filename, "w") as f:
                json.dump(json_result, f)

            print("\n Données de la page {} ont été enregistrées avec succès".format(k))

            # Ok donc la le fichier a été créé maintenant essayons de l'ouvrir avec spark
            spark = SparkSession.builder.appName("Test").getOrCreate()
            data = spark.read.json(filename).select("response").first()[0]
            added_players = 0
            for i in range(len(data)):
                row = data[i]

                # On définit quelques variables
                player = row.player
                statistics = row.statistics[0]
                dribbles = statistics.dribbles
                games = statistics.games

                if statistics.team.name in clubs:
                    line = [
                        player.id,
                        player.name,
                        statistics.team.name,
                        games.position,
                        player.height,
                        games.appearences,
                        games.lineups,
                        games.minutes,
                        games.rating,
                        statistics.goals.total,
                        statistics.penalty.scored,
                        statistics.goals.assists,
                        dribbles.attempts,
                        dribbles.success,
                        statistics.shots.total,
                        statistics.passes.total,
                        statistics.passes.key,
                        statistics.tackles.blocks,
                        statistics.tackles.interceptions,
                        statistics.tackles.total,
                        statistics.duels.total,
                        statistics.duels.won
                    ]
                    for j, value in enumerate(line):
                        if value is None:
                            line[j] = 0
                    values.append(line)
                    added_players += 1
            print("\n Page {} : {} Joueurs ont été ajoutés".format(k, added_players))

        ####Mettre un parquet spark



        struct_fields = [
            StructField('ID', IntegerType(), True),
            StructField('name', StringType(), True),
            StructField('club', StringType(), True),
            StructField('position', StringType(), True),
            StructField('taille', StringType(), True),
            StructField('matchs', IntegerType(), True),
            StructField('titulaire', IntegerType(), True),
            StructField('minutes', IntegerType(), True),
            StructField('note_moy', StringType(), True),
            StructField('buts', IntegerType(), True),
            StructField('penalty', IntegerType(), True),
            StructField('passe_de', IntegerType(), True),
            StructField('dribbles_tentes', IntegerType(), True),
            StructField('dribbles_reussis', IntegerType(), True),
            StructField('tirs', IntegerType(), True),
            StructField('passes', IntegerType(), True),
            StructField('passes_cles', IntegerType(), True),
            StructField('contres', IntegerType(), True),
            StructField('interceptions', IntegerType(), True),
            StructField('tacles', IntegerType(), True),
            StructField('duels', IntegerType(), True),
            StructField('duels_gagnes', IntegerType(), True)
        ]

        # Iterate over the column names and create StructFields

        # Define the schema using the list of StructFields
        schema = StructType(struct_fields)

        # Create an empty list to store rows
        rows = []

        # Iterate over the lines and construct rows
        for line in values:
            rows.append(tuple(line))

        # Create a dataframe from the list of rows and schema
        df = spark.createDataFrame(rows, schema)

        df = df.withColumn('ratio_but', F.when(df['matchs'] == 0, 0).otherwise(df['buts'] / df['matchs']))
        df = df.withColumn('assist_ratio', F.when(df['matchs'] == 0, 0).otherwise(df['passe_de'] / df['matchs']))
        df = df.withColumn('decisive', df['buts'] + df['passe_de'])
        df = df.withColumn('ratio_passes_cles',
                           F.when(df['matchs'] == 0, 0).otherwise(df['passes_cles'] / df['matchs']))
        df = df.withColumn('ratio_interceptions',
                           F.when(df['matchs'] == 0, 0).otherwise(df['interceptions'] / df['matchs']))
        df.write.parquet(RAW_PATH, mode="overwrite")

        print("\n Le parquet a été créé avec succès")

def raw_to_formatted_1():
    analyzer = SentimentIntensityAnalyzer()
    def integer_convert(s):
        k_format = False
        coma_format = False
        views_format = False
        string_format = False
        m_format = False
        if "K" in s:
            k_format = True
            s = int(float(s.replace("K", "")) * 1000)
            return s
        elif "M" in s:
            m_format = True
            s = int(float(s.replace("M", "")) * 1000000)
            return s
        elif "," in s:
            coma_format = True
            s = int(s.replace(",", ""))
            return s
        elif "Views" in s:
            views_format = True
            s = re.search(r'\d+', s)
            if s:
                return int(s.group())
        else:
            string_format = True
            return int(s)
    def convert_time(s):
        input_format = "%Y-%m-%dT%H:%M:%S.%fZ"

        # Convert input string to datetime object
        dt = datetime.strptime(s, input_format)

        return dt
    def get_sentiment(tweet):
        sentiment = analyzer.polarity_scores(tweet)
        return float(sentiment['compound'])

    print("Starting to format the Tweets")
    today = date.today()
    print(today)
    HOME = os.path.expanduser('~')
    DATALAKE_ROOT_FOLDER = HOME + "/airflow/Data/"
    RAW_PATH = DATALAKE_ROOT_FOLDER + "Tweets/Raw/clubTweets-{}.parquet".format(today)
    FORMAT_PATH = DATALAKE_ROOT_FOLDER + "Tweets/Format/clubTweets-{}.parquet".format(today)
    # Create a SparkSession
    spark = SparkSession.builder.appName("ParquetFileExample").getOrCreate()
    # Read the Parquet file
    df = spark.read.parquet(RAW_PATH)
    # Perform operations on the DataFrame
    print("Première version")
    print("\n\n\n")
    columns_to_convert = ["Retweet", "Followers", "Likes", "Replies", "Views"]
    # Convert columns to integers
    integer_convert_udf = udf(integer_convert, IntegerType())

    for column in columns_to_convert:
        df = df.withColumn(column, integer_convert_udf(df[column]))
    time_convert_udf = udf(convert_time, TimestampType())
    df = df.withColumn("Posting time", time_convert_udf(df["Posting time"]))
    df = df.withColumn("Fidelity", (col("Likes") + col("Replies") + col("Retweet")) / col("Followers"))

    df = df.withColumn("request_timestamp", unix_timestamp(col("Request time")))
    df = df.withColumn("posting_timestamp", unix_timestamp(col("Posting time")))

    # Calculate the time difference in hours
    df = df.withColumn("elapsed_time(h)", round((col("request_timestamp") - col("posting_timestamp")) / 3600, 2))
    df = df.drop("request_timestamp", "posting_timestamp")
    # Register the UDF
    sentiment_udf = udf(get_sentiment)
    # Apply the UDF to the "tweet" column
    df = df.withColumn("sentiment", sentiment_udf(col("Tweet")))
    df = df.withColumn("reaction_per_hour", (col("Retweet") + col("Likes") + col("Replies")) / col("elapsed_time(h)"))
    df.write.parquet(FORMAT_PATH, mode="overwrite")
    print("Les données ont été formatées avec succés")


def raw_to_formatted_2():
    def convert_height(s):
        height = int(s.split()[0])
        return height

    def convert_mean_rate(s):
        if s == "0":
            return float(0)
        else:
            return float(s)

    print("Starting to format players stats")
    today = date.today()
    HOME = os.path.expanduser('~')
    DATALAKE_ROOT_FOLDER = HOME + "/airflow/Data/"
    RAW_PATH = DATALAKE_ROOT_FOLDER + "Players/Raw/Players-{}.parquet".format(today)
    FORMAT_PATH = DATALAKE_ROOT_FOLDER + "Players/Format/Players-{}.parquet".format(today)
    spark = SparkSession.builder.appName("ParquetFileExample").getOrCreate()
    # Read the Parquet file
    df = spark.read.parquet(RAW_PATH)
    # On enlève les joueurs qui ont joué aucun match
    df = df.filter(df["matchs"] != 0)
    height_convert_udf = udf(convert_height, IntegerType())
    rate_convert_udf = udf(convert_mean_rate, FloatType())
    df = df.withColumn("taille", height_convert_udf(df["taille"]))
    df = df.withColumn("note_moy", rate_convert_udf(df["note_moy"]))

    df.write.parquet(FORMAT_PATH, mode="overwrite")
    print("Les données ont été formattées avec succès")


def combine_data():
    print("Starting to Combine the Data")
    today = date.today()
    HOME = os.path.expanduser('~')
    DATALAKE_ROOT_FOLDER = HOME + "/airflow/Data/"
    FORMAT_PATH_Tweets = DATALAKE_ROOT_FOLDER + "Tweets/Format/clubTweets-{}.parquet".format(today)
    FORMAT_PATH_Players = DATALAKE_ROOT_FOLDER + "Players/Format/Players-{}.parquet".format(today)
    COMBINE_PATH = DATALAKE_ROOT_FOLDER + "Combine/combined_data-{}.parquet".format(today)
    COMBINE_PATH_CSV = DATALAKE_ROOT_FOLDER + "Combine/combined_data-{}.csv".format(today)
    spark = SparkSession.builder.appName("ParquetFileExample").getOrCreate()
    # Read the Parquet file
    # Charger les deux DataFrames à partir des fichiers Parquet
    df1 = spark.read.parquet(FORMAT_PATH_Tweets)
    df2 = spark.read.parquet(FORMAT_PATH_Players)

    # Rename the "club" column in df2 to avoid conflict
    df2 = df2.withColumnRenamed("club", "club2")
    # Perform the join operation using the renamed column
    df_joint = df1.join(df2, df1["Club"] == df2["club2"], "left")
    df_joint = df_joint.filter(df_joint["name"].isNotNull())
    df_joint = df_joint.drop("club2")
    df_joint = df_joint.drop("Tweet")
    df_joint.write.parquet(COMBINE_PATH, mode="overwrite")
    df = pd.read_parquet(COMBINE_PATH)
    df.to_csv(COMBINE_PATH_CSV, index=False)
    print("Data Combined Successfully !!!!!")

def index_data():
    print("Indexing data to Elasticsearch - Task 8")



with DAG('Rohan_dagOP', default_args=default_args, schedule_interval=None) as dag:
    fetch_task_1 = PythonOperator(task_id='fetch_data_1', python_callable=fetch_data_1)
    fetch_task_2 = PythonOperator(task_id='fetch_data_2', python_callable=fetch_data_2)

    raw_to_formatted_task_1 = PythonOperator(task_id='raw_to_formatted_1', python_callable=raw_to_formatted_1)
    raw_to_formatted_task_2 = PythonOperator(task_id='raw_to_formatted_2', python_callable=raw_to_formatted_2)

    combine_task = PythonOperator(task_id='combine_data', python_callable=combine_data)
    index_task = PythonOperator(task_id='index_to_elastic', python_callable=index_data)

    fetch_task_1 >> raw_to_formatted_task_1 >> combine_task
    fetch_task_2 >> raw_to_formatted_task_2 >> combine_task
    combine_task >> index_task
