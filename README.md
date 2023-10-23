This project will scrap latest tweets of every Premier League clubs and also get statistics of every Premier League Players on real time. 

The only thing you need to make it work is a twitter account and subscribe to the rapidapi API of Premier League players (it is free). Read below for how to make the project work for you.


YOU SHOULD UPLOAD YOUR OWN JSON FILE IN THE FOLDER "Data/Logins" ("Logins" folder should be created).
This JSON file follows the structure bellow : 

{
  "username": "twitter_account_username",
  "password": "twitter_account_password",
  "api_key": "api_key_of_Beta_footbal_rapidAPI",
  "api_host": "api_host_of_Beta_footbal_rapidAPI"
}



SAVE THE JSON FILE AS "LogPass.json".
Then you are ready to run your Dag named "ProjectBDD.py" on Airflow 

