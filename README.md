# airflow
BE CAREFUL, TO MAKE THE DAG WORK, YOU SHOULD UPLOAD OWN YOUR JSON FILE IN THE FOLDER "Data/Logins" ("Logins" folder should be created).
this JSON file looks like this (I didn't put mine for privacy) : 

{
  "username": "put_twitter_username",
  "password": "put_twitter_password",
  "api_key": "api_key_of_Beta_footbal_rapidAPI",
  "api_host": "api_host_of_Beta_footbal_rapidAPI"
}
NAME THE JSON FILE "LogPass.json".

Then you are ready to run your Dag named "ProjectBDD.py" on Airflow.
This dag will scrapp latest tweets of every premier League clubs and also get statistics of every Premier League Players.
