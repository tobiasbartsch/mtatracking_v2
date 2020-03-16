# mtatracking_v2

This project aims to (1) track the position of trains in the NYC subway, (2) train machine learning models to predict train delays and transit times, and (3) a webapp to display predictions in real time.

(1) You can create database tables and start scraping into them using the following functions:
  a) set up a Postgres SQL database
  b) execute create_tables.py
  c) start scraping by executing scrape_MTA_feeds.py. You will need to enter an MTA API key which you can request here: https://api.mta.info .
  New data will then be automatically added to your postgres database every 30 seconds.

(2) There are three different machine learning models that have to be trained:
- A model based on minimum description length that determines systematic changes in transit time between adjacent stations (for example due to track maintenance). This model is described in detail here: https://www.tobiasbartsch.com/manhattan-bridge/
You can fit this model to your data using the historicTrainDelays class in the subway_system_analyzer module.
- Outlier detection to determine atypically late trains. This, too, is implemented in the historicTrainDelays class.
- The two models described above generate the features for a third Random Forest model, which predicts the transit times between stations in the subway system. See PredictCurrentTransitTime.ipynb. This model is saved and then used for predictions by the webapp.

(3) You can start the webapp by executing webapp/main.py.

Here is a flowchart that summarizes the bullet points above:
![how it works](https://github.com/tobiasbartsch/mtatracking_v2/blob/master/webapp/static/how_it_works.jpg)
