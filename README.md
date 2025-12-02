# Valorant-Data-Analysis-4700-5700-Fall-2025
Going to be scraping vlr.gg for all statistics related to the professional scene. Our team will sort the data into a database dating back to when the Valorant Professional scene first became franchised, and continue to update the database as new matches and stats are entered. Then we will create a clean way for the user to navigate through the data
# Run and setup the GUI
in order for the gui to run proberly all that is needed it run the scrapper (if not it will run the gui but the search wont work), be sure to change this area from line 5-12:

SERVER_NAME = 'localhost\SQLEXPRESS'  
DATABASE_NAME = 'vlr_matches'
USE_WINDOWS_AUTH = False   (Set to False for SQL Server Authentication)
SQL_USER = 'sa'
SQL_PASSWORD = 'zelda'

so it connects to your local database, be sure to change on both the run_scraper_enhanced.py and valorant_search_gui.py and once it connects proberly and you run the search then it should all work as intended
