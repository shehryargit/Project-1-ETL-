# Project-1-ETL-
Acquiring and Processing information on world's largest banks Data : 

This project is regarding the acquiring and processing information on world's largest banks with having Python as skill in hand we majorly used Python and its libraries like Pandas & matplotlib for transformation & visualization, Beautiful soap for data scraping & SQLite for creating database. 


1) Extract (extract function) :  The extract function uses BeautifulSoup to scrape data from a specified Wikipedia page and creates a DataFrame with relevant information such as bank names and market capitalization in USD billions.

2) Transform (transform function): The transform function accesses a CSV file containing exchange rate information and adds three columns to the DataFrame. These columns represent the transformed market capitalization values in GBP, EUR, and INR (scaled by the corresponding exchange rate factors).

3) Load to CSV (load_to_csv function): The load_to_csv function saves the final DataFrame as a CSV file in the specified output path.

4)Load to Database (load_to_db function): The load_to_db function saves the final DataFrame to a SQLite database table with the provided name.

5)Run Query (run_query function): The run_query function executes SQL queries on the database table and prints the results to the terminal.

6)Logging (log_progress function): The log_progress function logs messages at different stages of the ETL process to a log file (code_log.txt).
