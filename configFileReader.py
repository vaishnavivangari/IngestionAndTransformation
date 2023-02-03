import configparser

config_obj = configparser.ConfigParser()
config_obj.read(r'C:\Users\Vivek\IdeaProjects\IngestionAndTransformation\resources\config.properties')

host = config_obj.get('db', 'host')
user = config_obj.get('db', 'user')
password = config_obj.get('db', 'password')
dbName = config_obj.get('db','dbName')
sqlQuery = config_obj.get('db','sqlQuery')
sqlQueryAfterTransformation = config_obj.get('db','sqlQueryAfterTransformation')

credentials = config_obj.get('bq', 'credentials')
project_id = config_obj.get('bq','project_id')
dataset_id = config_obj.get('bq', 'dataset_id')
table_id = config_obj.get('bq', 'table_id')
result_file_path = config_obj.get('bq','result_file_path')

