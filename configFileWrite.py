import configparser

config = configparser.ConfigParser()

config.add_section('db')
config.set('db', 'host', 'localhost')
config.set('db', 'user', 'root')
config.set('db', 'password', 'root')
config.set('db', 'dbName', 'org')

config.add_section('bq')
config.set('bq', 'credentials', '../resources/primordial-ship-367212-f38e484a8812.json')
config.set('bq', 'project_id', 'primordial-ship-367212')
config.set('bq', 'dataset_id', "primordial-ship-367212.org")
config.set('bq', 'table_id', "primordial-ship-367212.org.empPartition")
config.set('bq', 'result_file_path', '../resources/FinalResult')

with open(r'C:\Users\Vivek\IdeaProjects\IngestionAndTransformation\resources\config.properties', 'w') as configfile:
    config.write(configfile)

