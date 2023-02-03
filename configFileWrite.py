import configparser

config = configparser.ConfigParser()

config.add_section('db')
config.set('db', 'host', 'localhost')
config.set('db', 'user', 'root')
config.set('db', 'password', 'root')
config.set('db', 'dbName', 'org')
config.set('db', 'sqlQuery', r'C:\Users\Vivek\IdeaProjects\IngestionAndTransformation\resources\query.sql')
config.set('db', 'sqlQueryAfterTransformation', r'C:\Users\Vivek\IdeaProjects\IngestionAndTransformation\resources\query_transform.sql')

config.add_section('bq')
config.set('bq', 'credentials', '../resources/regal-hybrid-376309-3532bdd667af.json')
config.set('bq', 'project_id', 'regal-hybrid-376309')
config.set('bq', 'dataset_id', "regal-hybrid-376309.org")
config.set('bq', 'table_id', "regal-hybrid-376309.org.resultPartition")
config.set('bq', 'result_file_path', '../resources/FinalResult')

with open(r'C:\Users\Vivek\IdeaProjects\IngestionAndTransformation\resources\config.properties', 'w') as configfile:
    config.write(configfile)

