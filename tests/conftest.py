import pytest
import yaml
import sys
import os

# Add the project root to Python path - this is the key fix
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from pyspark.sql import SparkSession
# from src.utility.general_utility import flatten
from databricks.connect import DatabricksSession

@pytest.fixture(scope='session')
def spark_session(request):
    # Check if any test requests a databricks connection
    # This is a simple way to decide which spark session to create
    # A more robust solution might use command-line arguments
    use_databricks = False
    config_path = request.node.fspath.dirname+'/config.yml'
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            if config.get('source', {}).get('type') == 'databricks' or config.get('target', {}).get('type') == 'databricks':
                use_databricks = True

    if use_databricks:
        # Creates a session using the Databricks Connect configuration
        spark = DatabricksSession.builder.getOrCreate()
    else:
        # Your existing local Spark Session logic
        dir_path = os.path.dirname(os.path.abspath(__file__))
        sql_server = r'C:\Users\PRASANNA\PycharmProjects\taf_automation\jars\mysql-connector-j-9.1.0.jar'
        jar_path = sql_server
        spark = SparkSession.builder.master("local[2]") \
            .appName("pytest_framework") \
            .config("spark.jars", jar_path) \
            .config("spark.driver.extraClassPath", jar_path) \
            .config("spark.executor.extraClassPath", jar_path) \
            .getOrCreate()
    
    return spark

# def read_cred(dir_path):
#     cred_file_path = os.path.dirname(dir_path)+'/project.cred.yml'
#     with open(cred_file_path,'r') as f:
#         creds = yaml.safe_load(f)
#     return creds

def read_query(dir_path):
    print(dir_path)
    sql_query_path = dir_path+'/transformation.sql'
    with open(sql_query_path,'r') as file:
        sql_query = file.read()
    return sql_query


def read_db(spark, config_data, dir_path):
    cred = load_credentials()
    cred_lookup = config_data['cred_lookup']
    cred = cred[cred_lookup]
    print('cred',cred)

    # Handle Databricks table reads
    if cred_lookup == 'databricks':
        print(f"Reading Databricks table: {config_data['table']}")
        return spark.read.table(config_data['table'])

    if config_data['transformation'][0].lower() == 'y' and config_data['transformation'][1].lower() == 'sql':
        sql_query = read_query(dir_path)
        print("sql_query",sql_query)
        df = spark.read.format("jdbc").\
            option("url",cred['url']). \
            option("user",cred['user']).\
            option("password",cred['password']).\
            option("query",sql_query).\
            option("driver",cred['driver']).load()
    else:
        df = (spark.read.format("jdbc").
            option("url",cred['url']).
            option("user",cred['user']).
            option("password",cred['password']).
            option("dbtable",config_data['table']).
            option("driver",cred['driver']).load())
    return df

def read_file(spark,config_data):
    df = None
    if config_data['type'] == 'csv':
        df = spark.read.csv(config_data['path'],inferSchema=True,header=True)
    elif config_data['type'] == 'json':
        df = spark.read.json(config_data['path'],multiLine=True)
        df = flatten(df)
    if config_data['type'] == 'parquet':
        df = spark.read.parquet(config_data['path'])
    elif config_data['type'] == 'avro':
        df = spark.read.format('avro').load(config_data['path'])
    return df

def load_credentials(env="qa"):
    """Load credentials from the centralized YAML file."""
    taf_path = os.path.dirname(os.path.abspath(__file__))
    credentials_path = taf_path+'\\project_cred.yml'

    with open(credentials_path, "r") as file:
        credentials = yaml.safe_load(file)
        print(credentials[env])
    return credentials[env]

# load_credentials()



# def test_1(spark_session):
#     assert True
@pytest.fixture(scope='module')
def read_config(request):
    config_path = request.node.fspath.dirname+'/config.yml'
    with open(config_path,'r') as f:
        config_data = yaml.safe_load(f)
    return config_data

@pytest.fixture(scope='module')
def read_data(spark_session,read_config,request):
    config_data = read_config
    spark = spark_session
    print("config yml data ",config_data)
    dir_path = request.node.fspath.dirname
    print(dir_path)
    source_config = config_data['source']
    print("source_config is : ",source_config)
    target_config = config_data['target']
    print("target_config is : ", target_config)
    # validation_config = config_data['validations']
    if source_config['type'] == 'database':
        source = read_db(spark,source_config,dir_path)
    else:
        source = read_file(spark,source_config)

    if target_config['type'] == 'database':
        target = read_db(spark,target_config,dir_path)
    else:
        target = read_file(spark,target_config)

    return source,target

