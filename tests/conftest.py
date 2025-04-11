import pytest
import yaml

from pyspark.sql import SparkSession
import os
# from src.utility.general_utility import flatten


#
# dir_path = os.path.dirname(os.path.abspath(__file__))
# print(dir_path)
# azure_jar_path = dir_path+'azure-storage-8.6.6.jar'
# print(azure_jar_path)

@pytest.fixture(scope='session')
def spark_session(request):
    dir_path = os.path.dirname(os.path.abspath(__file__))
    azure_jar_path = dir_path + 'azure-storage-8.6.6.jar'
    # snow_jar = '/jar/snowflake-jdbc-3.14.3.jar'
    # postgres_jar = '/Users/admin/PycharmProjects/test_automation_project/jar/postgresql-42.2.5.jar'
    # azure_storage = '/Users/admin/PycharmProjects/test_automation_project/jar/azure-storage-8.6.6.jar'
    # hadoop_azure = '/Users/admin/PycharmProjects/test_automation_project/jar/hadoop-azure-3.3.1.jar'
    sql_server = r'C:\Users\PRASANNA\PycharmProjects\taf_automation\jars\mysql-connector-j-9.1.0.jar'
    # jar_path = snow_jar + ',' + postgres_jar + ',' + azure_storage + ',' + hadoop_azure + ',' + sql_server
    jar_path = sql_server
    spark = SparkSession.builder.master("local[2]") \
        .appName("pytest_framework") \
        .config("spark.jars", jar_path) \
        .config("spark.driver.extraClassPath", jar_path) \
        .config("spark.executor.extraClassPath", jar_path) \
        .getOrCreate()
    print("Jars inside Spark:", spark.sparkContext.getConf().get("spark.jars"))
    return spark

# def read_cred(dir_path):
#     cred_file_path = os.path.dirname(dir_path)+'/project.cred.yml'
#     with open(cred_file_path,'r') as f:
#         creds = yaml.safe_load(f)
#     return creds

def read_query(dir_path):
    sql_query_path = dir_path+'/transformation.sql'
    with open(sql_query_path,'r') as file:
        sql_query = file.read()
    return sql_query


def read_db(spark, config_data, dir_path):
    cred = load_credentials()
    cred_lookup = config_data['cred_lookup']
    cred = cred[cred_lookup]
    print('cred',cred)
    if config_data['transformation'][0].lower() == 'Y' and config_data['transformation'][1].lower() == 'sql':
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
    target_config = config_data['target']
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

