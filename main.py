from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("my_app").getOrCreate()

df = spark.read.json(r'C:\Users\PRASANNA\PycharmProjects\taf_automation\source_files\Complex.json',multiLine=True)
df.show()

#target:
#  path: 'C:\Users\PRASANNA\PycharmProjects\taf_automation\source_files\Complex.json'
#  type: "json"
#  schema: 'n'
#  options:
#    multiline: True
#  transformation: ["Y","sql"]
#  table: "customer_silver_expected"
#  cred_lookup: "sqlserver"
#  exclude_cols: ["created_date","updated_date","hash_key","batch_id"]

#source:
#  path: 'C:\Users\PRASANNA\PycharmProjects\taf_automation\source_files\Complex.json'
#  type: "json"
#  schema: 'n'
#  options:
#    multiline: True
#  transformation: ["Y","sql"]
#  table: "customer_silver_expected"
#  cred_lookup: "sqlserver"
#  exclude_cols: ["created_date","updated_date","hash_key","batch_id"]