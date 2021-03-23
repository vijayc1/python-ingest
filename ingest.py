from pyspark.sql import SparkSession
import json
import vertica_python
import os


def db_connection():
    conn_info = {'host':'','port':'','user':'','password':''}
    db = vertca_python.connect(**conn_info)
    cur = db.cursor()

def data_copy(df, table, target_table, pk_cols):
    update_set = ','.join(["{0}=src.{0}".format(i) for i in schema.names])
    insert_set = ','.join(list(map(lambda x : "src.{0}".format(x), schema.names)))
    merge = "MERGE INTO {0} tgt using {1} src ON sku WHEN MATCHED THEN UPDATE SET {2} WHEN NOT MATCHED THEN INSERT {3}".format(table,target_table,update_set,insert_set)
    cur.execute(merge)
    agg_name = 'table' + '_agg'
    agg_table(agg_name)

def agg_table(agg_name):
    drop_table = "drop table {0}".format(agg_name)
    cur.execute(drop_table )
    create_table = "create table {0} as select name,count(*) from {1} group by 1".format(agg_name,table)
    cur.executecreate_table)

file list = ['products.csv']
schema_file = open('schema file','r')
schema = StructType.fromJson(json.loads(schema_file.read()))
db_connection()

for i in file_list:
    stage_feed_csv = spark.read.csv(i, schema = schema)
    stage_feed_csv.createOrReplace("stage")
    pk_cols = 'sku'
    table = 'sku_details'
    df = spark.sql("""
    select name,sku,description
    from stage
    """)
    df.write.saveAsTable('temp_table')
    data_copy(df, table, target_table = temp_table, pk_cols)