import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## custom import
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, 
    concat, 
    lit, 
    coalesce, 
    to_timestamp, 
    substring, 
    current_timestamp, 
    expr, 
    regexp_replace, 
    row_number,
    from_utc_timestamp,
    unix_timestamp
)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# 出力先フォルダを初期化
glueContext.purge_s3_path(
    "s3://consumer-293-onecrmprocmining-aggregated3a04edd4-1pbw6xp6y8vrf/DAC_test/L2/Activities/",
    options={"retentionPeriod":0})


# インプットデータを取得
# event
df_event = glueContext.create_dynamic_frame.from_catalog(
    database="work_dac_test_l0",
    table_name="work_l0_m_event"
).toDF()

# task
df_task = glueContext.create_dynamic_frame.from_catalog(
    database="work_dac_test_l0",
    table_name="work_l0_m_task"
).toDF()

# opportunity
df_opportunity = glueContext.create_dynamic_frame.from_catalog(
    database="work_dac_test_l0",
    table_name="work_l0_m_opportunity"
).toDF()

# user
df_user = glueContext.create_dynamic_frame.from_catalog(
    database="work_dac_test_l0",
    table_name="work_l0_m_user"
).toDF()


# データを抽出
event_filtered = df_event.filter((col("IsDeleted") == "false") & 
                            (col("Type").isNotNull() | col("EventSubtype").isNotNull()))

# task
task_filtered = df_task.filter((col("TaskSubtype") != "Task") & 
                        (col("TaskSubtype") != "Other") & 
                        col("TaskSubtype").isNotNull() & 
                        col("ActivityDate").isNotNull() & 
                        (col("Status") == "Completed") & 
                        (col("IsDeleted") == "false") & 
                        (to_timestamp(substring("ActivityDate", 1, 19), "yyyy-MM-dd-HH:mm:ss") < current_timestamp()))

# opportunity
opp_filtered = df_opportunity.filter(col("IsDeleted") == "false")


# インプットデータを結合（イベント）
event_data = opp_filtered.alias("opp")\
    .join(event_filtered.alias("event"), col("event.WhatId") == col("opp.opportunityid__c"))\
    .join(df_user.alias("user"), col("user.Id") == col("event.CreatedById"), "left")
    
# インプットデータを結合（タスク）
task_data = opp_filtered.alias("opp")\
    .join(task_filtered.alias("task"), col("task.WhatId") == col("opp.Id"), "inner") \
    .join(df_user.alias("user"), col("user.Id") == col("task.CreatedById"), "left")


# データを加工（イベント）
event_selected = event_data.select(

    # _CASE_KEY
    col("opp.OPPORTUNITYID__C").alias("_CASE_KEY"), 
        
    # EVENTTIME
    #to_timestamp(regexp_replace(regexp_replace("event.CreatedDate","T"," "),"Z",""), "yyyy-MM-dd HH:mm:ss.SSS").alias("EVENTTIME"),
    from_utc_timestamp(unix_timestamp("event.CreatedDate", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").cast('timestamp'), 'UTC').alias("EVENTTIME"),
        
    # CONTACT_TYPE
    coalesce(col("event.Type"), col("event.EventSubtype")).alias("CONTACT_TYPE"),
        
    # ACCOUNT_ID
    col("opp.ACCOUNT__C").alias("ACCOUNT_ID"), 
        
    # USER_NAME
    coalesce(col("user.Name"), col("event.CreatedById")).alias("USER_NAME"))


event_selected.write.format("csv").mode("overwrite").option("header","true").save("s3://consumer-293-onecrmprocmining-aggregated3a04edd4-1pbw6xp6y8vrf/DAC_test/L2/test/")



# データを加工（タスク）
task_selected = task_data.select(
    
    # _CASE_KEY
    col("opp.Id").alias("_CASE_KEY"), 
        
    # EVENTTIME
    #to_timestamp(col("task.ActivityDate").substr(1, 19), "yyyy-MM-dd-HH:mm:ss").alias("EVENTTIME"),
    col("task.ActivityDate").alias("EVENTTIME"),
    
    # CONTACT_TYPE
    col("task.TaskSubtype").alias("CONTACT_TYPE"), 
    
    # ACCOUNT_ID
    col("opp.ACCOUNT__C").alias("ACCOUNT_ID"), 
    
    # USER_NAME
    coalesce(col("user.Name"), col("task.CreatedById")).alias("USER_NAME"))


# イベント、タスクデータを結合し、アクティビティデータを作成
window = Window.partitionBy("_CASE_KEY").orderBy("EVENTTIME")

# _SORTING、ACTIVITY_EN、ACTIVITY_TYPE列を追加
df_activities = event_selected.union(task_selected)\
    .withColumn("_SORTING", 20 + row_number().over(window))\
    .withColumn("ACTIVITY_EN", expr("'Create ' || CONTACT_TYPE"))\
    .withColumn("ACTIVITY_TYPE", expr("'Customer Contact'"))


# 加工データを出力
dyf = DynamicFrame.fromDF(event_selected, glueContext, 'df_activities')
glueContext.write_dynamic_frame.from_catalog(
    frame=dyf,
    database="aggregated_dac_test_l2",
    table_name="work_l2_t_activities"
)

job.commit()
