import sys
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from pyspark.sql.functions import *
from pyspark.sql.types import *  
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import *
from awsglue.job import Job
import boto3

try:
    sns_client = boto3.client('sns')
    
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    
    args = getResolvedOptions(sys.argv, ['JOB_NAME','s3-input-path','s3-output-path'])
    input_path = args['s3_input_path']
    output_path = args['s3_output_path']
    
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # Extracting the S3 File passed by Lambda using GlueContext 
    dyf = glueContext.create_dynamic_frame_from_options(connection_type="s3", connection_options={"paths":[input_path]}, format="csv", format_options={"withHeader":True})
    
    #Converting DynamicFrame to PySpark DataFrame for further data transformation
    df = dyf.toDF()
    
    # Dropping duplicates if any exists
    df = df.drop_duplicates()
    
    # Renaming two columns with white spaces to PascalCase
    rename_columns={'Invoice':'InvoiceID','Customer ID':'CustomerID', 'InvoiceDate':'InvoiceDateTime'}
    df = df.withColumnsRenamed(rename_columns)
    
    # Fitering records where CustomerID is not NULL 
    df = df.filter(col('CustomerID') != '')
    
    # Filtering recordees where Quanity should be atleast 1 or more but 0 or below 0(Minus signed valuies) 
    df = df.where(col('Quantity') > 0)
    
    # Column Transformation - Trim and Capitalize first letter of the Description column using user defined function UDF
    udf_caps  = udf(lambda x: str(x).strip().capitalize(), StringType())
    df = df.withColumn('Description', udf_caps(col('Description')))
    
    # Type casting values for further processing - CustomerID to int type
    df = df.withColumn('CustomerID',expr('cast(CustomerID as int) as CustomerID'))
    
    #  Column TotalPrice is calculated with prepend value of Dollar sign($)
    udf_adddollar = udf(lambda x: ' $ '+ str(x) , StringType())
    df = df.withColumn('TotalPrice', udf_adddollar(((col('Quantity')*col('Price')).cast(FloatType())).cast(StringType())))
    
    # Adding Load_date of datetime(converted) to all records of the dataframe
    df = df.withColumn('t_InvoiceDate',expr('date(InvoiceDateTime)'))
    
    # Adding Partiton Columns for Parquet file writing 
    df = df.withColumn('day', expr('lpad(day(t_InvoiceDate),2,0)')).withColumn('month', expr('lpad(month(t_InvoiceDate),2,0)')).withColumn('year', expr('year(t_InvoiceDate)'))
    
    df = df.withColumnRenamed('t_InvoiceDate','InvoiceDate')
    
    # Dropping non-required column from dataframe
    df = df.drop('t_InvoiceDate')
    
    # Arranging column order in dataframe 
    columns = ['InvoiceID','CustomerID','StockCode','InvoiceDateTime','Description','Quantity','Price','TotalPrice','Country','InvoiceDate','year','month','day']
    df = df.select(columns)
    
    dyf = DynamicFrame.fromDF(df, glueContext)
    s3output = glueContext.getSink(
      path=output_path,
      connection_type="s3",
      updateBehavior="UPDATE_IN_DATABASE",
      partitionKeys=['year','month','day'],
      compression="snappy",
      enableUpdateCatalog=True
    )
    s3output.setCatalogInfo(
      catalogDatabase="db_orders", catalogTableName="retail_orders"
    )
    s3output.setFormat("glueparquet")
    s3output.writeFrame(dyf)
    sns_client.publish(
        Message="Orders Glue Job is executed Successfully",
        Subject="Glue Job Status",
        TopicArn = "arn:aws:sns:us-east-1:XXX:sns_send_orders_mail"
    )
except Exception as e:
    print(f'Glue Job Failed - {str(e)}')    
    sns_client.publish(
    Message="Orders Glue Job execution is Failed!",
    Subject="Glue Job Status",
    TopicArn = "arn:aws:sns:us-east-1:XXX:sns_send_orders_mail"
    )   

job.commit()



