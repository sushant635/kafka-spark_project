import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession 
from pyspark.sql.functions import from_json,col
from pyspark.sql.types import StructType,StructField,StringType


def create_keyspace(session):
    session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_strams
            WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};

    """)
    print("Keyspace is created successfuly")

def create_table(session):
    #create table
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_stream.create_users(
            id UUID PRIMARY KEY,
            frist_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            dob TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT);
        """)
    
    print("Table created successfully")


def insert_table(session,**kwargs):
    #inserting data into table 
   print("inserting data")

   user_id = kwargs.get('id')
   first_name = kwargs.get('first_name')
   last_name = kwargs.get('last_name')
   gender = kwargs.get('gender')
   address = kwargs.get('address')
   postcode  = kwargs.get('post_code')
   email = kwargs.get('email')
   username = kwargs.get('username')
   dob = kwargs.get('dob')
   registered_date = kwargs.get('registered_date')
   phone = kwargs.get('phone')
   picture = kwargs.get('photo')

   try:
       session.execute("""
                INSERT INTO spark_stream.create_users(id,first_name,last_name,gender,address,
                       post_code,email,username,dob,registered_date,phone,picture)
                       values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,(user_id,first_name,last_name,gender,address,postcode,email,username,dob,registered_date,phone,picture))
       logging.info(f"Data inserted for {first_name} {last_name}")
   except Exception as e:
       logging.error(f"could not insert data due to {e}")

def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder\
                .appName('SparkDataStreaming')\
                .config('spark.jars.packages',"com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1")\
                .config("saprk.cassandra.connection.host",'localhost')\
                .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spakr connection create successfuly")
    except Exception as e:
        logging.error(f"couldn't create the spark session to exception {e}")
    
    return s_conn

def connect_to_kafka(spark_con):
    spark_df = None
    try:
        
        spark_df = spark_con.readStream\
                    .formate('kafka')\
                    .option('kafka.bootstrap.servers','localhost:9092')\
                    .option('subscribe','users_created')\
                    .option('startingOffsets','earliest')\
                    .load()
        print(spark_df)
        logging.info(f"kafka dataframe created successfully")
       
       
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because {e}")

    return spark_df
    

def create_cassandra_connection():
    try:

        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"could not create cassandra connection due to {e}")
        return None
    
def selection_df(spark_df):
    print(spark_df)
    schema = StructType([
        StructField('id',StringType(),False),
        StructField('first_name',StringType(),False),
        StructField('last_name',StringType(),False),
        StructField('gender',StringType(),False),
        StructField('address',StringType(),False),
        StructField('post_code',StringType(),False),
        StructField('email',StringType(),False),
        StructField('username',StringType(),False),
        StructField('dob',StringType(),False),
        StructField('registered_date',StringType(),False),
        StructField('phone',StringType(),False),
        StructField('picture',StringType(),False),

    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)")\
                    .select(from_json(col('value'),schema).aliad('data')).select("data.*")
    
    print(sel)

    return sel


if __name__ == "__main__":

    spark_conn = create_spark_connection()

    if spark_conn is not None:
        df = connect_to_kafka(spark_conn)
        session = create_cassandra_connection()
        print(session)
        selection_df = selection_df(df)


        if session is not None:
            create_keyspace(session)
            create_table(session)
            # insert_table(session,)

            streaming_query = (selection_df.writeStream.format('org.apache.spark.sql.cassandra')\
                                            .option('checkpoointLocation','/tem/checkpoint')\
                                            .option('keyspace','spark_strams')\
                                            .option('table','create_users')
                                            .start())
            streaming_query.awaitTermination()




