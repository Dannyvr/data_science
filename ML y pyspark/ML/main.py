# imports
#from pyspark.sql.functions import col, dense_rank, sum, avg, max, min, count
from pyspark.sql import SparkSession
from pyspark.sql.types import (DateType, IntegerType, FloatType, StringType,
                               StructField, StructType, TimestampType)
import sys

from functions import drop_select_col,split_col,join_dfs,cast_columns, drop_null,order_columns

######################## se reciven los CSV #######################

input_1=sys.argv[1]
input_2=sys.argv[2]


####################### crear la session de spark #######################

spark = SparkSession \
    .builder \
    .appName("ML JDBC pipeline") \
    .config("spark.driver.extraClassPath", "postgresql-42.2.14.jar") \
    .config("spark.executor.extraClassPath", "postgresql-42.2.14.jar") \
    .getOrCreate()


######################## archivo Heart Attack #######################

df_Heart = spark \
    .read \
    .format("csv") \
    .option("path", input_1) \
    .option("header", True) \
    .schema(StructType([StructField('Patient ID',StringType()),
                        StructField('Age',FloatType()),
                        StructField('Sex',StringType()),
                        StructField('Cholesterol',FloatType()),
                        StructField('Blood Pressure',StringType()),
                        StructField('Heart Rate',FloatType()),
                        StructField('Diabetes',FloatType()),
                        StructField('Family History',FloatType()),
                        StructField('Smoking',FloatType()),
                        StructField('Obesity',FloatType()),
                        StructField('Alcohol Consumption',FloatType()),
                        StructField('Exercise Hours Per Week',FloatType()),
                        StructField('Diet',StringType()),
                        StructField('Previous Heart Problems',FloatType()),
                        StructField('Medication Use',FloatType()),
                        StructField('Stress Level',FloatType()),
                        StructField('Sedentary Hours Per Day',FloatType()),
                        StructField('Income',FloatType()),
                        StructField('BMI',FloatType()),
                        StructField('Triglycerides',FloatType()),
                        StructField('Physical Activity Days Per Week',FloatType()),
                        StructField('Sleep Hours Per Day',FloatType()),
                        StructField('Country',StringType()),
                        StructField('Continent',StringType()),
                        StructField('Hemisphere',StringType()),
                        StructField('Heart Attack Risk',FloatType())])) \
    .load()
         

#df_Heart.show(1)
#df_Heart.printSchema()


######################## archivo Quality life #######################
df_country = spark \
    .read \
    .format("csv") \
    .option("path", input_2) \
    .option("header", True) \
    .schema(StructType([StructField('Country',StringType()),
                        StructField('Quality of Life Index',FloatType()),
                        StructField('Purchasing Power Index',FloatType()),
                        StructField('Safety Index',FloatType()),
                        StructField('Health Care Index',FloatType()),
                        StructField('Cost of Living Index',FloatType()),
                        StructField('Property Price to Income Ratio',FloatType()),
                        StructField('Traffic Commute Time Index',FloatType()),
                        StructField('Pollution Index',FloatType()),
                        StructField('Climate Index',FloatType())])) \
    .load()




#df_country.show(1)
#df_country.printSchema()



####################### Borramos las columnas que no ocupamos #################################
#print(df_Heart.columns)

columns=['Patient ID','Continent','Hemisphere']
df_Heart=drop_select_col(df_Heart,columns,drop_columns=True)

#print(df_Heart.columns)


################### Separar la presion en los 2 tipos ###############
name1='systolic'
name2='diastolic'
column='Blood Pressure'
limit='/'
df_Heart=split_col(df_Heart, column, limit,name1,name2)


## pasar las columnas a float
columns_names=['systolic','diastolic']
df_Heart=cast_columns(df_Heart,columns_names,'float')


### pese a que no quedan nulls en este caso, creamos una funcion para eliminarlos

df_Heart=drop_null(df_Heart)
df_country=drop_null(df_country)


### imprimer summary
df_Heart.select(df_Heart.columns[:14]).summary().show()
df_Heart.select(df_Heart.columns[14:]).summary().show()
df_country.summary().show()

## al ver las estadistica no parece haber valores atipicos ya que el maximo y percentil 75 estan bastante cerca


####################### Unir los dos dataset ################

on_columns=['Country'] #columnas para el primer join
df_join=join_dfs(df_Heart, df_country,on_columns, type_join='inner')
#print(df_join.columns)
#print(len(df_join.columns))


##################### Pasamos la columna target al final para mantener el orden


column_order=['Heart Attack Risk']
df_join=order_columns(df_join,column_order)

df_join.printSchema()
### pese a que no quedan nulls en este caso, creamos una funcion para eliminarlos

df_join=drop_null(df_join)


############## Guardar los datos en la base de datos ##########################################
# Almacenar el conjunto de datos limpio en la base de datos

##Dataframe 1
df_Heart.write \
    .format("jdbc") \
    .mode('overwrite') \
    .option("url", "jdbc:postgresql://host.docker.internal:5433/postgres") \
    .option("user", "postgres") \
    .option("password", "testPassword") \
    .option("dbtable", "Heart") \
    .option("driver", "org.postgresql.Driver")\
    .save()
    
##Dataframe 2
df_country.write \
    .format("jdbc") \
    .mode('overwrite') \
    .option("url", "jdbc:postgresql://host.docker.internal:5433/postgres") \
    .option("user", "postgres") \
    .option("password", "testPassword") \
    .option("dbtable", "country") \
    .option("driver", "org.postgresql.Driver")\
    .save()


##Dataframe unido
df_join.write \
    .format("jdbc") \
    .mode('overwrite') \
    .option("url", "jdbc:postgresql://host.docker.internal:5433/postgres") \
    .option("user", "postgres") \
    .option("password", "testPassword") \
    .option("dbtable", "Heart_country") \
    .option("driver", "org.postgresql.Driver")\
    .save()