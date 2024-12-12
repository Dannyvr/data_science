from functions import drop_select_col,split_col,join_dfs,cast_columns, drop_null,order_columns
from pyspark.sql.types import (DateType, IntegerType, FloatType, StringType,
                               StructField, StructType, TimestampType)
#test 1
#probamos hacer drop en columnas 
def test_drop_columnas(spark_session):
# Definir el esquema de los datos
    schema = StructType([StructField('Patient ID',StringType()),
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
                        StructField('Heart Attack Risk',FloatType())])

    # Datos
    data = [
        ("BMW7812", 67.0, "Male", 208.0, "158/88", 72.0, 0.0, 0.0, 1.0, 0.0, 0.0, 4.0, "Average", 0.0, 0.0, 9.0, 7.0, 261404.0, 31.0, 286.0, 0.0, 6.0, "Argentina", "South America", "Southern Hemisphere", 0.0),
        ("CZE1114", 21.0, "Male", 389.0, "165/93", 98.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, "Unhealthy", 1.0, 0.0, 1.0, 5.0, 285768.0, 27.0, 235.0, 1.0, 7.0, "Canada", "North America", "Northern Hemisphere", 0.0),
        ("BNI9906", 21.0, "Female", 324.0, "174/99", 72.0, 1.0, 0.0, 0.0, 0.0, 0.0, 2.0, "Healthy", 1.0, 1.0, 9.0, 9.0, 235282.0, 28.0, 587.0, 4.0, 4.0, "France", "Europe", "Northern Hemisphere", 0.0),
        ("JLN3497", 84.0, "Male", 383.0, "163/100", 73.0, 1.0, 1.0, 1.0, 0.0, 1.0, 10.0, "Average", 1.0, 0.0, 9.0, 8.0, 125640.0, 36.0, 378.0, 3.0, 4.0, "Canada", "North America", "Northern Hemisphere", 0.0),
        ("GFO8847", 66.0, "Male", 318.0, "91/88", 93.0, 1.0, 1.0, 1.0, 1.0, 0.0, 6.0, "Unhealthy", 1.0, 0.0, 6.0, 2.0, 160555.0, 22.0, 231.0, 1.0, 5.0, "Thailand", "Asia", "Northern Hemisphere", 0.0)
    ]

    df = spark_session.createDataFrame(data, schema=schema)

    columns=['Patient ID','Continent','Hemisphere']
    df_result=drop_select_col(df,columns,drop_columns=True)

    df_result.show()
    
    ### Dataset esperado
    expected =[
        (67.0, "Male", 208.0, "158/88", 72.0, 0.0, 0.0, 1.0, 0.0, 0.0, 4.0, "Average", 0.0, 0.0, 9.0, 7.0, 261404.0, 31.0, 286.0, 0.0, 6.0, "Argentina",  0.0),
        ( 21.0, "Male", 389.0, "165/93", 98.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, "Unhealthy", 1.0, 0.0, 1.0, 5.0, 285768.0, 27.0, 235.0, 1.0, 7.0, "Canada",  0.0),
        ( 21.0, "Female", 324.0, "174/99", 72.0, 1.0, 0.0, 0.0, 0.0, 0.0, 2.0, "Healthy", 1.0, 1.0, 9.0, 9.0, 235282.0, 28.0, 587.0, 4.0, 4.0, "France", 0.0),
        (84.0, "Male", 383.0, "163/100", 73.0, 1.0, 1.0, 1.0, 0.0, 1.0, 10.0, "Average", 1.0, 0.0, 9.0, 8.0, 125640.0, 36.0, 378.0, 3.0, 4.0, "Canada",  0.0),
        (66.0, "Male", 318.0, "91/88", 93.0, 1.0, 1.0, 1.0, 1.0, 0.0, 6.0, "Unhealthy", 1.0, 0.0, 6.0, 2.0, 160555.0, 22.0, 231.0, 1.0, 5.0, "Thailand",  0.0)
    ]


    schema = StructType([StructField('Age',FloatType()),
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
                        StructField('Heart Attack Risk',FloatType())])

        
    expected_ds=spark_session.createDataFrame(expected, schema=schema)

    assert df_result.collect() == expected_ds.collect()
    
 
 
    
#test 2
#probamos hacer select en columnas 
def test_select_columnas(spark_session):
# Definir el esquema de los datos
    schema = StructType([StructField('Patient ID',StringType()),
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
                        StructField('Heart Attack Risk',FloatType())])

    # Datos
    data = [
        ("BMW7812", 67.0, "Male", 208.0, "158/88", 72.0, 0.0, 0.0, 1.0, 0.0, 0.0, 4.0, "Average", 0.0, 0.0, 9.0, 7.0, 261404.0, 31.0, 286.0, 0.0, 6.0, "Argentina", "South America", "Southern Hemisphere", 0.0),
        ("CZE1114", 21.0, "Male", 389.0, "165/93", 98.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, "Unhealthy", 1.0, 0.0, 1.0, 5.0, 285768.0, 27.0, 235.0, 1.0, 7.0, "Canada", "North America", "Northern Hemisphere", 0.0),
        ("BNI9906", 21.0, "Female", 324.0, "174/99", 72.0, 1.0, 0.0, 0.0, 0.0, 0.0, 2.0, "Healthy", 1.0, 1.0, 9.0, 9.0, 235282.0, 28.0, 587.0, 4.0, 4.0, "France", "Europe", "Northern Hemisphere", 0.0),
        ("JLN3497", 84.0, "Male", 383.0, "163/100", 73.0, 1.0, 1.0, 1.0, 0.0, 1.0, 10.0, "Average", 1.0, 0.0, 9.0, 8.0, 125640.0, 36.0, 378.0, 3.0, 4.0, "Canada", "North America", "Northern Hemisphere", 0.0),
        ("GFO8847", 66.0, "Male", 318.0, "91/88", 93.0, 1.0, 1.0, 1.0, 1.0, 0.0, 6.0, "Unhealthy", 1.0, 0.0, 6.0, 2.0, 160555.0, 22.0, 231.0, 1.0, 5.0, "Thailand", "Asia", "Northern Hemisphere", 0.0)
    ]

    df = spark_session.createDataFrame(data, schema=schema)

    columns=['Patient ID','Continent','Hemisphere']
    df_result=drop_select_col(df,columns)

    df_result.show()
    
    ### Dataset esperado
    expected =[
        ("BMW7812",  "South America", "Southern Hemisphere"),
        ("CZE1114",  "North America", "Northern Hemisphere"),
        ("BNI9906", "Europe", "Northern Hemisphere"),
        ("JLN3497", "North America", "Northern Hemisphere"),
        ("GFO8847",  "Asia", "Northern Hemisphere")
        ]


    schema = StructType([StructField('Patient ID',StringType()),
                        StructField('Continent',StringType()),
                        StructField('Hemisphere',StringType())])

        
    expected_ds=spark_session.createDataFrame(expected, schema=schema)

    assert df_result.collect() == expected_ds.collect()
 


#test 3
#probamos hacer un split  en un columna 
def test_split_col(spark_session):
# Definir el esquema de los datos
    schema = StructType([StructField('Patient ID',StringType()),
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
                        StructField('Heart Attack Risk',FloatType())])

    # Datos
    data = [
        ("BMW7812", 67.0, "Male", 208.0, "158/88", 72.0, 0.0, 0.0, 1.0, 0.0, 0.0, 4.0, "Average", 0.0, 0.0, 9.0, 7.0, 261404.0, 31.0, 286.0, 0.0, 6.0, "Argentina", "South America", "Southern Hemisphere", 0.0),
        ("CZE1114", 21.0, "Male", 389.0, "165/93", 98.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, "Unhealthy", 1.0, 0.0, 1.0, 5.0, 285768.0, 27.0, 235.0, 1.0, 7.0, "Canada", "North America", "Northern Hemisphere", 0.0),
        ("BNI9906", 21.0, "Female", 324.0, "174/99", 72.0, 1.0, 0.0, 0.0, 0.0, 0.0, 2.0, "Healthy", 1.0, 1.0, 9.0, 9.0, 235282.0, 28.0, 587.0, 4.0, 4.0, "France", "Europe", "Northern Hemisphere", 0.0),
        ("JLN3497", 84.0, "Male", 383.0, "163/100", 73.0, 1.0, 1.0, 1.0, 0.0, 1.0, 10.0, "Average", 1.0, 0.0, 9.0, 8.0, 125640.0, 36.0, 378.0, 3.0, 4.0, "Canada", "North America", "Northern Hemisphere", 0.0),
        ("GFO8847", 66.0, "Male", 318.0, "91/88", 93.0, 1.0, 1.0, 1.0, 1.0, 0.0, 6.0, "Unhealthy", 1.0, 0.0, 6.0, 2.0, 160555.0, 22.0, 231.0, 1.0, 5.0, "Thailand", "Asia", "Northern Hemisphere", 0.0)
    ]

    df = spark_session.createDataFrame(data, schema=schema)

    name1='systolic'
    name2='diastolic'
    column='Blood Pressure'
    limit='/'
    df_result=split_col(df, column, limit,name1,name2)

    df_result.show()
    
    ### Dataset esperado
    ### Dataset esperado
    expected =[
        ("BMW7812", 67.0, "Male", 208.0, 72.0, 0.0, 0.0, 1.0, 0.0, 0.0, 4.0, "Average", 0.0, 0.0, 9.0, 7.0, 261404.0, 31.0, 286.0, 0.0, 6.0, "Argentina", "South America", "Southern Hemisphere", 0.0,"158","88"),
        ("CZE1114", 21.0, "Male", 389.0, 98.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, "Unhealthy", 1.0, 0.0, 1.0, 5.0, 285768.0, 27.0, 235.0, 1.0, 7.0, "Canada", "North America", "Northern Hemisphere", 0.0,"165","93"),
        ("BNI9906", 21.0, "Female", 324.0, 72.0, 1.0, 0.0, 0.0, 0.0, 0.0, 2.0, "Healthy", 1.0, 1.0, 9.0, 9.0, 235282.0, 28.0, 587.0, 4.0, 4.0, "France", "Europe", "Northern Hemisphere", 0.0,"174","99"),
        ("JLN3497", 84.0, "Male", 383.0,73.0, 1.0, 1.0, 1.0, 0.0, 1.0, 10.0, "Average", 1.0, 0.0, 9.0, 8.0, 125640.0, 36.0, 378.0, 3.0, 4.0, "Canada", "North America", "Northern Hemisphere", 0.0,"163","100"),
        ("GFO8847", 66.0, "Male", 318.0,93.0, 1.0, 1.0, 1.0, 1.0, 0.0, 6.0, "Unhealthy", 1.0, 0.0, 6.0, 2.0, 160555.0, 22.0, 231.0, 1.0, 5.0, "Thailand", "Asia", "Northern Hemisphere", 0.0,"91","88")
    ]


    schema = StructType([StructField('Patient ID',StringType()),
                            StructField('Age',FloatType()),
                            StructField('Sex',StringType()),
                            StructField('Cholesterol',FloatType()),
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
                            StructField('Heart Attack Risk',FloatType()),
                            StructField('systolic',StringType()),
                            StructField('diastolic',StringType())])

        
    expected_ds=spark_session.createDataFrame(expected, schema=schema)

    assert df_result.collect() == expected_ds.collect()
 
 
#test 4
#probamos hacer un split  en un columna 
def test_split_col_2(spark_session):
# Definir el esquema de los datos
    schema = StructType([StructField('Patient ID',StringType()),
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
                        StructField('Heart Attack Risk',FloatType())])

    # Datos
    data = [
        ("BMW7812", 67.0, "Male", 208.0, "158:88", 72.0, 0.0, 0.0, 1.0, 0.0, 0.0, 4.0, "Average", 0.0, 0.0, 9.0, 7.0, 261404.0, 31.0, 286.0, 0.0, 6.0, "Argentina", "South America", "Southern Hemisphere", 0.0),
        ("CZE1114", 21.0, "Male", 389.0, "165:93", 98.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, "Unhealthy", 1.0, 0.0, 1.0, 5.0, 285768.0, 27.0, 235.0, 1.0, 7.0, "Canada", "North America", "Northern Hemisphere", 0.0),
        ("BNI9906", 21.0, "Female", 324.0, "174:99", 72.0, 1.0, 0.0, 0.0, 0.0, 0.0, 2.0, "Healthy", 1.0, 1.0, 9.0, 9.0, 235282.0, 28.0, 587.0, 4.0, 4.0, "France", "Europe", "Northern Hemisphere", 0.0),
        ("JLN3497", 84.0, "Male", 383.0, "163:100", 73.0, 1.0, 1.0, 1.0, 0.0, 1.0, 10.0, "Average", 1.0, 0.0, 9.0, 8.0, 125640.0, 36.0, 378.0, 3.0, 4.0, "Canada", "North America", "Northern Hemisphere", 0.0),
        ("GFO8847", 66.0, "Male", 318.0, "91:88", 93.0, 1.0, 1.0, 1.0, 1.0, 0.0, 6.0, "Unhealthy", 1.0, 0.0, 6.0, 2.0, 160555.0, 22.0, 231.0, 1.0, 5.0, "Thailand", "Asia", "Northern Hemisphere", 0.0)
    ]

    df = spark_session.createDataFrame(data, schema=schema)

    name1='systolic'
    name2='diastolic'
    column='Blood Pressure'
    limit=':'
    df_result=split_col(df, column, limit,name1,name2)

    df_result.show()
    
    ### Dataset esperado
    expected =[
        ("BMW7812", 67.0, "Male", 208.0, 72.0, 0.0, 0.0, 1.0, 0.0, 0.0, 4.0, "Average", 0.0, 0.0, 9.0, 7.0, 261404.0, 31.0, 286.0, 0.0, 6.0, "Argentina", "South America", "Southern Hemisphere", 0.0,"158","88"),
        ("CZE1114", 21.0, "Male", 389.0, 98.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, "Unhealthy", 1.0, 0.0, 1.0, 5.0, 285768.0, 27.0, 235.0, 1.0, 7.0, "Canada", "North America", "Northern Hemisphere", 0.0,"165","93"),
        ("BNI9906", 21.0, "Female", 324.0, 72.0, 1.0, 0.0, 0.0, 0.0, 0.0, 2.0, "Healthy", 1.0, 1.0, 9.0, 9.0, 235282.0, 28.0, 587.0, 4.0, 4.0, "France", "Europe", "Northern Hemisphere", 0.0,"174","99"),
        ("JLN3497", 84.0, "Male", 383.0,73.0, 1.0, 1.0, 1.0, 0.0, 1.0, 10.0, "Average", 1.0, 0.0, 9.0, 8.0, 125640.0, 36.0, 378.0, 3.0, 4.0, "Canada", "North America", "Northern Hemisphere", 0.0,"163","100"),
        ("GFO8847", 66.0, "Male", 318.0,93.0, 1.0, 1.0, 1.0, 1.0, 0.0, 6.0, "Unhealthy", 1.0, 0.0, 6.0, 2.0, 160555.0, 22.0, 231.0, 1.0, 5.0, "Thailand", "Asia", "Northern Hemisphere", 0.0,"91","88")
    ]


    schema = StructType([StructField('Patient ID',StringType()),
                            StructField('Age',FloatType()),
                            StructField('Sex',StringType()),
                            StructField('Cholesterol',FloatType()),
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
                            StructField('Heart Attack Risk',FloatType()),
                            StructField('systolic',StringType()),
                            StructField('diastolic',StringType())])

        
    expected_ds=spark_session.createDataFrame(expected, schema=schema)

    assert df_result.collect() == expected_ds.collect()
    
 


#test 5
#probamos caster a float
def test_cast_float(spark_session):
# Definir el esquema de los datos
    data =[
        ("BMW7812", 67.0, "Male", 208.0, 72.0, 0.0, 0.0, 1.0, 0.0, 0.0, 4.0, "Average", 0.0, 0.0, 9.0, 7.0, 261404.0, 31.0, 286.0, 0.0, 6.0, "Argentina", "South America", "Southern Hemisphere", 0.0,"158","88"),
        ("CZE1114", 21.0, "Male", 389.0, 98.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, "Unhealthy", 1.0, 0.0, 1.0, 5.0, 285768.0, 27.0, 235.0, 1.0, 7.0, "Canada", "North America", "Northern Hemisphere", 0.0,"165","93"),
        ("BNI9906", 21.0, "Female", 324.0, 72.0, 1.0, 0.0, 0.0, 0.0, 0.0, 2.0, "Healthy", 1.0, 1.0, 9.0, 9.0, 235282.0, 28.0, 587.0, 4.0, 4.0, "France", "Europe", "Northern Hemisphere", 0.0,"174","99"),
        ("JLN3497", 84.0, "Male", 383.0,73.0, 1.0, 1.0, 1.0, 0.0, 1.0, 10.0, "Average", 1.0, 0.0, 9.0, 8.0, 125640.0, 36.0, 378.0, 3.0, 4.0, "Canada", "North America", "Northern Hemisphere", 0.0,"163","100"),
        ("GFO8847", 66.0, "Male", 318.0,93.0, 1.0, 1.0, 1.0, 1.0, 0.0, 6.0, "Unhealthy", 1.0, 0.0, 6.0, 2.0, 160555.0, 22.0, 231.0, 1.0, 5.0, "Thailand", "Asia", "Northern Hemisphere", 0.0,"91","88")
    ]


    schema = StructType([StructField('Patient ID',StringType()),
                            StructField('Age',FloatType()),
                            StructField('Sex',StringType()),
                            StructField('Cholesterol',FloatType()),
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
                            StructField('Heart Attack Risk',FloatType()),
                            StructField('systolic',StringType()),
                            StructField('diastolic',StringType())])
                            
    df = spark_session.createDataFrame(data, schema=schema)

    columns_names=['systolic','diastolic']
    df_result=cast_columns(df,columns_names,'float')

    df_result.show()
    
    ### Dataset esperado
    expected =[
        ("BMW7812", 67.0, "Male", 208.0, 72.0, 0.0, 0.0, 1.0, 0.0, 0.0, 4.0, "Average", 0.0, 0.0, 9.0, 7.0, 261404.0, 31.0, 286.0, 0.0, 6.0, "Argentina", "South America", "Southern Hemisphere", 0.0,158.0,88.0),
        ("CZE1114", 21.0, "Male", 389.0, 98.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, "Unhealthy", 1.0, 0.0, 1.0, 5.0, 285768.0, 27.0, 235.0, 1.0, 7.0, "Canada", "North America", "Northern Hemisphere", 0.0,165.0,93.0),
        ("BNI9906", 21.0, "Female", 324.0, 72.0, 1.0, 0.0, 0.0, 0.0, 0.0, 2.0, "Healthy", 1.0, 1.0, 9.0, 9.0, 235282.0, 28.0, 587.0, 4.0, 4.0, "France", "Europe", "Northern Hemisphere", 0.0,174.0,99.0),
        ("JLN3497", 84.0, "Male", 383.0,73.0, 1.0, 1.0, 1.0, 0.0, 1.0, 10.0, "Average", 1.0, 0.0, 9.0, 8.0, 125640.0, 36.0, 378.0, 3.0, 4.0, "Canada", "North America", "Northern Hemisphere", 0.0,163.0,100.0),
        ("GFO8847", 66.0, "Male", 318.0,93.0, 1.0, 1.0, 1.0, 1.0, 0.0, 6.0, "Unhealthy", 1.0, 0.0, 6.0, 2.0, 160555.0, 22.0, 231.0, 1.0, 5.0, "Thailand", "Asia", "Northern Hemisphere", 0.0,91.0,88.0)
    ]


    schema = StructType([StructField('Patient ID',StringType()),
                            StructField('Age',FloatType()),
                            StructField('Sex',StringType()),
                            StructField('Cholesterol',FloatType()),
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
                            StructField('Heart Attack Risk',FloatType()),
                            StructField('systolic',FloatType()),
                            StructField('diastolic',FloatType())])

        
    expected_ds=spark_session.createDataFrame(expected, schema=schema)

    assert df_result.collect() == expected_ds.collect()
    

 #test 6
#probamos caster a string
def test_cast_string(spark_session):
# Definir el esquema de los datos
    data =[
        ("BMW7812", 67.0, "Male", 208.0, 72.0, 0.0, 0.0, 1.0, 0.0, 0.0, 4.0, "Average", 0.0, 0.0, 9.0, 7.0, 261404.0, 31.0, 286.0, 0.0, 6.0, "Argentina", "South America", "Southern Hemisphere", 0.0,158.0,88.0),
        ("CZE1114", 21.0, "Male", 389.0, 98.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, "Unhealthy", 1.0, 0.0, 1.0, 5.0, 285768.0, 27.0, 235.0, 1.0, 7.0, "Canada", "North America", "Northern Hemisphere", 0.0,165.0,93.0),
        ("BNI9906", 21.0, "Female", 324.0, 72.0, 1.0, 0.0, 0.0, 0.0, 0.0, 2.0, "Healthy", 1.0, 1.0, 9.0, 9.0, 235282.0, 28.0, 587.0, 4.0, 4.0, "France", "Europe", "Northern Hemisphere", 0.0,174.0,99.0),
        ("JLN3497", 84.0, "Male", 383.0,73.0, 1.0, 1.0, 1.0, 0.0, 1.0, 10.0, "Average", 1.0, 0.0, 9.0, 8.0, 125640.0, 36.0, 378.0, 3.0, 4.0, "Canada", "North America", "Northern Hemisphere", 0.0,163.0,100.0),
        ("GFO8847", 66.0, "Male", 318.0,93.0, 1.0, 1.0, 1.0, 1.0, 0.0, 6.0, "Unhealthy", 1.0, 0.0, 6.0, 2.0, 160555.0, 22.0, 231.0, 1.0, 5.0, "Thailand", "Asia", "Northern Hemisphere", 0.0,91.0,88.0)
    ]


    schema = StructType([StructField('Patient ID',StringType()),
                            StructField('Age',FloatType()),
                            StructField('Sex',StringType()),
                            StructField('Cholesterol',FloatType()),
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
                            StructField('Heart Attack Risk',FloatType()),
                            StructField('systolic',FloatType()),
                            StructField('diastolic',FloatType())])
                            
    df = spark_session.createDataFrame(data, schema=schema)

    columns_names=['systolic','diastolic']
    df_result=cast_columns(df,columns_names,'string')

    df_result.show()

    ### Dataset esperado


    expected =[
        ("BMW7812", 67.0, "Male", 208.0, 72.0, 0.0, 0.0, 1.0, 0.0, 0.0, 4.0, "Average", 0.0, 0.0, 9.0, 7.0, 261404.0, 31.0, 286.0, 0.0, 6.0, "Argentina", "South America", "Southern Hemisphere", 0.0,"158.0","88.0"),
        ("CZE1114", 21.0, "Male", 389.0, 98.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, "Unhealthy", 1.0, 0.0, 1.0, 5.0, 285768.0, 27.0, 235.0, 1.0, 7.0, "Canada", "North America", "Northern Hemisphere", 0.0,"165.0","93.0"),
        ("BNI9906", 21.0, "Female", 324.0, 72.0, 1.0, 0.0, 0.0, 0.0, 0.0, 2.0, "Healthy", 1.0, 1.0, 9.0, 9.0, 235282.0, 28.0, 587.0, 4.0, 4.0, "France", "Europe", "Northern Hemisphere", 0.0,"174.0","99.0"),
        ("JLN3497", 84.0, "Male", 383.0,73.0, 1.0, 1.0, 1.0, 0.0, 1.0, 10.0, "Average", 1.0, 0.0, 9.0, 8.0, 125640.0, 36.0, 378.0, 3.0, 4.0, "Canada", "North America", "Northern Hemisphere", 0.0,"163.0","100.0"),
        ("GFO8847", 66.0, "Male", 318.0,93.0, 1.0, 1.0, 1.0, 1.0, 0.0, 6.0, "Unhealthy", 1.0, 0.0, 6.0, 2.0, 160555.0, 22.0, 231.0, 1.0, 5.0, "Thailand", "Asia", "Northern Hemisphere", 0.0,"91.0","88.0")
    ]


    schema = StructType([StructField('Patient ID',StringType()),
                            StructField('Age',FloatType()),
                            StructField('Sex',StringType()),
                            StructField('Cholesterol',FloatType()),
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
                            StructField('Heart Attack Risk',FloatType()),
                            StructField('systolic',StringType()),
                            StructField('diastolic',StringType())])




    expected_ds=spark_session.createDataFrame(expected, schema=schema)

    assert df_result.collect() == expected_ds.collect()
 
 
 
#test 7
#probamos hacer un split  en un columna 
def test_Join_left(spark_session):
# Definir el esquema de los datos
    data =[
        (67.0, "Male", 208.0, 72.0, 0.0, 0.0, 1.0, 0.0, 0.0, 4.0, "Average", 0.0, 0.0, 9.0, 7.0, 261404.0, 31.0, 286.0, 0.0, 6.0, "Argentina", 0.0,158.0,88.0),
        (21.0, "Male", 389.0, 98.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, "Unhealthy", 1.0, 0.0, 1.0, 5.0, 285768.0, 27.0, 235.0, 1.0, 7.0, "Canada",  0.0,165.0,93.0),
        (21.0, "Female", 324.0, 72.0, 1.0, 0.0, 0.0, 0.0, 0.0, 2.0, "Healthy", 1.0, 1.0, 9.0, 9.0, 235282.0, 28.0, 587.0, 4.0, 4.0, "France",  0.0,174.0,99.0),
        ( 84.0, "Male", 383.0,73.0, 1.0, 1.0, 1.0, 0.0, 1.0, 10.0, "Average", 1.0, 0.0, 9.0, 8.0, 125640.0, 36.0, 378.0, 3.0, 4.0, "Canada",  0.0,163.0,100.0),
        (66.0, "Male", 318.0,93.0, 1.0, 1.0, 1.0, 1.0, 0.0, 6.0, "Unhealthy", 1.0, 0.0, 6.0, 2.0, 160555.0, 22.0, 231.0, 1.0, 5.0, "Thailand", 0.0,91.0,88.0)
    ]


    schema = StructType([   StructField('Age',FloatType()),
                            StructField('Sex',StringType()),
                            StructField('Cholesterol',FloatType()),
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
                            StructField('Heart Attack Risk',FloatType()),
                            StructField('systolic',FloatType()),
                            StructField('diastolic',FloatType())])

    df = spark_session.createDataFrame(data, schema=schema)
    
    
    ### Segundo DataFrame
    schema2 =StructType([StructField('Country',StringType()),
                        StructField('Quality of Life Index',FloatType()),
                        StructField('Purchasing Power Index',FloatType()),
                        StructField('Safety Index',FloatType()),
                        StructField('Health Care Index',FloatType()),
                        StructField('Cost of Living Index',FloatType()),
                        StructField('Property Price to Income Ratio',FloatType()),
                        StructField('Traffic Commute Time Index',FloatType()),
                        StructField('Pollution Index',FloatType()),
                        StructField('Climate Index',FloatType())])
    
    data2 = [
        ("Luxembourg", 200.1, 133.2, 66.0, 77.3, 71.7, 10.1, 25.4, 22.1, 82.6),
        ("Argentina", 110.2, 33.9, 36.0, 68.3, 30.3, 22.3, 44.3, 51.0, 98.3),
        ("France", 153.2, 81.5, 45.4, 78.8, 69.1, 10.8, 35.0, 42.8, 90.3),
        ("Chile", 99.1, 33.9, 41.3, 63.6, 47.7, 16.2, 36.2, 77.2, 90.2),
        ("Thailand", 98.5, 31.7, 61.7, 77.9, 38.5, 25.8, 38.9, 75.4, 69.4)
    ]
    
    df2 = spark_session.createDataFrame(data2, schema=schema2)
    

    on_columns=['Country'] #columnas para el primer join
    df_result=join_dfs(df, df2,on_columns, type_join='left')

    df_result.show()
    
    
    
    
    
    ### Dataset esperado
    expected=[
        (21.0, "Female", 324.0, 72.0, 1.0, 0.0, 0.0, 0.0, 0.0, 2.0, "Healthy", 1.0, 1.0, 9.0, 9.0, 235282.0, 28.0, 587.0, 4.0, 4.0, "France",  0.0,174.0,99.0,153.2, 81.5, 45.4, 78.8, 69.1, 10.8, 35.0, 42.8, 90.3),
        (21.0, "Male", 389.0, 98.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, "Unhealthy", 1.0, 0.0, 1.0, 5.0, 285768.0, 27.0, 235.0, 1.0, 7.0, "Canada",  0.0,165.0,93.0,None,None,None,None,None,None,None,None,None),     
        (66.0, "Male", 318.0,93.0, 1.0, 1.0, 1.0, 1.0, 0.0, 6.0, "Unhealthy", 1.0, 0.0, 6.0, 2.0, 160555.0, 22.0, 231.0, 1.0, 5.0, "Thailand", 0.0,91.0,88.0, 98.5, 31.7, 61.7, 77.9, 38.5, 25.8, 38.9, 75.4, 69.4),
        (67.0, "Male", 208.0, 72.0, 0.0, 0.0, 1.0, 0.0, 0.0, 4.0, "Average", 0.0, 0.0, 9.0, 7.0, 261404.0, 31.0, 286.0, 0.0, 6.0, "Argentina", 0.0,158.0,88.0,110.2, 33.9, 36.0, 68.3, 30.3, 22.3, 44.3, 51.0, 98.3),           
        ( 84.0, "Male", 383.0,73.0, 1.0, 1.0, 1.0, 0.0, 1.0, 10.0, "Average", 1.0, 0.0, 9.0, 8.0, 125640.0, 36.0, 378.0, 3.0, 4.0, "Canada",  0.0,163.0,100.0,None,None,None,None,None,None,None,None,None)        
    ]


    schema = StructType([StructField('Age',FloatType()),
                        StructField('Sex',StringType()),
                        StructField('Cholesterol',FloatType()),
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
                        StructField('Heart Attack Risk',FloatType()),
                        StructField('systolic',FloatType()),
                        StructField('diastolic',FloatType()),
                        StructField('Quality of Life Index',FloatType()),
                        StructField('Purchasing Power Index',FloatType()),
                        StructField('Safety Index',FloatType()),
                        StructField('Health Care Index',FloatType()),
                        StructField('Cost of Living Index',FloatType()),
                        StructField('Property Price to Income Ratio',FloatType()),
                        StructField('Traffic Commute Time Index',FloatType()),
                        StructField('Pollution Index',FloatType()),
                        StructField('Climate Index',FloatType())])

        
    expected_ds=spark_session.createDataFrame(expected, schema=schema)

    assert df_result.collect() == expected_ds.collect()
    
    
#test 8
#probamos hacer un split  en un columna 
def test_Join_inner(spark_session):
# Definir el esquema de los datos
    data =[
        (67.0, "Male", 208.0, 72.0, 0.0, 0.0, 1.0, 0.0, 0.0, 4.0, "Average", 0.0, 0.0, 9.0, 7.0, 261404.0, 31.0, 286.0, 0.0, 6.0, "Argentina", 0.0,158.0,88.0),
        (21.0, "Male", 389.0, 98.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, "Unhealthy", 1.0, 0.0, 1.0, 5.0, 285768.0, 27.0, 235.0, 1.0, 7.0, "Canada",  0.0,165.0,93.0),
        (21.0, "Female", 324.0, 72.0, 1.0, 0.0, 0.0, 0.0, 0.0, 2.0, "Healthy", 1.0, 1.0, 9.0, 9.0, 235282.0, 28.0, 587.0, 4.0, 4.0, "France",  0.0,174.0,99.0),
        ( 84.0, "Male", 383.0,73.0, 1.0, 1.0, 1.0, 0.0, 1.0, 10.0, "Average", 1.0, 0.0, 9.0, 8.0, 125640.0, 36.0, 378.0, 3.0, 4.0, "Canada",  0.0,163.0,100.0),
        (66.0, "Male", 318.0,93.0, 1.0, 1.0, 1.0, 1.0, 0.0, 6.0, "Unhealthy", 1.0, 0.0, 6.0, 2.0, 160555.0, 22.0, 231.0, 1.0, 5.0, "Thailand", 0.0,91.0,88.0)
    ]


    schema = StructType([   StructField('Age',FloatType()),
                            StructField('Sex',StringType()),
                            StructField('Cholesterol',FloatType()),
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
                            StructField('Heart Attack Risk',FloatType()),
                            StructField('systolic',FloatType()),
                            StructField('diastolic',FloatType())])

    df = spark_session.createDataFrame(data, schema=schema)
    
    
    ### Segundo DataFrame
    schema2 =StructType([StructField('Country',StringType()),
                        StructField('Quality of Life Index',FloatType()),
                        StructField('Purchasing Power Index',FloatType()),
                        StructField('Safety Index',FloatType()),
                        StructField('Health Care Index',FloatType()),
                        StructField('Cost of Living Index',FloatType()),
                        StructField('Property Price to Income Ratio',FloatType()),
                        StructField('Traffic Commute Time Index',FloatType()),
                        StructField('Pollution Index',FloatType()),
                        StructField('Climate Index',FloatType())])
    
    data2 = [
        ("Luxembourg", 200.1, 133.2, 66.0, 77.3, 71.7, 10.1, 25.4, 22.1, 82.6),
        ("Argentina", 110.2, 33.9, 36.0, 68.3, 30.3, 22.3, 44.3, 51.0, 98.3),
        ("France", 153.2, 81.5, 45.4, 78.8, 69.1, 10.8, 35.0, 42.8, 90.3),
        ("Chile", 99.1, 33.9, 41.3, 63.6, 47.7, 16.2, 36.2, 77.2, 90.2),
        ("Thailand", 98.5, 31.7, 61.7, 77.9, 38.5, 25.8, 38.9, 75.4, 69.4)
    ]
    
    df2 = spark_session.createDataFrame(data2, schema=schema2)
    

    on_columns=['Country'] #columnas para el primer join
    df_result=join_dfs(df, df2,on_columns, type_join='inner')

    df_result.show()
    
    
    
    
    
    ### Dataset esperado
    expected=[
        (21.0, "Female", 324.0, 72.0, 1.0, 0.0, 0.0, 0.0, 0.0, 2.0, "Healthy", 1.0, 1.0, 9.0, 9.0, 235282.0, 28.0, 587.0, 4.0, 4.0, "France",  0.0,174.0,99.0,153.2, 81.5, 45.4, 78.8, 69.1, 10.8, 35.0, 42.8, 90.3),        
        (66.0, "Male", 318.0,93.0, 1.0, 1.0, 1.0, 1.0, 0.0, 6.0, "Unhealthy", 1.0, 0.0, 6.0, 2.0, 160555.0, 22.0, 231.0, 1.0, 5.0, "Thailand", 0.0,91.0,88.0, 98.5, 31.7, 61.7, 77.9, 38.5, 25.8, 38.9, 75.4, 69.4),
        (67.0, "Male", 208.0, 72.0, 0.0, 0.0, 1.0, 0.0, 0.0, 4.0, "Average", 0.0, 0.0, 9.0, 7.0, 261404.0, 31.0, 286.0, 0.0, 6.0, "Argentina", 0.0,158.0,88.0,110.2, 33.9, 36.0, 68.3, 30.3, 22.3, 44.3, 51.0, 98.3)        
     
        
    ]


    schema = StructType([StructField('Age',FloatType()),
                        StructField('Sex',StringType()),
                        StructField('Cholesterol',FloatType()),
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
                        StructField('Heart Attack Risk',FloatType()),
                        StructField('systolic',FloatType()),
                        StructField('diastolic',FloatType()),
                        StructField('Quality of Life Index',FloatType()),
                        StructField('Purchasing Power Index',FloatType()),
                        StructField('Safety Index',FloatType()),
                        StructField('Health Care Index',FloatType()),
                        StructField('Cost of Living Index',FloatType()),
                        StructField('Property Price to Income Ratio',FloatType()),
                        StructField('Traffic Commute Time Index',FloatType()),
                        StructField('Pollution Index',FloatType()),
                        StructField('Climate Index',FloatType())])

        
    expected_ds=spark_session.createDataFrame(expected, schema=schema)

    assert df_result.collect() == expected_ds.collect()
    
    
   
    
#test 9
#probamos eliminar nullos
def test_drop_null(spark_session):
# Definir el esquema de los datos
    data=[
        (21.0, "Female", 324.0, 72.0, 1.0, 0.0, 0.0, 0.0, 0.0, 2.0, "Healthy", 1.0, 1.0, 9.0, 9.0, 235282.0, 28.0, 587.0, 4.0, 4.0, "France",  0.0,174.0,99.0,153.2, 81.5, 45.4, 78.8, 69.1, 10.8, 35.0, 42.8, 90.3),
        (21.0, "Male", 389.0, 98.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, "Unhealthy", 1.0, 0.0, 1.0, 5.0, 285768.0, 27.0, 235.0, 1.0, 7.0, "Canada",  0.0,165.0,93.0,None,None,None,None,None,None,None,None,None),     
        (66.0, "Male", 318.0,93.0, 1.0, 1.0, 1.0, 1.0, 0.0, 6.0, "Unhealthy", 1.0, 0.0, 6.0, 2.0, 160555.0, 22.0, 231.0, 1.0, 5.0, "Thailand", 0.0,91.0,88.0, 98.5, 31.7, 61.7, 77.9, 38.5, 25.8, 38.9, 75.4, 69.4),
        (67.0, "Male", 208.0, 72.0, 0.0, 0.0, 1.0, 0.0, 0.0, 4.0, "Average", 0.0, 0.0, 9.0, 7.0, 261404.0, 31.0, 286.0, 0.0, 6.0, "Argentina", 0.0,158.0,88.0,110.2, 33.9, 36.0, 68.3, 30.3, 22.3, 44.3, 51.0, 98.3),           
        ( 84.0, "Male", 383.0,73.0, 1.0, 1.0, 1.0, 0.0, 1.0, 10.0, "Average", 1.0, 0.0, 9.0, 8.0, 125640.0, 36.0, 378.0, 3.0, 4.0, "Canada",  0.0,163.0,100.0,None,None,None,None,None,None,None,None,None)        
    ]


    schema = StructType([StructField('Age',FloatType()),
                        StructField('Sex',StringType()),
                        StructField('Cholesterol',FloatType()),
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
                        StructField('Heart Attack Risk',FloatType()),
                        StructField('systolic',FloatType()),
                        StructField('diastolic',FloatType()),
                        StructField('Quality of Life Index',FloatType()),
                        StructField('Purchasing Power Index',FloatType()),
                        StructField('Safety Index',FloatType()),
                        StructField('Health Care Index',FloatType()),
                        StructField('Cost of Living Index',FloatType()),
                        StructField('Property Price to Income Ratio',FloatType()),
                        StructField('Traffic Commute Time Index',FloatType()),
                        StructField('Pollution Index',FloatType()),
                        StructField('Climate Index',FloatType())])

    df = spark_session.createDataFrame(data, schema=schema)
    
    
    df_result=drop_null(df)

    df_result.show()
    
    
    
    
    
    ### Dataset esperado
    expected=[
        (21.0, "Female", 324.0, 72.0, 1.0, 0.0, 0.0, 0.0, 0.0, 2.0, "Healthy", 1.0, 1.0, 9.0, 9.0, 235282.0, 28.0, 587.0, 4.0, 4.0, "France",  0.0,174.0,99.0,153.2, 81.5, 45.4, 78.8, 69.1, 10.8, 35.0, 42.8, 90.3),        
        (66.0, "Male", 318.0,93.0, 1.0, 1.0, 1.0, 1.0, 0.0, 6.0, "Unhealthy", 1.0, 0.0, 6.0, 2.0, 160555.0, 22.0, 231.0, 1.0, 5.0, "Thailand", 0.0,91.0,88.0, 98.5, 31.7, 61.7, 77.9, 38.5, 25.8, 38.9, 75.4, 69.4),
        (67.0, "Male", 208.0, 72.0, 0.0, 0.0, 1.0, 0.0, 0.0, 4.0, "Average", 0.0, 0.0, 9.0, 7.0, 261404.0, 31.0, 286.0, 0.0, 6.0, "Argentina", 0.0,158.0,88.0,110.2, 33.9, 36.0, 68.3, 30.3, 22.3, 44.3, 51.0, 98.3)        
     
        
    ]


    schema = StructType([StructField('Age',FloatType()),
                        StructField('Sex',StringType()),
                        StructField('Cholesterol',FloatType()),
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
                        StructField('Heart Attack Risk',FloatType()),
                        StructField('systolic',FloatType()),
                        StructField('diastolic',FloatType()),
                        StructField('Quality of Life Index',FloatType()),
                        StructField('Purchasing Power Index',FloatType()),
                        StructField('Safety Index',FloatType()),
                        StructField('Health Care Index',FloatType()),
                        StructField('Cost of Living Index',FloatType()),
                        StructField('Property Price to Income Ratio',FloatType()),
                        StructField('Traffic Commute Time Index',FloatType()),
                        StructField('Pollution Index',FloatType()),
                        StructField('Climate Index',FloatType())])

        
    expected_ds=spark_session.createDataFrame(expected, schema=schema)

    assert df_result.collect() == expected_ds.collect()
    
    
    
#test 10
#probamos ordernas las columnas al final 
def test_order_columns(spark_session):
# Definir el esquema de los datos
    data=[
        (21.0, "Female", 324.0, 72.0, 1.0, 0.0, 0.0, 0.0, 0.0, 2.0, "Healthy", 1.0, 1.0, 9.0, 9.0, 235282.0, 28.0, 587.0, 4.0, 4.0, "France",  0.0,174.0,99.0,153.2, 81.5, 45.4, 78.8, 69.1, 10.8, 35.0, 42.8, 90.3),        
        (66.0, "Male", 318.0,93.0, 1.0, 1.0, 1.0, 1.0, 0.0, 6.0, "Unhealthy", 1.0, 0.0, 6.0, 2.0, 160555.0, 22.0, 231.0, 1.0, 5.0, "Thailand", 0.0,91.0,88.0, 98.5, 31.7, 61.7, 77.9, 38.5, 25.8, 38.9, 75.4, 69.4),
        (67.0, "Male", 208.0, 72.0, 0.0, 0.0, 1.0, 0.0, 0.0, 4.0, "Average", 0.0, 0.0, 9.0, 7.0, 261404.0, 31.0, 286.0, 0.0, 6.0, "Argentina", 0.0,158.0,88.0,110.2, 33.9, 36.0, 68.3, 30.3, 22.3, 44.3, 51.0, 98.3)               
    ]


    schema = StructType([StructField('Age',FloatType()),
                        StructField('Sex',StringType()),
                        StructField('Cholesterol',FloatType()),
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
                        StructField('Heart Attack Risk',FloatType()),
                        StructField('systolic',FloatType()),
                        StructField('diastolic',FloatType()),
                        StructField('Quality of Life Index',FloatType()),
                        StructField('Purchasing Power Index',FloatType()),
                        StructField('Safety Index',FloatType()),
                        StructField('Health Care Index',FloatType()),
                        StructField('Cost of Living Index',FloatType()),
                        StructField('Property Price to Income Ratio',FloatType()),
                        StructField('Traffic Commute Time Index',FloatType()),
                        StructField('Pollution Index',FloatType()),
                        StructField('Climate Index',FloatType())])

    df = spark_session.createDataFrame(data, schema=schema)
    
    column_order=['Heart Attack Risk']
    df_result=order_columns(df,column_order)

    df_result.show()
    
    
    
    
    
    ### Dataset esperado
    expected=[
        (21.0, "Female", 324.0, 72.0, 1.0, 0.0, 0.0, 0.0, 0.0, 2.0, "Healthy", 1.0, 1.0, 9.0, 9.0, 235282.0, 28.0, 587.0, 4.0, 4.0, "France", 174.0,99.0,153.2, 81.5, 45.4, 78.8, 69.1, 10.8, 35.0, 42.8, 90.3,0.0),        
        (66.0, "Male", 318.0,93.0, 1.0, 1.0, 1.0, 1.0, 0.0, 6.0, "Unhealthy", 1.0, 0.0, 6.0, 2.0, 160555.0, 22.0, 231.0, 1.0, 5.0, "Thailand",91.0,88.0, 98.5, 31.7, 61.7, 77.9, 38.5, 25.8, 38.9, 75.4, 69.4, 0.0),
        (67.0, "Male", 208.0, 72.0, 0.0, 0.0, 1.0, 0.0, 0.0, 4.0, "Average", 0.0, 0.0, 9.0, 7.0, 261404.0, 31.0, 286.0, 0.0, 6.0, "Argentina",158.0,88.0,110.2, 33.9, 36.0, 68.3, 30.3, 22.3, 44.3, 51.0, 98.3, 0.0)        
     
        
    ]


    schema = StructType([StructField('Age',FloatType()),
                        StructField('Sex',StringType()),
                        StructField('Cholesterol',FloatType()),
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
                        StructField('systolic',FloatType()),
                        StructField('diastolic',FloatType()),
                        StructField('Quality of Life Index',FloatType()),
                        StructField('Purchasing Power Index',FloatType()),
                        StructField('Safety Index',FloatType()),
                        StructField('Health Care Index',FloatType()),
                        StructField('Cost of Living Index',FloatType()),
                        StructField('Property Price to Income Ratio',FloatType()),
                        StructField('Traffic Commute Time Index',FloatType()),
                        StructField('Pollution Index',FloatType()),
                        StructField('Climate Index',FloatType()),
                        StructField('Heart Attack Risk',FloatType())])

        
    expected_ds=spark_session.createDataFrame(expected, schema=schema)

    assert df_result.collect() == expected_ds.collect()