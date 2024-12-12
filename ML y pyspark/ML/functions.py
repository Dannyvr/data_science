from pyspark.sql.functions import col, dense_rank, sum, avg, max, min, count,split
from pyspark.sql.window import Window

#resive un datase y seleciona o borra las columnas seleccionadas
#retorna el dataframe las columnas indicadas
def drop_select_col(df, columns,drop_columns=False):
    if drop_columns:
        df_new=df.drop(*columns)
    else:
        df_new=df.select(*columns)
        
    return df_new

# separa una columna en 2 por un limitador
# retorna el nuevo dataset
def split_col(df, column, limit,name1,name2):
    df = df.withColumn(name1, split(df[column],limit).getItem(0)) \
           .withColumn(name2, split(df[column],limit).getItem(1))
           
    df=df.drop(column)
    return df


# Caste las columnas indicadas al tipo indicado, el fro es solo para poder hacerlo en multiples columnas
# retorna el nuevo dataframe
def cast_columns(df, columns_names,new_type):
    for column in columns_names:
        df = df.withColumn(column, col(column).cast(new_type))
    
    return df

    
#une 2 Dataframe por las columnas y el tipo de join indicado, comenzdo por el 1 y 2 y el resultado lo une con el 3
#retorna el dataframe con la union de los 3
def join_dfs(df1, df2,on_columns, type_join='inner'):
    # Realizar el join de 1 y 2
    df_join = df1.join(df2, on_columns, how=type_join)    
    
    # Ordena las columnas en el orden deseado
    column_order = df1.columns + [col for col in df2.columns if col not in df1.columns]
    df_join = df_join.select(*column_order)
   
    
    # Ordena el DataFrame por todas las columnas en orden descendente
    df_join = df_join.orderBy(*[col_name for col_name in df_join.columns])
    #retornar df_join
    return df_join


## elimina las columnas que presentan los nulls de un dataframe    
def drop_null(df):
    df_sin_nulos = df.dropna()
    return df_sin_nulos
    

## neuves las columnas indicas al final del dataframe
def order_columns(df,column_order):
    column_order = [col for col in df.columns if col not in column_order ]+ column_order
    df_order = df.select(*column_order)
       
    return df_order
