# Challenge :paperclip:
**Deserializacion de un Json en Databricks**

Se busca cargar un archivo JSON en un DataFrame, seleccionar las columnas: site_id, id, nickname, points y guardar la información deserializada en archivos CSV según los puntos de los vendedores.

> [!TIP]
Crear entorno en Databricks https://community.cloud.databricks.com/login.html

- Usar version "Community Edition"
- Crear un cluster
- En workspace crear una notebook
- **La sesion de Spark se realiza automaticamente**
  
## Desarrollo
Dado el archivo *Sellers.json* , leer la información en un dataframe.
```
df=spark.read.json("/fileStore/tables/json/seller-1.json)
```
> [!NOTE] 
*mi archivo se llamo sellers-1.json, porque lo subi varias veces pero deberia ser seller.json*

Mostrar el esquema original del dataframe para entender como estan orgnizados los datos.
```
df.printSchema()
```
Usar "Select" para acceder a los campos: "site_id", "id", "nickname" y "points" desde la columna "Body"
```
df = df.select(
    col("body.site_id").alias("site_id"),
    col("body.id").alias("id"),
    col("body.nickname").alias("nickname"),
    col("body.points").alias("points")
)
```
Mostar el dataframe resultante con los datos seleccionados
```
df.show(truncate=False)
```
Una vez hecha la deserializacion se crea tres dataframes distintos:
1. para los vendedores con puntos **positivos**
2. para los vendedores con puntos **negativos**
3. para los vendedores con **cero** puntos.

Filtrar segun los puntos
```
positivo_df = df.filter(col("points") > 0)
cero_df = df.filter(col("points") == 0)
negativo_df = df.filter(col("points") < 0)
```
Se guarda la informacion con el siguiente patron: sitio/mes/año/dia/segmento/(archivo).csv

``` 
current_date = datetime.datetime.now()
year = current_date.year
month = current_date.strftime('%m')
day = current_date.strftime('%d')
 ```
Ruta del archivo
``` 
positivo_path = f"/mnt/{year}/{month}/{day}/positivo/sellers.csv"
cero_path = f"/mnt/{year}/{month}/{day}/cero/sellers.csv"
negativo_path = f"/mnt/{year}/{month}/{day}/negativo/sellers.csv"
```

Guardar los dataframes en archivos CSV
```
positivo_df.write.mode("overwrite").csv(positivo_path, header=True)
cero_df.write.mode("overwrite").csv(cero_path, header=True)
negativo_df.write.mode("overwrite").csv(negativo_path, header=True)
``` 


## Documentación
https://docs.databricks.com/en/index.html
https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html
