#LIBRERIAS DE PYSPARK
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#VARIABLES
env='dev'
BUCKETINICIO="xenon-set-352813"
BUCKETFIN='juan-ortiz-'+env+'-ntt'

#LEVANTAR SPARK
spark = SparkSession.builder.appName('DataFrame').getOrCreate()

PATH_DATA="gs://"+BUCKETINICIO+"/data.csv"

#CARGO EN DATASETS LOS CSV LEIDOS DE RAW
DATA = spark.read.load(PATH_DATA, format='csv', sep=",", header="true")

#QUITAR DUPLICADOS
DATA.dropDuplicates()

#CREO VISTAS
DATA.createOrReplaceTempView('DATA')

#PROCESAMIENTO
tablon = spark.sql("""
SELECT
da.title as titulo_app,
da.developer as desarrollador_app,
da.rating as clasificacion_app,
da.reviews_count as contador_opiniones_app,
da.description as descripcion_app,
da.tagline as eslogan_app,
da.pricing_hint as prueba_gratis_app

FROM data da

""")

#PATH DE GUARDADO
PATH="gs://"+BUCKETFIN+"/trusted/DATA/"

#GUARDADO EN TRUSTED EN FORMATO PARQUET
tablon.write.save(path= PATH, format="parquet", mode='overwrite')