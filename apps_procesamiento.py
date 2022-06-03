#LIBRERIAS DE PYSPARK
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#VARIABLES
env='dev'
BUCKET='juan-ortiz-'+env+'-ntt'

#LEVANTAR SPARK
spark = SparkSession.builder.appName('DataFrame').getOrCreate()

#LECTURA DESDE RUTAS RAW
PATH_APPS="gs://"+BUCKET+"/rawdata/apps/"
PATH_APPS_CATEGORIES="gs://"+BUCKET+"/rawdata/apps_categories/"
PATH_CATEGORIES="gs://"+BUCKET+"/rawdata/categories/"
PATH_KEY_BENEFITS="gs://"+BUCKET+"/rawdata/key_benefits/"
PATH_PRICING_PLAN_FEATURES="gs://"+BUCKET+"/rawdata/pricing_plan_features/"
PATH_PRICING_PLANS="gs://"+BUCKET+"/rawdata/pricing_plans/"
PATH_REVIEWS="gs://"+BUCKET+"/rawdata/reviews/"

#CARGO EN DATASETS LOS CSV LEIDOS DE RAW
APPS = spark.read.load(PATH_APPS, format='csv', sep=",", header="true")
APPS_CATEGORIES = spark.read.load(PATH_APPS_CATEGORIES, format='csv', sep=",", header="true")
CATEGORIES = spark.read.load(PATH_CATEGORIES, format='csv', sep=",", header="true")
KEY_BENEFITS = spark.read.load(PATH_KEY_BENEFITS, format='csv', sep=",", header="true")
PRICING_PLAN_FEATURES = spark.read.load(PATH_PRICING_PLAN_FEATURES, format='csv', sep=",", header="true")
PRICING_PLANS = spark.read.load(PATH_PRICING_PLANS, format='csv', sep=",", header="true")
REVIEWS = spark.read.load(PATH_REVIEWS, format='csv', sep=",", header="true")

#QUITAR DUPLICADOS
APPS.dropDuplicates()
APPS_CATEGORIES.dropDuplicates()
CATEGORIES.dropDuplicates()
KEY_BENEFITS.dropDuplicates()
PRICING_PLAN_FEATURES.dropDuplicates()
PRICING_PLANS.dropDuplicates()
REVIEWS.dropDuplicates()

#CREO VISTAS
APPS.createOrReplaceTempView('APPS')
APPS_CATEGORIES.createOrReplaceTempView('APPS_CATEGORIES')
CATEGORIES.createOrReplaceTempView('CATEGORIES')
KEY_BENEFITS.createOrReplaceTempView('KEY_BENEFITS')
PRICING_PLAN_FEATURES.createOrReplaceTempView('PRICING_PLAN_FEATURES')
PRICING_PLANS.createOrReplaceTempView('PRICING_PLANS')
REVIEWS.createOrReplaceTempView('REVIEWS')

#PROCESAMIENTO
tablon = spark.sql("""
SELECT
ap.title as titulo_app,
ap.developer as desarrollador_app,
ap.rating as clasificacion_app,
ap.reviews_count as contador_opiniones_app,
ap.description as descripcion_app,
ap.tagline as eslogan_app,
ap.pricing_hint as prueba_gratis_app,
cat.title as titulo_categoria,
ben.title as titulo_beneficios,
ben.description as descripcion_beneficios,
pri_plan_fea.feature as caracteristica_precios,
pri_pla.title as titulo_plan_precios,
pri_pla.price as precios,
rev.author as autor_reviews,
rev.rating as clasificacion,
rev.body as comentario,
rev.helpful_count as contador_ayuda 

FROM apps ap

FULL OUTER JOIN apps_categories app_cat
ON app_cat.app_id = ap.id
FULL OUTER JOIN categories cat
ON app_cat.app_id = cat.id

LEFT JOIN key_benefits ben
ON ap.id = ben.app_id

LEFT JOIN pricing_plan_features pri_plan_fea
on ap.id = pri_plan_fea.app_id

LEFT JOIN pricing_plans pri_pla
on ap.id = pri_pla.app_id

LEFT JOIN reviews rev
on ap.id = rev.app_id

""")

#PATH DE GUARDADO
PATH="gs://"+BUCKET+"/trusted/APPS/"

#GUARDADO EN TRUSTED EN FORMATO PARQUET
tablon.write.save(path= PATH, format="parquet", mode='overwrite')