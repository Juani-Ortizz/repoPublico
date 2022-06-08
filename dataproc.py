#!/usr/bin/python
"""BigQuery I/O PySpark example."""
from pyspark.sql import SparkSession

spark = SparkSession \
  .builder \
  .appName('spark-bigquery-demo') \
  .getOrCreate()

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
bucket = "bucket_temporal1403"
spark.conf.set('temporaryGcsBucket', bucket)

# Load data from BigQuery.
APPS = spark.read.load(PATH_APPS, format='csv', sep=",", header="true")
APPS.createOrReplaceTempView('APPS')

APPS_CATEGORIES = spark.read.load(PATH_APPS_CATEGORIES, format='csv', sep=",", header="true")
APPS_CATEGORIES.createOrReplaceTempView('APPS_CATEGORIES')

CATEGORIES = spark.read.load(PATH_CATEGORIES, format='csv', sep=",", header="true")
CATEGORIES.createOrReplaceTempView('CATEGORIES')

KEY_BENEFITS = spark.read.load(PATH_KEY_BENEFITS, format='csv', sep=",", header="true")
KEY_BENEFITS.createOrReplaceTempView('KEY_BENEFITS')

PRICING_PLAN_FEATURES = spark.read.load(PATH_PRICING_PLAN_FEATURES, format='csv', sep=",", header="true")
PRICING_PLAN_FEATURES.createOrReplaceTempView('PRICING_PLAN_FEATURES')

PRICING_PLANS = spark.read.load(PATH_PRICING_PLANS, format='csv', sep=",", header="true")
PRICING_PLANS.createOrReplaceTempView('PRICING_PLANS')

REVIEWS = spark.read.load(PATH_REVIEWS, format='csv', sep=",", header="true")
REVIEWS.createOrReplaceTempView('REVIEWS')


apps = spark.sql(
    'SELECT * FROM apps')
apps.show()
apps.printSchema()

apps_categories = spark.sql(
    'SELECT * FROM apps_categories')
apps_categories.show()
apps_categories.printSchema()

categories = spark.sql(
    'SELECT * FROM categories')
categories.show()
categories.printSchema()

key_benefits = spark.sql(
    'SELECT * FROM key_benefits')
key_benefits.show()
key_benefits.printSchema()

pricing_plan_features = spark.sql(
    'SELECT * FROM pricing_plan_features')
pricing_plan_features.show()
pricing_plan_features.printSchema()

pricing_plans = spark.sql(
    'SELECT * FROM pricing_plans')
pricing_plans.show()
pricing_plans.printSchema()

reviews = spark.sql(
    'SELECT * FROM reviews')
reviews.show()
reviews.printSchema()

# Saving the data to BigQuery
apps.write.format('bigquery') \
  .option('table', 'apps_dataset.apps') \
  .save()

apps_categories.write.format('bigquery') \
  .option('table', 'apps_dataset.apps_categories') \
  .save()

categories.write.format('bigquery') \
  .option('table', 'apps_dataset.categories') \
  .save()

key_benefits.write.format('bigquery') \
  .option('table', 'apps_dataset.key_benefits') \
  .save()

pricing_plan_features.write.format('bigquery') \
  .option('table', 'apps_dataset.pricing_plan_features') \
  .save()

pricing_plans.write.format('bigquery') \
  .option('table', 'apps_dataset.pricing_plans') \
  .save()

reviews.write.format('bigquery') \
  .option('table', 'apps_dataset.reviews') \
  .save()