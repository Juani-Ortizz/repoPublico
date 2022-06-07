#LIBRERIAS
from email.policy import default
from http import client
import imp
from unittest.mock import DEFAULT
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.datafusion import (
    CloudDataFusionCreateInstanceOperator,
    CloudDataFusionDeleteInstanceOperator,
    CloudDataFusionCreatePipelineOperator,
    CloudDataFusionStartPipelineOperator)
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataProcPySparkOperator,
    DataprocClusterDeleteOperator)
from google.cloud import storage
from datetime import datetime
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash import BashOperator

#VARIABLES
LOCATION = 'us-central1'
client = storage.Client()
PATH = "gs://juan-ortiz-dev-ntt/rawdata/apps/"
INSTANCE_NAME = "instancia1"
SERVICE_ACCOUNT = "443654786848-compute@developer.gserviceaccount.com"
CLUSTER_NAME = "cluster-efimero"
INSTANCE = {
    "type":"BASIC",
    "displayName":INSTANCE_NAME,
    "dataprocServiceAccount":SERVICE_ACCOUNT
}
PYSPARK_JOB_2 = "gs://juan-ortiz-script-procesamiento-test/app/apps_procesamiento.py"
PIPELINE_NAME_1 = "ingesta_apps"
PIPELINE_1 = {
    "artifact": {
        "name": "cdap-data-pipeline",
        "version": "6.6.0",
        "scope": "SYSTEM",
        "label": "Data Pipeline - Batch"
    },
    "description": "",
    "name": "apps",
    "config": {
        "resources": {
            "memoryMB": 2048,
            "virtualCores": 1
        },
        "driverResources": {
            "memoryMB": 2048,
            "virtualCores": 1
        },
        "connections": [
            {
                "from": "CloudSQL PostgreSQL",
                "to": "GCS"
            }
        ],
        "comments": [],
        "postActions": [],
        "properties": {},
        "processTimingEnabled": "true",
        "stageLoggingEnabled": "false",
        "stages": [
            {
                "name": "CloudSQL PostgreSQL",
                "plugin": {
                    "name": "CloudSQLPostgreSQL",
                    "type": "batchsource",
                    "label": "CloudSQL PostgreSQL",
                    "artifact": {
                        "name": "cloudsql-postgresql-plugin",
                        "version": "1.7.0",
                        "scope": "USER"
                    },
                    "properties": {
                        "referenceName": "apps",
                        "jdbcPluginName": "cloudsql-postgresql",
                        "database": "csv-juor",
                        "user": "postgres",
                        "password": "1234",
                        "instanceType": "public",
                        "connectionName": "entrega-350514:us-central1:db-ntt",
                        "importQuery": "select * from apps;",
                        "numSplits": "1",
                        "fetchSize": "1000",
                        "schema": "{\"type\":\"record\",\"name\":\"etlSchemaBody\",\"fields\":[{\"name\":\"id\",\"type\":[\"string\",\"null\"]},{\"name\":\"url\",\"type\":[\"string\",\"null\"]},{\"name\":\"title\",\"type\":[\"string\",\"null\"]},{\"name\":\"developer\",\"type\":[\"string\",\"null\"]},{\"name\":\"developer_link\",\"type\":[\"string\",\"null\"]},{\"name\":\"icon\",\"type\":[\"string\",\"null\"]},{\"name\":\"rating\",\"type\":[\"string\",\"null\"]},{\"name\":\"reviews_count\",\"type\":[\"string\",\"null\"]},{\"name\":\"description_raw\",\"type\":[\"string\",\"null\"]},{\"name\":\"description\",\"type\":[\"string\",\"null\"]},{\"name\":\"tagline\",\"type\":[\"string\",\"null\"]},{\"name\":\"pricing_hint\",\"type\":[\"string\",\"null\"]}]}"
                    }
                },
                "outputSchema": [
                    {
                        "name": "etlSchemaBody",
                        "schema": "{\"type\":\"record\",\"name\":\"etlSchemaBody\",\"fields\":[{\"name\":\"id\",\"type\":[\"string\",\"null\"]},{\"name\":\"url\",\"type\":[\"string\",\"null\"]},{\"name\":\"title\",\"type\":[\"string\",\"null\"]},{\"name\":\"developer\",\"type\":[\"string\",\"null\"]},{\"name\":\"developer_link\",\"type\":[\"string\",\"null\"]},{\"name\":\"icon\",\"type\":[\"string\",\"null\"]},{\"name\":\"rating\",\"type\":[\"string\",\"null\"]},{\"name\":\"reviews_count\",\"type\":[\"string\",\"null\"]},{\"name\":\"description_raw\",\"type\":[\"string\",\"null\"]},{\"name\":\"description\",\"type\":[\"string\",\"null\"]},{\"name\":\"tagline\",\"type\":[\"string\",\"null\"]},{\"name\":\"pricing_hint\",\"type\":[\"string\",\"null\"]}]}"
                    }
                ],
                "id": "CloudSQL-PostgreSQL"
            },
            {
                "name": "GCS",
                "plugin": {
                    "name": "GCS",
                    "type": "batchsink",
                    "label": "GCS",
                    "artifact": {
                        "name": "google-cloud",
                        "version": "0.19.0",
                        "scope": "SYSTEM"
                    },
                    "properties": {
                        "referenceName": "apps",
                        "project": "auto-detect",
                        "path": "gs://juan-ortiz-dev-ntt/rawdata/apps/",
                        "format": "csv",
                        "serviceAccountType": "filePath",
                        "serviceFilePath": "auto-detect",
                        "location": "us",
                        "contentType": "text/csv",
                        "schema": "{\"type\":\"record\",\"name\":\"outputSchema\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"url\",\"type\":[\"string\",\"null\"]},{\"name\":\"title\",\"type\":[\"string\",\"null\"]},{\"name\":\"developer\",\"type\":[\"string\",\"null\"]},{\"name\":\"developer_link\",\"type\":[\"string\",\"null\"]},{\"name\":\"icon\",\"type\":[\"string\",\"null\"]},{\"name\":\"rating\",\"type\":[\"string\",\"null\"]},{\"name\":\"reviews_count\",\"type\":[\"string\",\"null\"]},{\"name\":\"description_raw\",\"type\":[\"string\",\"null\"]},{\"name\":\"description\",\"type\":[\"string\",\"null\"]},{\"name\":\"tagline\",\"type\":[\"string\",\"null\"]},{\"name\":\"pricing_hint\",\"type\":[\"string\",\"null\"]}]}",
                        "writeHeader": "true"
                    }
                },
                "outputSchema": "{\"type\":\"record\",\"name\":\"outputSchema\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"url\",\"type\":[\"string\",\"null\"]},{\"name\":\"title\",\"type\":[\"string\",\"null\"]},{\"name\":\"developer\",\"type\":[\"string\",\"null\"]},{\"name\":\"developer_link\",\"type\":[\"string\",\"null\"]},{\"name\":\"icon\",\"type\":[\"string\",\"null\"]},{\"name\":\"rating\",\"type\":[\"string\",\"null\"]},{\"name\":\"reviews_count\",\"type\":[\"string\",\"null\"]},{\"name\":\"description_raw\",\"type\":[\"string\",\"null\"]},{\"name\":\"description\",\"type\":[\"string\",\"null\"]},{\"name\":\"tagline\",\"type\":[\"string\",\"null\"]},{\"name\":\"pricing_hint\",\"type\":[\"string\",\"null\"]}]}",
                "inputSchema": "{\"type\":\"record\",\"name\":\"outputSchema\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"url\",\"type\":[\"string\",\"null\"]},{\"name\":\"title\",\"type\":[\"string\",\"null\"]},{\"name\":\"developer\",\"type\":[\"string\",\"null\"]},{\"name\":\"developer_link\",\"type\":[\"string\",\"null\"]},{\"name\":\"icon\",\"type\":[\"string\",\"null\"]},{\"name\":\"rating\",\"type\":[\"string\",\"null\"]},{\"name\":\"reviews_count\",\"type\":[\"string\",\"null\"]},{\"name\":\"description_raw\",\"type\":[\"string\",\"null\"]},{\"name\":\"description\",\"type\":[\"string\",\"null\"]},{\"name\":\"tagline\",\"type\":[\"string\",\"null\"]},{\"name\":\"pricing_hint\",\"type\":[\"string\",\"null\"]}]}",
                "id": "GCS"
            }
        ],
        "schedule": "0 1 */1 * *",
        "engine": "spark",
        "numOfRecordsPreview": 100,
        "rangeRecordsPreview": {
            "min": 1,
            "max": "5000"
        },
        "maxConcurrentRuns": 1
    }
}

PIPELINE_NAME_2 = "ingesta_apps_categories"
PIPELINE_2 = {
    "artifact": {
        "name": "cdap-data-pipeline",
        "version": "6.6.0",
        "scope": "SYSTEM",
        "label": "Data Pipeline - Batch"
    },
    "description": "",
    "name": "apps_categories",
    "config": {
        "resources": {
            "memoryMB": 2048,
            "virtualCores": 1
        },
        "driverResources": {
            "memoryMB": 2048,
            "virtualCores": 1
        },
        "connections": [
            {
                "from": "CloudSQL PostgreSQL",
                "to": "GCS"
            }
        ],
        "comments": [],
        "postActions": [],
        "properties": {},
        "processTimingEnabled": "true",
        "stageLoggingEnabled": "false",
        "stages": [
            {
                "name": "CloudSQL PostgreSQL",
                "plugin": {
                    "name": "CloudSQLPostgreSQL",
                    "type": "batchsource",
                    "label": "CloudSQL PostgreSQL",
                    "artifact": {
                        "name": "cloudsql-postgresql-plugin",
                        "version": "1.7.0",
                        "scope": "USER"
                    },
                    "properties": {
                        "referenceName": "apps_categories",
                        "jdbcPluginName": "cloudsql-postgresql",
                        "database": "csv-juor",
                        "user": "postgres",
                        "password": "1234",
                        "instanceType": "public",
                        "connectionName": "entrega-350514:us-central1:db-ntt",
                        "importQuery": "select * from apps_categories;",
                        "numSplits": "1",
                        "fetchSize": "1000",
                        "schema": "{\"name\":\"etlSchemaBody\",\"type\":\"record\",\"fields\":[{\"name\":\"app_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"category_id\",\"type\":[\"string\",\"null\"]}]}"
                    }
                },
                "outputSchema": [
                    {
                        "name": "etlSchemaBody",
                        "schema": "{\"name\":\"etlSchemaBody\",\"type\":\"record\",\"fields\":[{\"name\":\"app_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"category_id\",\"type\":[\"string\",\"null\"]}]}"
                    }
                ],
                "id": "CloudSQL-PostgreSQL"
            },
            {
                "name": "GCS",
                "plugin": {
                    "name": "GCS",
                    "type": "batchsink",
                    "label": "GCS",
                    "artifact": {
                        "name": "google-cloud",
                        "version": "0.19.0",
                        "scope": "SYSTEM"
                    },
                    "properties": {
                        "referenceName": "apps_categories",
                        "project": "auto-detect",
                        "path": "juan-ortiz-dev-ntt/rawdata/apps_categories/",
                        "format": "csv",
                        "serviceAccountType": "filePath",
                        "serviceFilePath": "auto-detect",
                        "location": "us",
                        "contentType": "text/csv",
                        "schema": "{\"name\":\"etlSchemaBody\",\"type\":\"record\",\"fields\":[{\"name\":\"app_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"category_id\",\"type\":[\"string\",\"null\"]}]}",
                        "writeHeader": "true"
                    }
                },
                "outputSchema": "{\"name\":\"etlSchemaBody\",\"type\":\"record\",\"fields\":[{\"name\":\"app_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"category_id\",\"type\":[\"string\",\"null\"]}]}",
                "inputSchema": "{\"name\":\"etlSchemaBody\",\"type\":\"record\",\"fields\":[{\"name\":\"app_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"category_id\",\"type\":[\"string\",\"null\"]}]}",
                "id": "GCS"
            }
        ],
        "schedule": "0 1 */1 * *",
        "engine": "spark",
        "numOfRecordsPreview": 100,
        "rangeRecordsPreview": {
            "min": 1,
            "max": "5000"
        },
        "maxConcurrentRuns": 1
    }
}

PIPELINE_NAME_3 = "ingesta_categories"
PIPELINE_3 = {
    "artifact": {
        "name": "cdap-data-pipeline",
        "version": "6.6.0",
        "scope": "SYSTEM",
        "label": "Data Pipeline - Batch"
    },
    "description": "",
    "name": "categories",
    "config": {
        "resources": {
            "memoryMB": 2048,
            "virtualCores": 1
        },
        "driverResources": {
            "memoryMB": 2048,
            "virtualCores": 1
        },
        "connections": [],
        "comments": [],
        "postActions": [],
        "properties": {},
        "processTimingEnabled": "true",
        "stageLoggingEnabled": "false",
        "stages": [
            {
                "name": "CloudSQL PostgreSQL",
                "plugin": {
                    "name": "CloudSQLPostgreSQL",
                    "type": "batchsource",
                    "label": "CloudSQL PostgreSQL",
                    "artifact": {
                        "name": "cloudsql-postgresql-plugin",
                        "version": "1.7.0",
                        "scope": "USER"
                    },
                    "properties": {
                        "referenceName": "categories",
                        "jdbcPluginName": "cloudsql-postgresql",
                        "database": "csv-juor",
                        "user": "postgres",
                        "password": "1234",
                        "instanceType": "public",
                        "connectionName": "entrega-350514:us-central1:db-ntt",
                        "importQuery": "select * from categories;",
                        "numSplits": "1",
                        "fetchSize": "1000",
                        "schema": "{\"type\":\"record\",\"name\":\"etlSchemaBody\",\"fields\":[{\"name\":\"id\",\"type\":[\"string\",\"null\"]},{\"name\":\"title\",\"type\":[\"string\",\"null\"]}]}"
                    }
                },
                "outputSchema": "{\"type\":\"record\",\"name\":\"etlSchemaBody\",\"fields\":[{\"name\":\"id\",\"type\":[\"string\",\"null\"]},{\"name\":\"title\",\"type\":[\"string\",\"null\"]}]}",
                "id": "CloudSQL-PostgreSQL"
            },
            {
                "name": "GCS",
                "plugin": {
                    "name": "GCS",
                    "type": "batchsink",
                    "label": "GCS",
                    "artifact": {
                        "name": "google-cloud",
                        "version": "0.19.0",
                        "scope": "SYSTEM"
                    },
                    "properties": {
                        "referenceName": "categories",
                        "project": "auto-detect",
                        "path": "juan-ortiz-dev-ntt/rawdata/categories/",
                        "format": "csv",
                        "serviceAccountType": "filePath",
                        "serviceFilePath": "auto-detect",
                        "location": "us",
                        "contentType": "text/csv",
                        "writeHeader": "true"
                    }
                },
                "outputSchema": [
                    {
                        "name": "etlSchemaBody",
                        "schema": ""
                    }
                ],
                "inputSchema": [],
                "id": "GCS"
            }
        ],
        "schedule": "0 1 */1 * *",
        "engine": "spark",
        "numOfRecordsPreview": 100,
        "rangeRecordsPreview": {
            "min": 1,
            "max": "5000"
        },
        "maxConcurrentRuns": 1
    }
}

PIPELINE_NAME_4 = "ingesta_key_benefits"
PIPELINE_4 = {
    "artifact": {
        "name": "cdap-data-pipeline",
        "version": "6.6.0",
        "scope": "SYSTEM",
        "label": "Data Pipeline - Batch"
    },
    "description": "",
    "name": "key_benefits",
    "config": {
        "resources": {
            "memoryMB": 2048,
            "virtualCores": 1
        },
        "driverResources": {
            "memoryMB": 2048,
            "virtualCores": 1
        },
        "connections": [
            {
                "from": "CloudSQL PostgreSQL",
                "to": "GCS"
            }
        ],
        "comments": [],
        "postActions": [],
        "properties": {},
        "processTimingEnabled": "true",
        "stageLoggingEnabled": "false",
        "stages": [
            {
                "name": "CloudSQL PostgreSQL",
                "plugin": {
                    "name": "CloudSQLPostgreSQL",
                    "type": "batchsource",
                    "label": "CloudSQL PostgreSQL",
                    "artifact": {
                        "name": "cloudsql-postgresql-plugin",
                        "version": "1.7.0",
                        "scope": "USER"
                    },
                    "properties": {
                        "referenceName": "key_benefits",
                        "jdbcPluginName": "cloudsql-postgresql",
                        "database": "csv-juor",
                        "user": "postgres",
                        "password": "1234",
                        "instanceType": "public",
                        "connectionName": "entrega-350514:us-central1:db-ntt",
                        "importQuery": "select * from key_benefits;",
                        "numSplits": "1",
                        "fetchSize": "1000",
                        "schema": "{\"type\":\"record\",\"name\":\"etlSchemaBody\",\"fields\":[{\"name\":\"app_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"title\",\"type\":[\"string\",\"null\"]},{\"name\":\"description\",\"type\":[\"string\",\"null\"]}]}"
                    }
                },
                "outputSchema": [
                    {
                        "name": "etlSchemaBody",
                        "schema": "{\"type\":\"record\",\"name\":\"etlSchemaBody\",\"fields\":[{\"name\":\"app_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"title\",\"type\":[\"string\",\"null\"]},{\"name\":\"description\",\"type\":[\"string\",\"null\"]}]}"
                    }
                ],
                "id": "CloudSQL-PostgreSQL"
            },
            {
                "name": "GCS",
                "plugin": {
                    "name": "GCS",
                    "type": "batchsink",
                    "label": "GCS",
                    "artifact": {
                        "name": "google-cloud",
                        "version": "0.19.0",
                        "scope": "SYSTEM"
                    },
                    "properties": {
                        "referenceName": "key_benefits",
                        "project": "auto-detect",
                        "path": "juan-ortiz-dev-ntt/rawdata/key_benefits/",
                        "format": "csv",
                        "serviceAccountType": "filePath",
                        "serviceFilePath": "auto-detect",
                        "location": "us",
                        "contentType": "text/csv",
                        "schema": "{\"type\":\"record\",\"name\":\"etlSchemaBody\",\"fields\":[{\"name\":\"app_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"title\",\"type\":[\"string\",\"null\"]},{\"name\":\"description\",\"type\":[\"string\",\"null\"]}]}",
                        "writeHeader": "true"
                    }
                },
                "outputSchema": "{\"type\":\"record\",\"name\":\"etlSchemaBody\",\"fields\":[{\"name\":\"app_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"title\",\"type\":[\"string\",\"null\"]},{\"name\":\"description\",\"type\":[\"string\",\"null\"]}]}",
                "inputSchema": "{\"type\":\"record\",\"name\":\"etlSchemaBody\",\"fields\":[{\"name\":\"app_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"title\",\"type\":[\"string\",\"null\"]},{\"name\":\"description\",\"type\":[\"string\",\"null\"]}]}",
                "id": "GCS"
            }
        ],
        "schedule": "0 1 */1 * *",
        "engine": "spark",
        "numOfRecordsPreview": 100,
        "rangeRecordsPreview": {
            "min": 1,
            "max": "5000"
        },
        "maxConcurrentRuns": 1
    }
}

PIPELINE_NAME_5 = "ingesta_pricing_plan_features"
PIPELINE_5 = {
    "artifact": {
        "name": "cdap-data-pipeline",
        "version": "6.6.0",
        "scope": "SYSTEM",
        "label": "Data Pipeline - Batch"
    },
    "description": "",
    "name": "pricing_plan_features",
    "config": {
        "resources": {
            "memoryMB": 2048,
            "virtualCores": 1
        },
        "driverResources": {
            "memoryMB": 2048,
            "virtualCores": 1
        },
        "connections": [
            {
                "from": "CloudSQL PostgreSQL",
                "to": "GCS"
            }
        ],
        "comments": [],
        "postActions": [],
        "properties": {},
        "processTimingEnabled": "true",
        "stageLoggingEnabled": "false",
        "stages": [
            {
                "name": "CloudSQL PostgreSQL",
                "plugin": {
                    "name": "CloudSQLPostgreSQL",
                    "type": "batchsource",
                    "label": "CloudSQL PostgreSQL",
                    "artifact": {
                        "name": "cloudsql-postgresql-plugin",
                        "version": "1.7.0",
                        "scope": "USER"
                    },
                    "properties": {
                        "referenceName": "pricing_plan_features",
                        "jdbcPluginName": "cloudsql-postgresql",
                        "database": "csv-juor",
                        "user": "postgres",
                        "password": "1234",
                        "instanceType": "public",
                        "connectionName": "entrega-350514:us-central1:db-ntt",
                        "importQuery": "select * from pricing_plan_features;",
                        "numSplits": "1",
                        "fetchSize": "1000",
                        "schema": "{\"type\":\"record\",\"name\":\"outputSchema\",\"fields\":[{\"name\":\"app_id\",\"type\":\"string\"},{\"name\":\"pricing_plan_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"feature\",\"type\":[\"string\",\"null\"]}]}"
                    }
                },
                "outputSchema": [
                    {
                        "name": "etlSchemaBody",
                        "schema": "{\"type\":\"record\",\"name\":\"outputSchema\",\"fields\":[{\"name\":\"app_id\",\"type\":\"string\"},{\"name\":\"pricing_plan_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"feature\",\"type\":[\"string\",\"null\"]}]}"
                    }
                ],
                "id": "CloudSQL-PostgreSQL"
            },
            {
                "name": "GCS",
                "plugin": {
                    "name": "GCS",
                    "type": "batchsink",
                    "label": "GCS",
                    "artifact": {
                        "name": "google-cloud",
                        "version": "0.19.0",
                        "scope": "SYSTEM"
                    },
                    "properties": {
                        "referenceName": "pricing_plan_features",
                        "project": "auto-detect",
                        "path": "juan-ortiz-dev-ntt/rawdata/pricing_plan_features/",
                        "format": "csv",
                        "serviceAccountType": "filePath",
                        "serviceFilePath": "auto-detect",
                        "location": "us",
                        "contentType": "text/csv",
                        "schema": "{\"type\":\"record\",\"name\":\"outputSchema\",\"fields\":[{\"name\":\"app_id\",\"type\":\"string\"},{\"name\":\"pricing_plan_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"feature\",\"type\":[\"string\",\"null\"]}]}",
                        "writeHeader": "true"
                    }
                },
                "outputSchema": "{\"type\":\"record\",\"name\":\"outputSchema\",\"fields\":[{\"name\":\"app_id\",\"type\":\"string\"},{\"name\":\"pricing_plan_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"feature\",\"type\":[\"string\",\"null\"]}]}",
                "inputSchema": "{\"type\":\"record\",\"name\":\"outputSchema\",\"fields\":[{\"name\":\"app_id\",\"type\":\"string\"},{\"name\":\"pricing_plan_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"feature\",\"type\":[\"string\",\"null\"]}]}",
                "id": "GCS"
            }
        ],
        "schedule": "0 1 */1 * *",
        "engine": "spark",
        "numOfRecordsPreview": 100,
        "rangeRecordsPreview": {
            "min": 1,
            "max": "5000"
        },
        "maxConcurrentRuns": 1
    }
}

PIPELINE_NAME_6 = "ingesta_pricing_plans"
PIPELINE_6 = {
    "artifact": {
        "name": "cdap-data-pipeline",
        "version": "6.6.0",
        "scope": "SYSTEM",
        "label": "Data Pipeline - Batch"
    },
    "description": "",
    "name": "pricing_plans",
    "config": {
        "resources": {
            "memoryMB": 2048,
            "virtualCores": 1
        },
        "driverResources": {
            "memoryMB": 2048,
            "virtualCores": 1
        },
        "connections": [
            {
                "from": "CloudSQL PostgreSQL",
                "to": "GCS"
            }
        ],
        "comments": [],
        "postActions": [],
        "properties": {},
        "processTimingEnabled": "true",
        "stageLoggingEnabled": "false",
        "stages": [
            {
                "name": "CloudSQL PostgreSQL",
                "plugin": {
                    "name": "CloudSQLPostgreSQL",
                    "type": "batchsource",
                    "label": "CloudSQL PostgreSQL",
                    "artifact": {
                        "name": "cloudsql-postgresql-plugin",
                        "version": "1.7.0",
                        "scope": "USER"
                    },
                    "properties": {
                        "referenceName": "pricing_plans",
                        "jdbcPluginName": "cloudsql-postgresql",
                        "database": "csv-juor",
                        "user": "postgres",
                        "password": "1234",
                        "instanceType": "public",
                        "connectionName": "entrega-350514:us-central1:db-ntt",
                        "importQuery": "select * from pricing_plans;",
                        "numSplits": "1",
                        "fetchSize": "1000",
                        "schema": "{\"type\":\"record\",\"name\":\"outputSchema\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"app_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"title\",\"type\":[\"string\",\"null\"]},{\"name\":\"price\",\"type\":[\"string\",\"null\"]}]}"
                    }
                },
                "outputSchema": [
                    {
                        "name": "etlSchemaBody",
                        "schema": "{\"type\":\"record\",\"name\":\"outputSchema\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"app_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"title\",\"type\":[\"string\",\"null\"]},{\"name\":\"price\",\"type\":[\"string\",\"null\"]}]}"
                    }
                ],
                "id": "CloudSQL-PostgreSQL"
            },
            {
                "name": "GCS",
                "plugin": {
                    "name": "GCS",
                    "type": "batchsink",
                    "label": "GCS",
                    "artifact": {
                        "name": "google-cloud",
                        "version": "0.19.0",
                        "scope": "SYSTEM"
                    },
                    "properties": {
                        "referenceName": "pricing_plans",
                        "project": "auto-detect",
                        "path": "juan-ortiz-dev-ntt/rawdata/pricing_plans/",
                        "format": "csv",
                        "serviceAccountType": "filePath",
                        "serviceFilePath": "auto-detect",
                        "location": "us",
                        "contentType": "text/csv",
                        "schema": "{\"type\":\"record\",\"name\":\"outputSchema\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"app_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"title\",\"type\":[\"string\",\"null\"]},{\"name\":\"price\",\"type\":[\"string\",\"null\"]}]}",
                        "writeHeader": "true"
                    }
                },
                "outputSchema": "{\"type\":\"record\",\"name\":\"outputSchema\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"app_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"title\",\"type\":[\"string\",\"null\"]},{\"name\":\"price\",\"type\":[\"string\",\"null\"]}]}",
                "inputSchema": "{\"type\":\"record\",\"name\":\"outputSchema\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"app_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"title\",\"type\":[\"string\",\"null\"]},{\"name\":\"price\",\"type\":[\"string\",\"null\"]}]}",
                "id": "GCS"
            }
        ],
        "schedule": "0 1 */1 * *",
        "engine": "spark",
        "numOfRecordsPreview": 100,
        "rangeRecordsPreview": {
            "min": 1,
            "max": "5000"
        },
        "maxConcurrentRuns": 1
    }
}

PIPELINE_NAME_7 = "ingesta_reviews"
PIPELINE_7 = {
    "artifact": {
        "name": "cdap-data-pipeline",
        "version": "6.6.0",
        "scope": "SYSTEM",
        "label": "Data Pipeline - Batch"
    },
    "description": "",
    "name": "reviews",
    "config": {
        "resources": {
            "memoryMB": 2048,
            "virtualCores": 1
        },
        "driverResources": {
            "memoryMB": 2048,
            "virtualCores": 1
        },
        "connections": [
            {
                "from": "CloudSQL PostgreSQL",
                "to": "GCS"
            }
        ],
        "comments": [],
        "postActions": [],
        "properties": {},
        "processTimingEnabled": "true",
        "stageLoggingEnabled": "false",
        "stages": [
            {
                "name": "CloudSQL PostgreSQL",
                "plugin": {
                    "name": "CloudSQLPostgreSQL",
                    "type": "batchsource",
                    "label": "CloudSQL PostgreSQL",
                    "artifact": {
                        "name": "cloudsql-postgresql-plugin",
                        "version": "1.7.0",
                        "scope": "USER"
                    },
                    "properties": {
                        "referenceName": "reviews",
                        "jdbcPluginName": "cloudsql-postgresql",
                        "database": "csv-juor",
                        "user": "postgres",
                        "password": "1234",
                        "instanceType": "public",
                        "connectionName": "entrega-350514:us-central1:db-ntt",
                        "importQuery": "select * from reviews;",
                        "numSplits": "1",
                        "fetchSize": "1000",
                        "schema": "{\"type\":\"record\",\"name\":\"outputSchema\",\"fields\":[{\"name\":\"app_id\",\"type\":\"string\"},{\"name\":\"author\",\"type\":[\"string\",\"null\"]},{\"name\":\"rating\",\"type\":[\"string\",\"null\"]},{\"name\":\"posted_at\",\"type\":[\"string\",\"null\"]},{\"name\":\"body\",\"type\":[\"string\",\"null\"]},{\"name\":\"helpful_count\",\"type\":[\"string\",\"null\"]},{\"name\":\"developer_reply\",\"type\":[\"string\",\"null\"]},{\"name\":\"veloper_reply_posted_at\",\"type\":[\"string\",\"null\"]}]}"
                    }
                },
                "outputSchema": [
                    {
                        "name": "etlSchemaBody",
                        "schema": "{\"type\":\"record\",\"name\":\"outputSchema\",\"fields\":[{\"name\":\"app_id\",\"type\":\"string\"},{\"name\":\"author\",\"type\":[\"string\",\"null\"]},{\"name\":\"rating\",\"type\":[\"string\",\"null\"]},{\"name\":\"posted_at\",\"type\":[\"string\",\"null\"]},{\"name\":\"body\",\"type\":[\"string\",\"null\"]},{\"name\":\"helpful_count\",\"type\":[\"string\",\"null\"]},{\"name\":\"developer_reply\",\"type\":[\"string\",\"null\"]},{\"name\":\"veloper_reply_posted_at\",\"type\":[\"string\",\"null\"]}]}"
                    }
                ],
                "id": "CloudSQL-PostgreSQL"
            },
            {
                "name": "GCS",
                "plugin": {
                    "name": "GCS",
                    "type": "batchsink",
                    "label": "GCS",
                    "artifact": {
                        "name": "google-cloud",
                        "version": "0.19.0",
                        "scope": "SYSTEM"
                    },
                    "properties": {
                        "referenceName": "reviews",
                        "project": "auto-detect",
                        "path": "juan-ortiz-dev-ntt/rawdata/reviews/",
                        "format": "csv",
                        "serviceAccountType": "filePath",
                        "serviceFilePath": "auto-detect",
                        "location": "us",
                        "contentType": "text/csv",
                        "schema": "{\"type\":\"record\",\"name\":\"outputSchema\",\"fields\":[{\"name\":\"app_id\",\"type\":\"string\"},{\"name\":\"author\",\"type\":[\"string\",\"null\"]},{\"name\":\"rating\",\"type\":[\"string\",\"null\"]},{\"name\":\"posted_at\",\"type\":[\"string\",\"null\"]},{\"name\":\"body\",\"type\":[\"string\",\"null\"]},{\"name\":\"helpful_count\",\"type\":[\"string\",\"null\"]},{\"name\":\"developer_reply\",\"type\":[\"string\",\"null\"]},{\"name\":\"veloper_reply_posted_at\",\"type\":[\"string\",\"null\"]}]}",
                        "writeHeader": "true"
                    }
                },
                "outputSchema": "{\"type\":\"record\",\"name\":\"outputSchema\",\"fields\":[{\"name\":\"app_id\",\"type\":\"string\"},{\"name\":\"author\",\"type\":[\"string\",\"null\"]},{\"name\":\"rating\",\"type\":[\"string\",\"null\"]},{\"name\":\"posted_at\",\"type\":[\"string\",\"null\"]},{\"name\":\"body\",\"type\":[\"string\",\"null\"]},{\"name\":\"helpful_count\",\"type\":[\"string\",\"null\"]},{\"name\":\"developer_reply\",\"type\":[\"string\",\"null\"]},{\"name\":\"veloper_reply_posted_at\",\"type\":[\"string\",\"null\"]}]}",
                "inputSchema": "{\"type\":\"record\",\"name\":\"outputSchema\",\"fields\":[{\"name\":\"app_id\",\"type\":\"string\"},{\"name\":\"author\",\"type\":[\"string\",\"null\"]},{\"name\":\"rating\",\"type\":[\"string\",\"null\"]},{\"name\":\"posted_at\",\"type\":[\"string\",\"null\"]},{\"name\":\"body\",\"type\":[\"string\",\"null\"]},{\"name\":\"helpful_count\",\"type\":[\"string\",\"null\"]},{\"name\":\"developer_reply\",\"type\":[\"string\",\"null\"]},{\"name\":\"veloper_reply_posted_at\",\"type\":[\"string\",\"null\"]}]}",
                "id": "GCS"
            }
        ],
        "schedule": "0 1 */1 * *",
        "engine": "spark",
        "numOfRecordsPreview": 100,
        "rangeRecordsPreview": {
            "min": 1,
            "max": "5000"
        },
        "maxConcurrentRuns": 1
    }
}

#PARAMETROS DEL DAG
DEFAULT_DAG_ARGS = {
    'owner':"airflow",
    'start_date':datetime.utcnow(),
    'scheduled_interval':None,
    'project_id':client.project
}
with DAG('dag_pipeline', 
    schedule_interval = '0 3 * * sun',
    start_date=datetime(2022, 1, 19, 4, 00),
    end_date=None,
    carchup=False,
    default_args=DEFAULT_DAG_ARGS) as dag:
   
    inicio = DummyOperator(
        task_id = 'inicio'
    )

    fin = DummyOperator(
        task_id = 'fin'
    )

    create_instance = CloudDataFusionCreateInstanceOperator(
        location = LOCATION,
        instance_name = INSTANCE_NAME,
        instance = INSTANCE,
        task_id = "create_instance"
    )

    create_cluster = DataprocClusterCreateOperator(
        task_id = 'create_dataproc_cluster_2',
        cluster_name = CLUSTER_NAME,
        master_machine_type="n1-standard-4",
        master_disk_type='pd-ssd',
        master_disk_size=100,
        num_workers=0,
        num_masters=1,
        region=LOCATION,
        zone='us-central1-a'
    )    
    
    delete_instance = CloudDataFusionDeleteInstanceOperator(
        location=LOCATION,
        instance_name=INSTANCE_NAME,
        task_id="delete_instance"
    )

    apps_process = DataProcPySparkOperator(
        task_id='apps_process',
        main = PYSPARK_JOB_2,
        cluster_name = CLUSTER_NAME,
        region = LOCATION
    )

    delete_cluster = DataprocClusterDeleteOperator(
        task_id = 'delete_cluster',
        cluster_name = CLUSTER_NAME,
        region= LOCATION
    )

    set_profile = DataProcPySparkOperator(
        task_id = 'set_profile',
        main = "gs://juan-ortiz-script-procesamiento-test/set_profile.py",
        cluster_name = CLUSTER_NAME,
        region = LOCATION
    )

    pipeline = TriggerDagRunOperator(
        task_id="pipeline_app_ingesta",
        trigger_dag_id="dag_pipeline"
    )

#     #APPS
#     create_pipeline_apps = CloudDataFusionCreatePipelineOperator(
#         location=LOCATION,
#         pipeline_name= PIPELINE_NAME_1,
#         pipeline=PIPELINE_1,
#         instance_name=INSTANCE_NAME,
#         task_id="apps"
#     )
#     delete_apps = BashOperator(
#         task_id='delete_apps',
#         bash_command=f'gsutil -m rm -f -r gs://juan-ortiz-dev-ntt/rawdata/apps/'
#     )
#     start_pipeline_apps = CloudDataFusionStartPipelineOperator(
#         location=LOCATION,
#         pipeline_name=PIPELINE_NAME_1,
#         instance_name=INSTANCE_NAME,
#         task_id="start_apps"
#     )

#     #APPS_CATEGORIES
#     create_pipeline_apps_categories = CloudDataFusionCreatePipelineOperator(
#         location=LOCATION,
#         pipeline_name= PIPELINE_NAME_2,
#         pipeline=PIPELINE_2,
#         instance_name=INSTANCE_NAME,
#         task_id="apps_categories"
#     )
#     delete_apps_categories =BashOperator(
#     task_id='delete_apps_categories',
#     bash_command=f'gsutil -m rm -r gs://juor-dev/rawdata/apps_categories/'    
#     )
#     start_pipeline_apps_categories = CloudDataFusionStartPipelineOperator(
#         location=LOCATION,
#         pipeline_name=PIPELINE_NAME_2,
#         instance_name=INSTANCE_NAME,
#         task_id="start_apps_categories"
#     )

#     #CATEGORIES
#     create_pipeline_categories = CloudDataFusionCreatePipelineOperator(
#         location=LOCATION,
#         pipeline_name= PIPELINE_NAME_3,
#         pipeline=PIPELINE_3,
#         instance_name=INSTANCE_NAME,
#         task_id="categories"
#     )
#     delete_categories =BashOperator(
#     task_id='delete_categories',
#     bash_command=f'gsutil -m rm -r gs://juor-dev/rawdata/categories/'    
#     )
#     start_pipeline_categories = CloudDataFusionStartPipelineOperator(
#         location=LOCATION,
#         pipeline_name=PIPELINE_NAME_3,
#         instance_name=INSTANCE_NAME,
#         task_id="start_categories"
#     )

#     #KEY_BENEFITS
#     create_pipeline_key_benefits = CloudDataFusionCreatePipelineOperator(
#         location=LOCATION,
#         pipeline_name= PIPELINE_NAME_4,
#         pipeline=PIPELINE_4,
#         instance_name=INSTANCE_NAME,
#         task_id="key_benefits"
#     )
#     delete_key_benefits =BashOperator(
#     task_id='delete_key_benefits',
#     bash_command=f'gsutil -m rm -r gs://juor-dev/rawdata/key_benefits/'    
#     )
#     start_pipeline_key_benefits = CloudDataFusionStartPipelineOperator(
#         location=LOCATION,
#         pipeline_name=PIPELINE_NAME_4,
#         instance_name=INSTANCE_NAME,
#         task_id="start_key_benefits"
#     )

#     #PRICING_PLAN_FEATURES
#     create_pipeline_pricing_plan_features = CloudDataFusionCreatePipelineOperator(
#         location=LOCATION,
#         pipeline_name= PIPELINE_NAME_5,
#         pipeline=PIPELINE_5,
#         instance_name=INSTANCE_NAME,
#         task_id="pricing_plan_features"
#     )
#     delete_pricing_plan_features =BashOperator(
#     task_id='delete_pricing_plan_features',
#     bash_command=f'gsutil -m rm -r gs://juor-dev/rawdata/pricing_plan_features/'    
#     )
#     start_pipeline_pricing_plan_features = CloudDataFusionStartPipelineOperator(
#         location=LOCATION,
#         pipeline_name=PIPELINE_NAME_5,
#         instance_name=INSTANCE_NAME,
#         task_id="start_pricing_plan_features"
#     )

#     #PRICING_PLANS
#     create_pipeline_pricing_plans = CloudDataFusionCreatePipelineOperator(
#         location=LOCATION,
#         pipeline_name= PIPELINE_NAME_6,
#         pipeline=PIPELINE_6,
#         instance_name=INSTANCE_NAME,
#         task_id="pricing_plans"
#     )
#     delete_pricing_plans =BashOperator(
#     task_id='pricing_plans',
#     bash_command=f'gsutil -m rm -r gs://juor-dev/rawdata/pricing_plans/'    
#     )
#     start_pipeline_pricing_plans = CloudDataFusionStartPipelineOperator(
#         location=LOCATION,
#         pipeline_name=PIPELINE_NAME_6,
#         instance_name=INSTANCE_NAME,
#         task_id="start_pricing_plans"
#     )

#     #REVIEWS
#     create_pipeline_reviews = CloudDataFusionCreatePipelineOperator(
#         location=LOCATION,
#         pipeline_name= PIPELINE_NAME_7,
#         pipeline=PIPELINE_7,
#         instance_name=INSTANCE_NAME,
#         task_id="reviews"
#     )
#     delete_reviews =BashOperator(
#     task_id='delete_reviews',
#     bash_command=f'gsutil -m rm -r gs://juor-dev/rawdata/reviews/'    
#     )
#     start_pipeline_reviews = CloudDataFusionStartPipelineOperator(
#         location=LOCATION,
#         pipeline_name=PIPELINE_NAME_7,
#         instance_name=INSTANCE_NAME,
#         task_id="start_reviews"
#     )

# #ARMADO DE DAG
# inicio >> create_pipeline_apps >> delete_apps >> start_pipeline_apps >> fin
# inicio >> create_pipeline_apps_categories >> delete_apps_categories >> start_pipeline_apps_categories >> fin
# inicio >> create_pipeline_categories >> delete_categories >> start_pipeline_apps >> fin
# inicio >> create_pipeline_key_benefits >> delete_key_benefits >> start_pipeline_key_benefits >> fin
# inicio >> create_pipeline_pricing_plan_features >> delete_pricing_plan_features >> start_pipeline_pricing_plan_features >> fin
# inicio >> create_pipeline_pricing_plans >> delete_pricing_plans >> start_pipeline_pricing_plans >> fin
# inicio >> create_pipeline_reviews >> delete_reviews >> start_pipeline_reviews >> fin

inicio >> create_cluster >> set_profile >> pipeline >> apps_process >> delete_cluster >> delete_instance >>fin
inicio >> set_profile >> pipeline >>fin
