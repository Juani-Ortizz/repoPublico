import google.auth 
import requests
import google.auth.transport.requests
import subprocess
import sys
from google.cloud import storage
from pprint import pprint

# google-auth==1.34.0
# google-auth-httplib2==0.1.0
# google-auth-oauthlib==0.4.5
# google-cloud-storage==1.41.1
# requests==2.22.0

#Paquetes para que dataproc trabaje en Data Fusion
def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

install("google-auth")
install("google-auth-httplib2")
install("google-auth-oauthlib")

#VARIABLES
instance_type="BASIC"
client = storage.Client()
project = client.project
location="us-central1"
region_acronym = "usc1"
namespace_id="default"
instance_name= 'instancia1'
# bucket_name = "profile-"+env+"-data-fusion"
profile_name= "max-personal-ingesta"

#PROFILE VARIABLES
LABEL = "Dataproc-personal"
NAME = "create_dataproc_cluster"

profile_json = {
  "name": "dataproc",
  "label": "Dataproc",
  "description": "Creates Dataproc clusters for program runs",
  "scope": "SYSTEM",
  "status": "ENABLED",
  "provisioner": {
    "name": "gcp-dataproc",
    "properties": [
      {
        "name": "masterNumNodes",
        "value": "1",
        "isEditable": "true"
      },
      {
        "name": "masterCPUs",
        "value": "1",
        "isEditable": "true"
      },
      {
        "name": "masterMemoryMB",
        "value": "4096",
        "isEditable": "true"
      },
      {
        "name": "masterDiskGB",
        "value": "1000",
        "isEditable": "true"
      },
      {
        "name": "workerNumNodes",
        "value": "2",
        "isEditable": "true"
      },
      {
        "name": "workerCPUs",
        "value": "2",
        "isEditable": "true"
      },
      {
        "name": "workerMemoryMB",
        "value": "8192",
        "isEditable": "true"
      },
      {
        "name": "workerDiskGB",
        "value": "1000",
        "isEditable": "true"
      }
    ],
    "totalProcessingCpusLabel": "4"
  },
  "created": 1651087693
}

def setProfile():
 
    credentials, your_project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)  # refresh token
    authtoken=credentials.token


    ## Crear Profile
    print("Obteniendo profile","\n")

    url = "https://"+ instance_name +"-"+ project + "-dot-" + region_acronym + ".datafusion.googleusercontent.com/api/v3/namespaces/"+ namespace_id +"/profiles/" + profile_name
    print("URL: ",url)

    response = requests.request("PUT", url, headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' + authtoken}, json=profile_json)

    pprint(response)     
    print(response.reason)
    print(response.ok)
    print(response.status_code)      
    print("\n")
    # pprint(profile_json)  
    
        
    # # # Enable Profile NO ES NECESARIO PUES AL CREARLO YA SE HABILITA, AL YA ESTAR CREADA DA "CONFLICT"
    # # print("Habilitando Profile del Bucket en la Instancia","\n")

    # # #POST /v3/profiles/<profile-name>/enable
    # # url = "https://"+ instance_name +"-"+ project + "-dot-" + region_acronym + ".datafusion.googleusercontent.com/api/v3/namespaces/"+ namespace_id +"/profiles/" + profile_name + "/enable" 
    # # print("URL: ",url)

    # # response = requests.request("POST", url, headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' + authtoken})
    # # pprint(response)     
    # # print(response.reason)
    # # print(response.ok)
    # # print(response.status_code)      
    # # print("\n")




    ## Setear el Profile como default para el Namespace "default"

    url= "https://"+ instance_name +"-"+ project + "-dot-" + region_acronym + ".datafusion.googleusercontent.com/api/v3/namespaces/" + namespace_id + '/preferences'
    print("URL: ",url)

    #Se modifica el payload para que setee el perfil de computo del cluster existente de dataproc
    payload = {"system.profile.name":"USER:"+ profile_name}
    print("The payload is: %s" % payload)
    response = requests.request("PUT", url, headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' + authtoken}, json=payload)
    pprint(response)     
    print(response.reason)
    print(response.ok)
    print(response.status_code)      
    print("\n")
    # pprint(profile_json)  


setProfile()