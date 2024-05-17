import functions_framework
import vertexai
from vertexai.generative_models import GenerativeModel, Part
from google.cloud import storage
import os
import json
from datetime import datetime

project_id = "{your-project}"
cliente_de_almacenamiento = storage.Client(project=project_id)
bucket = cliente_de_almacenamiento.bucket("{your-bucket}")

def create_json_description(name, data):
  path = os.path.join("descripcion/", name + ".json")
  blob = bucket.blob(path)
  blob.custom_time = datetime.now()
  blob.upload_from_string(json.dumps(data, indent=4))
  return

def videito(bucket_name, filename):
  vertexai.init(project=project_id, location="{project-location}")

  model = GenerativeModel(model_name="gemini-1.5-pro-preview-0409")

  folder_path = "videos/"  # Replace with your actual folder path
  video_file = Part.from_uri("gs://" + bucket_name + "/" + folder_path + filename, mime_type="video/mp4")

  blob = bucket.get_blob("videos/" + filename)

  if blob is not None and blob.exists():
    language = blob.metadata["language"]

    propmt = ""

    if language == "ingles":
      propmt = "Is there an attack? If there is, it returns true and a description of it, if not, it only returns false"
    elif language == "español":
      propmt = "hay una agresion? en caso de que la haya, devolve true y una descripcion del mismo, en caso de que no la haya, retorna solo false"
    else:
      print("error", language)

    print(video_file)

    contents = [
      video_file,
      propmt
    ]

    response = model.generate_content(contents)
    print(response.text)

    # Create JSON description (replace placeholder data)
    description_data = {
      "filename": filename,
      "content_language": language,  # Handle missing metadata
      "description": response.text,
    }

    create_json_description(filename, description_data)

  else:
    print(f"File {filename} does not exist in bucket {bucket_name}.")

@functions_framework.cloud_event
def hello_gcs(cloud_event):
  data = cloud_event.data

  bucket = data["bucket"]
  filename = os.path.basename(data["name"])

  videito(bucket, filename)