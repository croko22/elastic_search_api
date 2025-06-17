from elasticsearch import Elasticsearch, helpers
import json
import os, random, json, uuid
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, helpers

es = Elasticsearch("http://localhost:9200", api_key="UEttVF9aWUJ1Q0hTNl9FdzhWX3M6eDFKZUlLSEJDel94aWl3aHlENUVNdw==")

video_files = [f for f in os.listdir("./videos") if f.endswith(".mp4")]
resolutions = ["640x360", "1280x720", "1920x1080"]
codecs = ["h264", "mpeg4", "vp9", "h265"]
object_types = [
    "car", "truck", "bus", "motorcycle", "bicycle", "person", "dog", "cat", "traffic_light",
    "stop_sign", "bench", "tree", "trash_can", "street_lamp", "building", "fence", "crosswalk",
    "fire_hydrant", "parking_meter", "sidewalk"
]

def gen_actions():
    for _ in range(100):
        filename = random.choice(video_files)
        video_id = str(uuid.uuid4())
        duration = round(random.uniform(10, 120), 2)
        resolution = random.choice(resolutions)
        codec = random.choice(codecs)
        date = (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat(timespec='minutes')
        location = f"camera_{random.randint(1, 5)}"
        sampled_objects = random.sample(object_types, k=random.randint(2, 6))
        objects_detected = [{"type": obj, "count": random.randint(1, 100)} for obj in sampled_objects]

        yield {
            "_index": "videos",
            "_id": video_id,
            "_source": {
                "id": video_id,
                "filename": filename,
                "url": f"http://localhost:8080/videos/{filename}",
                "duration": duration,
                "objects_detected": objects_detected,
                "date": date,
                "location": location,
                "resolution": resolution,
                "codec": codec
            }
        }

actions = list(gen_actions())

# Si quieres guardar el NDJSON para debug:
with open("bulk_videos.json", "w") as f:
    for action in actions:
        f.write(json.dumps(action["_source"]) + "\n")

helpers.bulk(es, actions)