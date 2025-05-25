from fastapi import APIRouter, UploadFile, File, Request, HTTPException
from fastapi.responses import StreamingResponse
from hdfs import InsecureClient
import shutil, uuid, os

router = APIRouter()
hdfs_client = InsecureClient('http://localhost:9870', user='hadoop')

@router.post("/upload")
def upload_video(file: UploadFile = File(...)):
    video_id = str(uuid.uuid4())
    temp_path = f"/tmp/{video_id}.mp4"
    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    hdfs_path = f"/videos/raw/{video_id}.mp4"
    hdfs_client.upload(hdfs_path, temp_path)
    os.remove(temp_path)
    return {"video_id": video_id, "hdfs_path": hdfs_path}