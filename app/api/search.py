from fastapi import APIRouter, Query
from elasticsearch import Elasticsearch
from app.models.search import SearchResponse
from dotenv import load_dotenv
import os

load_dotenv()
router = APIRouter()

es = Elasticsearch(
    os.getenv("ELASTIC_URL"),
    api_key=os.getenv("ELASTIC_API_KEY")
)

# @router.get("/search", response_model=SearchResponse)
# def search(q: str = Query(..., min_length=1)):
#     res = es.search(index="videos", query={
#         "multi_match": {
#             "query": q,
#             "fields": ["title", "content"]
#         }
#     })
#     hits = [hit["_source"] for hit in res["hits"]["hits"]]
#     return {"hits": hits}
@router.get("/search", response_model=SearchResponse)
def search(q: str = Query(..., min_length=1)):
    res = es.search(index="videos", query={
        "bool": {
            "should": [
                { "multi_match": {
                    "query": q,
                    "fields": ["filename", "location", "codec", "resolution"]
                }},
                { "nested": {
                    "path": "objects_detected",
                    "query": {
                        "match": {
                            "objects_detected.type": q
                        }
                    }
                }}
            ]
        }
    })
    hits = [hit["_source"] for hit in res["hits"]["hits"]]
    return {"hits": hits}