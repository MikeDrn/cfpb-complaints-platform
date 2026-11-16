from fastapi import FastAPI

app = FastAPI(title="CFPB collector")


@app.get("/health")
def health():
    return {"status": "ok"}