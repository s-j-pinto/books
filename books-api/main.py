from fastapi import FastAPI
app = FastAPI()
@app.get("/")
def get_root(path):
    return "You are inside books api"