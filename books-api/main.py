from fastapi import FastAPI

@app.get("/")
def get_root():
    return "You are inside books api"