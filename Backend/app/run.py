from fastapi import FastAPI
from app.routes import sql_handler

app = FastAPI()

app.include_router(sql_handler.app)
