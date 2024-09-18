from fastapi import FastAPI
from models.database import DataSource, database

from contextlib import asynccontextmanager

class Application:
    def __init__(self):
        self.app = FastAPI(lifespan=self.lifespan)
        self.source = DataSource(database)
        self.setup_routes()
    def setup_routes(self):
        @self.app.get("/")
        async def root():
            return {"message": "Hello World"}

        @self.app.post("/router/api/assign")
        async def assign_post(uid: str):
            await self.source.fetch_driver_info(uid)

            return

    @asynccontextmanager
    async def lifespan(self, _: FastAPI):
        await self.startup()
        print("Application startup")
        yield
        await self.shutdown()
        print("Application shutdown")

    async def startup(self):
        await self.source.setup_hook()

    async def shutdown(self):
        await self.source.database.disconnect()


app_instance = Application()
app = app_instance.app

