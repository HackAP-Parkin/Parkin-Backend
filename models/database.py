import json
import os

from dataclasses import dataclass
from typing import Optional

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, BigInteger, ForeignKey, inspect
from databases import Database

from dotenv import load_dotenv

load_dotenv('.env', verbose=True)


@dataclass(kw_only=True, init=True)
class DatabaseConfig:
    host: Optional[str]
    port: Optional[int]
    username: Optional[str]
    password: Optional[str]
    db_name: Optional[str]

    def get_link(self):
        return f"postgresql+asyncpg://{self.username}:{self.password}@{self.host}:{self.port}/{self.db_name}"

    @classmethod
    def from_config(cls, path: str):
        with open(path) as f:
            data = json.load(f)
        return cls(
            host=data['host'],
            port=data['port'],
            username=data['username'],
            password=os.environ["DB_PASSWORD"],
            db_name=data['dbName']
        )


db_config = DatabaseConfig.from_config('config.json')
database = Database(db_config.get_link())
engine = create_engine(db_config.get_link())

metadata = MetaData()

users_table = Table(
    'users',
    metadata,
    Column('uid', String(255), primary_key=True),
    Column('id', Integer, primary_key=True),
    Column('type', Integer)
)

drivers_table = Table(
    'drivers',
    metadata,
    Column('driver_id', Integer, ForeignKey('users.id'), primary_key=True),
    Column('vehicle_id_assigned', Integer, ForeignKey('vehicles.vid'), default=None)
)

vehicles_table = Table(
    'vehicles',
    metadata,
    Column('vid', Integer, ForeignKey('users.id'), primary_key=True),
    Column('reg_no', String(255))
)


class DataSource:
    def __init__(self, database: Database):
        self.database = database

    async def setup_hook(self):
        metadata.create_all(engine)

        if self._schema_changed():
            await self._reset_database()

    async def _schema_changed(self):
        inspector = inspect(engine)
        current_tables = inspector.get_table_names()
        defined_tables = metadata.tables.keys()

        # Check if the number of tables is different
        if set(current_tables) != set(defined_tables):
            return True

        # Check if the columns in each table are different
        for table_name in defined_tables:
            if table_name not in current_tables:
                return True

            current_columns = inspector.get_columns(table_name)
            defined_columns = metadata.tables[table_name].columns.keys()

            current_column_names = {col['name'] for col in current_columns}
            if set(current_column_names) != set(defined_columns):
                return True
        return False

    async def _reset_database(self):
        async with self.database.transaction():
            await self.database.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
            metadata.create_all(engine)

    async def fetch_driver_info(self, user_id: str):
        query = """
        SELECT d.*
        FROM "users" u
        INNER JOIN "drivers" d ON u.id = d.driver_id
        WHERE u.type = 2 AND u.id = :user_id;
        """

        results = await self.database.fetch_one(query=query, values={"user_id": user_id})
        return results

    async def get_vehicles(self):
        query = """
        SELECT *
        FROM vehicles
        LEFT JOIN drivers ON vehicles.vid = drivers.vehicle_id_assigned
        WHERE drivers.vehicle_id_assigned IS NULL;
        """
        return [x['vid'] for x in await self.database.fetch_all(query)]

    async def assign_vehicle(self, _id: int):
        vehicle_ids = await self.get_vehicles()
        selected_vehicle = vehicle_ids[0]

        query = """
        UPDATE drivers
        SET vehicle_id_assigned = :vid
        WHERE driver_id = :id;
        """

        async with self.database.transaction():
            await self.database.execute(query=query, values={'id': _id, 'vid': selected_vehicle})
