from services.db import Base, engine
from orm.shout import Shout
from orm.community import Community

def init_tables():
    Base.metadata.create_all(engine)
    Shout.init_table()
    Community.init_table()
    print("[orm] tables initialized")
