from base.orm import Base, engine
from orm.shout import Shout


def init_tables():
    Base.metadata.create_all(engine)
    Shout.init_table()
    print("[orm] tables initialized")
