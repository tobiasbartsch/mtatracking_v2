from sqlalchemy import create_engine

from models import Base

engine = create_engine('postgresql://tbartsch:test@localhost/mtatrackingv2')

Base.metadata.create_all(engine)
