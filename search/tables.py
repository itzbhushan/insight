from sqlalchemy import Column, Integer, String, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import ForeignKey


Base = declarative_base()


class Users(Base):
    __tablename__ = "users"
    site = Column("site", String, primary_key=True)
    user_id = Column("user_id", Integer, primary_key=True)
    reputation = Column("reputation", Integer)


class Questions(Base):
    __tablename__ = "questions"
    site = Column("site", String, primary_key=True)
    id = Column("id", Integer, primary_key=True)
    creation_date = Column("creation_date", TIMESTAMP)
    score = Column("score", Integer)
    user_id = Column("user_id", ForeignKey(Users.user_id))
    answer_count = Column("answer_count", Integer)
    link = Column("link", String)
