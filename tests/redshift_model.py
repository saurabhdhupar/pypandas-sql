from sqlalchemy import Column, String, DateTime, Integer
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Person(Base):
    __tablename__ = 'test_people'

    user_name = Column(String, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    team = Column(String)
    employment_term = Column(String)
    start_date = Column(DateTime)
    age = Column(Integer)
    end_date = Column(DateTime, nullable=True)
