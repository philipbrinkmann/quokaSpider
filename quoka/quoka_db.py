from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, Numeric, String

Base = declarative_base()

def db_connect():
	"""
	Performs database connection.
	Returns sqlalchemy engine instance
	"""
	engine = create_engine('sqlite:////home/philip/bitbucked/quoka/quoka_ads.db')
	return engine

def create_table(engine):
	""""""
	Base.metadata.create_all(engine)

class Quoka_DB(Base):
		__tablename__ = 'Quoka_DB'

		id = Column(Integer, primary_key=True)
		Boersen_ID = Column(Numeric(8))
		OBID = Column(Numeric(8), nullable=True)
		erzeugt_am = Column(Numeric(8))
		Anbieter_ID = Column(String(16), nullable=True)
		Stadt = Column(String(150), nullable=True)
		PLZ = Column(String(10), nullable=True)
		Ueberschrift = Column(String(500))
		Beschreibung = Column(String(15000), nullable=True)
		Kaufpreis = Column(Numeric(8))
		Monat = Column(String(16))
		url= Column(String(1000), nullable=True)
		Telefon= Column(String(20), nullable=True)
		Erstellungsdatum = Column(Numeric(8), nullable=True)
		Gewerblich = Column(Numeric(8))
