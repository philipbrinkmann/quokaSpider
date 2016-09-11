# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from datetime import date, timedelta
from quoka_db import Quoka_DB, db_connect, create_table
from sqlalchemy.orm import sessionmaker

class QuokaPipeline(object):
	def __init__(self):
		"""initialise the database with all required fields"""
		engine = db_connect()
		create_table(engine)
		self.Session = sessionmaker(bind=engine)

	def process_item(self, item, spider):
		if item['Anbieter_ID'] == ' ':
			item['OBID'] = int(item['OBID'][0]) # extract() returns an array with one element, which is a number (the id) as string
			item['erzeugt_am'] = int(date.today().strftime("%Y%m%d")) # date of the crawl -> today; format: yyyymmdd as integer
			item['Stadt'] = item['Stadt'][0].strip() # take the string with the city out of the array and remove preceeding or trailing whitespaces
			item['PLZ'] = item['PLZ'][0] # the post code is in the string that is first (and only) in this array
			item['Ueberschrift'] = item['Ueberschrift'][0].strip() # the title is the strong on first (and only) place in the array, remove unnecessary whitespaces
			beschreibung = '' # the description might cover several lines. In this case each liune is a single entry in the array returned by extract() -> concatenate
			for b in item['Beschreibung']:
				b.strip()	# remove unnecessary whitespaces
				beschreibung += b + " " # add an extra whitespace so that the last word of a line and the first of the follwing line are separated
			beschreibung.strip()	# remove unnecessary whitespaces (the last " ")
			item['Beschreibung'] = beschreibung
			try: # there might be no price given, then the following would cause an error
				Kaufpreis = item['Kaufpreis'][0] # gives the price as array containing a string that again contains dots as separators (a.bcd) and ",-" at the end
				Kaufpreis = Kaufpreis.replace(".","")	# remove the dots
				Kaufpreis = Kaufpreis.replace(",-","")	# remove the ",-"
				Kaufpreis = Kaufpreis.replace(",", ".") # change the German separator to the international one
				item['Kaufpreis'] = float(Kaufpreis)
			except:
				item['Kaufpreis'] = 0 # default value if no value is given
			item['Telefon'] = item['Telefon'][0].replace("/","") # the telephone number is separated by "/" or "/ "; there might be several (take the first)
			item['Telefon'] = item['Telefon'].replace(" ","")
			for d in item['Erstellungsdatum']: # extract() gibt zuviele Zeilen zurueck, aber nur die Datumszeile enthaelt nicht-Whitespaces
				d = d.strip()
				if not d == '':
					if "Heute" in d:
						item['Erstellungsdatum'] = int(date.today().strftime("%Y%m%d")) # today; format: yyyymmdd as integer
					elif "Gestern" in d:
						yesterday = date.today() - timedelta(1)
						item['Erstellungsdatum'] = int(yesterday.strftime("%Y%m%d")) # yesterday; format: yyyymmdd as integer
					elif "vor" in d:
						# it can happen that the date reads "vor 6 Monaten" or similar
						if "Monat" in d:
							day = date.today() - timedelta(int(d.split(' ')[1])*30) # count every month with 30 days to get an as accurate date as possible
						elif "Jahr" in d:
							day = date.today() - timedelta(int(d.split(' ')[1])*365) # count every year with 365 days to get an as accurate date as possible
						item['Erstellungsdatum'] = int(day.strftime("%Y%m%d")) # day; format: yyyymmdd as integer
					else:
						d = d.split(".")	# now d is an array of strings: [day, month, year]
						item['Erstellungsdatum'] =  int(d[2]+d[1]+d[0])
					break
		else:
			item['erzeugt_am'] = int(date.today().strftime("%Y%m%d")) # date of the crawl -> today; format: yyyymmdd as integer
		# NOW: insert the data into the database if it is not already there
		session = self.Session()
		dbentry = Quoka_DB(**item)

		try:
			session.add(dbentry)
			session.commit()
		except:
			session.rollback()
			raise
		finally:
			session.close()

		return item
