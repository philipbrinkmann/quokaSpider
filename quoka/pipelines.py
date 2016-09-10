# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from datetime import date, timedelta
import sqlite3 as sqlite

class QuokaPipeline(object):
	def __init__(self):
		"""initialise the database with all required fields"""
		self.connection = sqlite.connect('./quoka_ads.db')
		self.cursor = self.connection.cursor()
		self.cursor.execute('CREATE TABLE IF NOT EXISTS quokascrapedata \
                    (id INTEGER PRIMARY KEY,\
					  Boersen_ID INTEGER, \
					  OBID INTEGER, \
					  erzeugt_am INTEGER, \
					  Anbieter_Id VARCHAR(20), \
					  Stadt VARCHAR(150), \
					  PLZ VARCHAR(10), \
					  Ueberschrift VARCHAR(500), \
					  Beschreibung VARCHAR(15000), \
					  Kaufpreis INTEGER, \
					  Monat INTEGER, \
					  url VARCHAR(1000), \
					  Telefon INTEGER, \
					  Erstellungsdatum INTEGER, \
					  Gewerblich INTEGER)')

	def process_item(self, item, spider):
		item['Boersen_ID'] = 0 # !!!was muss hier wirklich hin???
		item['OBID'] = eval(item['OBID'][0]) # extract() returns an array with one element, which is a number (the id) as string
		item['erzeugt_am'] = eval(date.today().strftime("%Y%m%d")) # date of the crawl -> today; format: yyyymmdd as integer
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
			item['Kaufpreis'] = eval(Kaufpreis)
		except:
			item['Kaufpreis'] = 0 # default value if no value is given
		item['Monat'] = date.today().month # !!!was soll hier wirklich hin???
		#item['url'] = item['url'][0] # the url is in the string that is on position 0 in the array
		item['Telefon'] = 0 # !!!muss noch geschrieben werden!!!
		for d in item['Erstellungsdatum']: # extract() gibt zuviele Zeilen zurueck, aber nur die Datumszeile enthaelt nicht-Whitespaces
			d = d.strip()
			if not d == '':
				if "Heute" in d:
					item['Erstellungsdatum'] = eval(date.today().strftime("%Y%m%d")) # today; format: yyyymmdd as integer
				elif "Gestern" in d:
					yesterday = date.today() - timedelta(1)
					item['Erstellungsdatum'] = eval(yesterday.strftime("%Y%m%d")) # yesterday; format: yyyymmdd as integer
				else:
					d = d.split(".")	# now d is an array of strings: [day, month, year]
					item['Erstellungsdatum'] =  eval(d[2]+d[1]+d[0]) # kann auch "Heute, um..." oder "Gestern" sein!!!
				break
		# NOW: insert the data into the database if it is not already there
		self.cursor.execute("SELECT * FROM quokascrapedata WHERE OBID=?", (item['OBID'],)) # get the entry (if it exists)
		result = self.cursor.fetchone() # if the entry exists, then result == True
		if not result:
			self.cursor.execute("INSERT INTO quokascrapedata (Boersen_ID, OBID, erzeugt_am, Stadt, PLZ, Ueberschrift, \
															  Beschreibung, Kaufpreis, Monat, url, Telefon, Erstellungsdatum, Gewerblich) \
								 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (item['Boersen_ID'], item['OBID'], item['erzeugt_am'], item['Stadt'], item['PLZ'], item['Ueberschrift'], \
					 item['Beschreibung'], item['Kaufpreis'], item['Monat'], item['url'], item['Telefon'], item['Erstellungsdatum'], item['Gewerblich']))
			self.connection.commit()
		return item
