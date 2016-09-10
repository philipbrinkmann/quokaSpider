# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class QuokaItem(scrapy.Item):
	Boersen_ID = scrapy.Field()
	OBID = scrapy.Field()
	erzeugt_am = scrapy.Field()
	Anbieter_ID = scrapy.Field()
	Stadt = scrapy.Field()
	PLZ = scrapy.Field()
	Ueberschrift = scrapy.Field()
	Beschreibung = scrapy.Field()
	Kaufpreis = scrapy.Field()
	Monat = scrapy.Field()
	url = scrapy.Field()
	Telefon = scrapy.Field()
	Erstellungsdatum = scrapy.Field()
	Gewerblich = scrapy.Field()
