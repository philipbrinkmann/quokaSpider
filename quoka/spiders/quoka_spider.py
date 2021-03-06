import scrapy
import json
from quoka.items import QuokaItem
from scrapy.http import FormRequest

class QuokaSpider(scrapy.Spider):
	name="quoka"
	allowed_domains = ["quoka.de"] # make sure the spider does not leave the main server; it shall not trace links to other portals
	start_urls = ["http://www.quoka.de/immobilien/bueros-gewerbeflaechen/"] # we want to find all offers in this category, so we can start with it

	def parse(self, response):
		"""parses the main page and sets the first filter
				(only offers, distinguish between private and commercial)
		"""
		# iterate over the integers 0,1 (private, commercial)
		for comm in xrange(2):
			request = FormRequest.from_response(response, formdata={'classtype': 'of', 'comm': str(comm)}, callback=self.parse_overview_page1)
			request.meta['comm'] = comm # save the status (private/commercial) in the meta data to pass it to the next parse function
			yield request

	def parse_comm(self, response):
		"""parses the first overview page for private/commercial
				looks for the different cities available and sets the respective filter
		"""
		comm = response.meta['comm'] # the private/commercial indicator
		# iterate over the visible cities
		for line in response.xpath('//div[@class="cnt"]'):
			for ci in line.xpath('ul/li/ul/li/a/@onclick').extract():
				cityid = ci.split("'")[1] # syntax: onclick="qsn.changeCity('cityid',25,this); return false;"
				request = FormRequest.from_response(response, formdata={'classtype': 'of', 'comm': str(comm), 'cityid': cityid},
													callback=self.parse_overview_page1)
				request.meta['comm'] = comm
				request.meta['cityid'] = cityid
				yield request
		# iterate over the non-visible cities
		for line in response.xpath('//div[@id="NAV_CONTENT_CITIES_MOREELEMENTS"]'):
			for ci in line.xpath('ul/li/ul/li/a/@onclick').extract():
				cityid = ci.split("'")[1] # syntax: onclick="qsn.changeCity('cityid',25,this); return false;"
				request = FormRequest.from_response(response, formdata={'classtype': 'of', 'comm': str(comm), 'cityid': cityid},
													callback=self.parse_overview_page1)
				request.meta['comm'] = comm
				request.meta['cityid'] = cityid
				yield request

	def parse_overview_page1(self, response):
		"""parses the first overview page
			these filters must have been set beforehand: comm (private/commercial), city
			does not read the ads, but calls for every of these pages the processing function
		"""
		comm = response.meta['comm'] # the private/commercial indicator
		#cityid = response.meta['cityid'] # the id of the city of which we look for the ads (as string)
		# find the number of pages in total and open all other pages from 1,...,last page
		if len(response.xpath('//li[@class="pageno"]/a[@class="nothing"]/strong')) > 1:
			numpages = int(response.xpath('//li[@class="pageno"]/a[@class="nothing"]/strong[2]/text()').extract()[0])
			for pageno in xrange(1,numpages+1):
				# we have to re-post our form for the filter settings
				#request = FormRequest.from_response(response, formdata={'classtype': 'of', 'comm': str(comm), 'pageno': str(pageno), 'cityid': cityid},
				#									callback=self.parse_overview_page2)
				request = FormRequest.from_response(response, formdata={'classtype': 'of', 'comm': str(comm), 'pageno': str(pageno)},
													callback=self.parse_overview_page2)
				request.meta['comm'] = comm
				yield request
				# find the immoscout ads for this site
				request = scrapy.Request('http://www.quoka.de/qs/qpc/xmlSearch.php?search=&view=quoka&platform=desktop&catid=27_2710&maxresults=20&page=' +str(pageno)+
										'&output=json&oe=UTF-8', callback=self.parse_immoscout)
				request.meta['comm'] = comm
				yield request
		else:
			# in this case there is no "Seite 1 von n", so we simply scrape this page
			request = scrapy.Request(response.url, callback=self.parse_overview_page2)
			request.meta['comm'] = comm
			yield request

	def parse_overview_page2(self, response):
		"""parses the page with (20ish) availabe offers
			for the specified case offers, private/commercially, city
			this page contains the links to the detail pages
			the spider follows these, unless they drive it away from quoka.de, then the ad is processed right away
			the spider also follows the "next page" link at the bottom of the page to crawl all offers
			the processing of the detail pages is done in parse_ad.
		"""
		comm = response.meta['comm'] # the private/commercial indicator
		inserate_link = response.xpath('//div[@class="q-col n2"]/a[@href]') # some offers come directly with a link we can trace
		inserate_js = response.xpath('//div[@class="q-col n2"]/a[@data-qng-prg]') # some offers need JavaScript (?) to be opened
		for line in inserate_link:
			url = response.urljoin(line.xpath('@href').extract()[0])
			request = scrapy.Request(url, callback=self.parse_ad)
			request.meta['comm'] = comm
			yield request
		for line in inserate_js:
			adno = line.xpath('@data-qng-prg').extract()[0].split('|')[0] # the id of the ad (as string)
			request = FormRequest.from_response(response, formdata={'classtype': 'of', 'comm': str(comm), 'prg_adno': adno},
												callback=self.parse_ad)
			request.meta['comm'] = comm
			yield request
	
	def parse_immoscout(self, response):
		"""response is a JSON with the ads stored in the field result"""
		rs = json.loads(response._body)
		for line in rs['result']:
			I = QuokaItem()
			# those fields we do not know leave blank (will be None in the DB)
			I['Boersen_ID'] = 1
			I['Anbieter_ID'] = 'Immobilienscout'
			I['Ueberschrift'] = line['title']
			I['Beschreibung'] = line['description']
			I['Kaufpreis'] = float(line['priceTotal'])
			if I['Kaufpreis'] == []:
				I['Kaufpreis'] = 0
			I['Monat'] = "September"
			I['url'] = line['urlClick']
			I['Gewerblich'] = response.meta['comm']
			yield I

	def parse_ad(self, response):
		"""parses the detail pages of every offer
			the QuokaPipeline processes the data into the desired format
		"""
		I = QuokaItem()
		I['Boersen_ID'] = 1
		I['OBID'] = response.xpath('//div[@class="date-and-clicks"]/strong[1]/text()').extract()
		I['Anbieter_ID'] = ' ' #only set for immoscout ads
		I['Stadt'] = response.xpath('//a/span[@class="locality"]/text()').extract()
		if I['Stadt'] == []: # s.t. there is no city given
			I['Stadt'] = ['']
		I['PLZ'] = response.xpath('//strong/span[@class="address location"]/span[@class="countryzip"]/span[@class="postal-code"]/text()').extract()
		if I['PLZ'] == []: # s.t. there is no post code given
			I['PLZ'] = ['']
		I['Ueberschrift'] = response.xpath('//h1[@itemprop="name"]/text()').extract()
		I['Beschreibung'] = response.xpath('//div[@itemprop="description"]/text()').extract()
		I['Kaufpreis'] = response.xpath('//div[@class="price has-type"]/strong/span/text()').extract()
		I['Monat'] = "September"
		I['url'] = response.url
		I['Erstellungsdatum'] = response.xpath('//div[@class="date-and-clicks"]/text() | //span[@class="today"]/text()').extract()
		I['Gewerblich'] = response.meta['comm']
		# check, whether there is a phone number on the page
		telcode = response.xpath('//ul[@class="contacts"]/li/span/a/@onclick').extract()
		if not telcode == []:
			# to read the telephone number, we have to follow the link http://www.quoka.de/ajax/detail/displayphonenumber.php?coded=...
			# the value of coded is hidden in @onclick="jQuery( '#Handy1' ).load( '/ajax/detail/displayphonenumber.php?coded=MDE1MTQvIDAwNTY4NzM-&adno=173479169&..."
			coded = telcode[0].split("coded=")[1].split('&')[0]
			request = scrapy.Request("http://www.quoka.de/ajax/detail/displayphonenumber.php?coded="+coded, self.parse_tel)
			request.meta['item'] = I
			yield request
		else:
			I['Telefon'] = ['']
			yield I

	def parse_tel(self, response):
		"""gets the telephone number"""
		I = response.meta['item']
		I['Telefon'] = response.xpath('//span[@class]/text()').extract()
		yield I
