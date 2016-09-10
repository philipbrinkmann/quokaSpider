import scrapy
from quoka.items import QuokaItem

class QuokaSpider(scrapy.Spider):
	name="quoka"
	allowed_domains = ["quoka.de"] # make sure the spider does not leave the main server; it shall not trace links to other portals
	start_urls = ["http://www.quoka.de/immobilien/bueros-gewerbeflaechen/"] # we want to find all offers in this category, so we can start with it

	def parse(self, response):
		"""parses the page with (20ish) availabe offers
			this page contains the links to the detail pages
			the spider follows these, unless they drive it away from quoka.de
			the spider also follows the "next page" link at the bottom of the page to crawl all offers
			the processing of the detail pages is done in parse_ad.
		"""
		inserate_link = response.xpath('//div[@class="q-col n2"]/a[@href]') # some offers come directly with a link we can trace
		inserate_js = response.xpath('//div[@class="q-col n2"]/a[@data-qng-prg]') # some offers need JavaScript (?) to be opened
		for line in inserate_link:
			url = response.urljoin(line.xpath('@href').extract()[0])
			yield scrapy.Request(url, callback=self.parse_ad)
		for line in inserate_js:
			print line.xpath('@id | @data-qng-prg').extract()
		# find the "next page" button and follow it
		try:
			url = response.urljoin(response.xpath('//li[@class="arr-rgt active"]/a/@href').extract()[0])
			yield scrapy.Request(url, self.parse)
		except:
			pass

	def parse_ad(self, response):
		"""parses the detail pages of every offer
			the QuokaPipeline processes the data into the desired format
		"""
		I = QuokaItem()
		I['Boersen_ID'] = 1
		I['OBID'] = response.xpath('//div[@class="date-and-clicks"]/strong[1]/text()').extract()
		I['Anbieter_ID'] = ' ' #immoscout-Anzeigen muessen gesondert gelesen werden!!!
		I['Stadt'] = response.xpath('//a/span[@class="locality"]/text()').extract()
		I['PLZ'] = response.xpath('//strong/span[@class="address location"]/span[@class="countryzip"]/span[@class="postal-code"]/text()').extract()
		I['Ueberschrift'] = response.xpath('//h1[@itemprop="name"]/text()').extract()
		I['Beschreibung'] = response.xpath('//div[@itemprop="description"]/text()').extract()
		I['Kaufpreis'] = response.xpath('//div[@class="price has-type"]/strong/span/text()').extract()
		I['Monat'] = "September"
		#I['url'] = response.xpath('//a[@class="twitter"]/@data-url').extract()
		I['url'] = response.url
		#I['Telefon'] = #wird erst auf Klick hin angezeigt!!!
		I['Erstellungsdatum'] = response.xpath('//div[@class="date-and-clicks"]/text() | //span[@class="today"]/text()').extract()
		I['Gewerblich'] = 0 # !!!muss als Filter frueher gesetzt werden!!! hier erstmal nur default value damit die DB keinen Fehler auswirft
		yield I
