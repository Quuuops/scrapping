import scrapy

MIN_PRICE = 0
MAX_PRICE = 10000
class ZillowSpider(scrapy.Spider):
    name = "zillow"
    allowed_domains = ["zillow.com"]

    search_query_state = {
        "pagination": {},
        "isMapVisible": True,
        "mapBounds": {
            # "north": 25.856011,
            # "south": 25.550306,
            # "east": -80.140245,
            # "west": -80.548696
        },
        "usersSearchTerm": "Miami FL",
        "regionSelection": [{"regionId": 12700, "regionType": 6}],
        "filterState": {
            "fr": {"value": True},  # "For Rent" filter
            "mp": {"min": MIN_PRICE, "max": MAX_PRICE},  # Price range filter
            "beds": {"min": 0},  # Minimum bedrooms
        },
        "isListVisible": True,
        "mapZoom": 11
    }
    start_urls = [
        f'https://www.zillow.com/homes/for_rent/Miami,-FL_rb/price={MIN_PRICE}-{MAX_PRICE}',
        f'https://www.zillow.com/homes/for_rent/Dallas,-TX_rb/price={MIN_PRICE}-{MAX_PRICE}'
    ]

    def parse(self, response):
        for listing in response.css('.StyledListCardWrapper-srp-8-105-2__sc-wtsrtn-0'):
            address = listing.css('address[data-test="property-card-addr"]::text').get()
            general_price_range = listing.css('span[data-test="property-card-price"]::text').get()

            for apartment in listing.css('.Anchor-c11n-8-105-2__sc-hn4bge-0'):

                specific_price = apartment.css('.PropertyCardInventoryBox__PriceText-srp-8-105-2__sc-1jotqb7-3::text').get()
                type_apartment = apartment.css('.PropertyCardInventoryBox__BedText-srp-8-105-2__sc-1jotqb7-2::text').get()
                link = apartment.css('::attr(href)').get()

                full_link = response.urljoin(link)
                yield response.follow(full_link, self.parse_apartment, meta={
                    'address': address,
                    'general_price_range': general_price_range,
                    'specific_price': specific_price,
                    'type_apartment': type_apartment
                })

        # next_page = response.css('a[title="Next page"]::attr(href)').get()
        next_page = response.css('a[rel="next"]::attr(href)').get()
        if next_page:
            next_page_url = response.urljoin(next_page)
            yield response.follow(next_page_url, callback=self.parse)

    def parse_apartment(self, response):
        additional_data = [', '.join(fact.css('::text').getall()).strip() for fact in response.css('div.AtAGlanceFactsHollywood__StyledContainer-sc-34d077-0.jevfwQ')]
        phone_number = response.css('.styled__PhoneNumberContainer-egkps0-18.ytego::text').get()

        for various_price_apartment in response.css('.floorplan-info-v2'):
            bed_bath_info = various_price_apartment.css('.bed-bath-info::text').get()
            diapason_prices = various_price_apartment.css('.units-table__text--sectionheading::text').get()
            square = various_price_apartment.css('.Text-c11n-8-101-4__sc-aiai24-0.gtFYdd::text').get()
            units = various_price_apartment.css('.Text-c11n-8-101-4__sc-aiai24-0.cBlwPi::text').get()


            yield {
                'address': response.meta['address'],
                'general_price_range': response.meta['general_price_range'],
                'specific_price': response.meta['specific_price'],
                'type_apartment': response.meta['type_apartment'],
                'bed_bath_info': bed_bath_info,
                'diapason_prices': diapason_prices,
                'square': square,
                'units': units,
                'phone_number': phone_number,
                'additional_data': additional_data,
                'url': response.url
            }