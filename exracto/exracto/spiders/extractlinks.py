# import scrapy 

# class ExtractUrl(scrapy.Spider):
#     name ="extract"

#     def start_requests(self):
#         urls = ['https://www.geeksforgeeks.org/']

#         for url in urls:
#             yield scrapy.Request(url = url , callback=self.parse)

    
#     def parse(self, response):
#         title = response.css('title::text').extract_first()

#         links = response.css('a::attr(href)').extract()

#         for link in links:
#             yield {
#                 'title': title,
#                 'links': links 
#                 }

#         # if 'geeksforgeeks' in link:          
#         #         yield scrapy.Request(url = link, callback = self.parse) 


import scrapy
from urllib.parse import urljoin

class ZipFileSpider(scrapy.Spider):
    name = 'zip_file_spider'
    start_urls = ['https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets']

    # Set custom headers to mimic a browser
    custom_settings = {
    'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'RETRY_ENABLED': True,
    'RETRY_TIMES': 5,  # Number of retries before giving up
    'RETRY_HTTP_CODES': [403, 500, 502, 503, 504],
    'DOWNLOAD_DELAY': 2,
    'REFERER': 'https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets',

}


    def parse(self, response):
        # Extract all 'a' tags with links containing the "href" attribute
        links = response.css('a[href$=".zip"]::attr(href)').getall()

        # Check if any links were found
        if not links:
            self.log("No ZIP links found on the page.")
        
        # Loop through the links and create absolute URLs
        for link in links:
            absolute_url = urljoin(response.url, link)  # Join relative URLs with base URL
            yield {'zip_file_url': absolute_url}

