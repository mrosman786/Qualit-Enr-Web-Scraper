from utils.basescraper import BaseScraper
import logging


class QualitEnrConfig:
    # Required configuration
    SITE_NAME = "qualit-enr"
    BASE_URL = "https://www.qualit-enr.org"

    # Optional configurations with defaults
    USE_CURL = False
    DEFAULT_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.qualit-enr.org/annuaire/",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1"
    }
    DEFAULT_COOKIES = {
        'axeptio_authorized_vendors': '%2Cgoogle_analytics%2CGoogleRecaptcha%2Caddthis%2Chotjar%2CYoutube%2Cgoogle_recaptcha%2Cyoutube%2C',
        'axeptio_all_vendors': '%2Cgoogle_analytics%2CGoogleRecaptcha%2Caddthis%2Chotjar%2CYoutube%2Cgoogle_recaptcha%2Cyoutube%2C',
        'axeptio_cookies': '{%22$$token%22:%22lsvu0voywrb8zj1i8yw83r%22%2C%22$$date%22:%222025-03-26T17:55:51.726Z%22%2C%22$$cookiesVersion%22:{%22name%22:%22qenr%22%2C%22identifier%22:%225df8f53f730c99249ab59382%22}%2C%22google_analytics%22:true%2C%22GoogleRecaptcha%22:true%2C%22addthis%22:true%2C%22hotjar%22:true%2C%22Youtube%22:true%2C%22google_recaptcha%22:true%2C%22youtube%22:true%2C%22$$completed%22:true}',
        '_ga': 'GA1.2.628301976.1743011727',
        '_gid': 'GA1.2.1514183877.1743011752',
        '_hjSession_2459276': 'eyJpZCI6IjM4M2IwNWJmLTIwOWQtNDZmMC1hMzE1LTI3ZTIzZGQwMDdlMSIsImMiOjE3NDMwMTE3NTQ1MzEsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjoxLCJzcCI6MH0=',
        '_hjSessionUser_2459276': 'eyJpZCI6ImM2MmFjN2Y1LWE0NWItNTViOS05NDM0LWNlYmJkODBiMmFmNCIsImNyZWF0ZWQiOjE3NDMwMTE3NTQ1MzAsImV4aXN0aW5nIjp0cnVlfQ==',
        '_ga_7F6CMXQNPE': 'GS1.1.1743011726.1.1.1743011843.0.0.0',
    }
    REQUEST_DELAY = None  # Random delay between 1-3 seconds
    MAX_RETRIES = 3
    LOG_LEVEL = logging.INFO
    LOG_FILE = "qualit_enr_scraper.log"


class QualitEnrScraper(BaseScraper, QualitEnrConfig):
    def __init__(self):
        # Initialize with class variables
        BaseScraper.__init__(
            self,
            site_name=self.SITE_NAME,
            base_url=self.BASE_URL,
            use_curl=self.USE_CURL,
            default_headers=self.DEFAULT_HEADERS,
            default_cookies=self.DEFAULT_COOKIES,
            request_delay=self.REQUEST_DELAY,
            max_retries=self.MAX_RETRIES,
            log_level=self.LOG_LEVEL,
            log_file=self.LOG_FILE
        )
        self.output_file = f"data/qualit-enr_output.csv"

    def _get_company_details(self, link: str, category: str) -> dict:
        """Get detailed information for a single company"""
        self.logger.info(f"Scraping company details from: {link}")
        response = self.make_request(link)
        soup = self.get_soup(response.text)

        try:
            name = soup.find("h1").get_text(strip=True)
            addr = soup.find("div", "fs-lg lh-md").get_text("||", True)
            addr_sp = addr.split("||")

            street = addr_sp[0]
            zip_city = addr_sp[-1]
            zip_city_sp = zip_city.split(" ")
            zip_code = zip_city_sp[0]
            city = " ".join(zip_city_sp[1:])

            skills = soup.find("h2", string="Nos compétences")
            skills_name = skills.find_next_sibling("div", "cms").get_text("\n ", strip=True) if skills else ""

            phone_container = soup.find("div", "phone-container d-none")
            phone = phone_container.find("a").get_text(strip=True) if phone_container else ""

            return {
                'link': link,
                'type': category,
                'name': name,
                'zip_code': zip_code,
                'city': city,
                'street': street,
                'phone': phone,
                'skills_name': skills_name
            }
        except Exception as e:
            self.logger.error(f"Error parsing company details from {link}: {str(e)}")
            return {}

    def scrape_region_category(self, category: str, region: str) -> list:
        """Scrape all pages for a specific category and region"""
        self.logger.info(f"Starting scrape for category: {category}, region: {region}")
        all_results = []
        page = 1

        while True:
            url = f"{self.BASE_URL}/annuaire/page/{page}/?type={category}&ville={region}&city&lat&lng&loc"
            self.logger.info(f"Scraping page {page} for {category} in region {region}")

            response = self.make_request(url)
            soup = self.get_soup(response.text)

            total_pages = 1
            if page == 1:
                try:
                    count_text = soup.find(id="company-search-results").get_text(strip=True)
                    total_count = int(count_text.split("/")[1].replace(" résultat(s)", ""))
                    total_pages = (total_count // 20) + 1
                    self.logger.info(f"Found {total_count} results, estimated {total_pages} pages")
                except Exception as e:
                    self.logger.warning(f"Could not determine total pages: {str(e)}")

            # Process company listings
            items = soup.find_all("a", "results-item")
            results = []
            for item in items:
                href = item.get("href")
                company_data = self._get_company_details(href, category)
                if company_data:
                    results.append(company_data)
                    self.save_to_csv([company_data], self.output_file)

            page_results, total_pages = results, total_pages
            all_results.extend(page_results)

            self.logger.info(f"Page {page}/{total_pages} completed - {len(page_results)} companies found")

            if page >= total_pages:
                break
            page += 1

        self.logger.info(f"Completed scrape for {category}/{region} - {len(all_results)} total companies")
        return all_results


if __name__ == '__main__':
    scraper = QualitEnrScraper()

    categories_urls = ["installateurs-photovoltaique", "installateurs-pompe-a-chaleur"]
    regions_code = ['72', '75']

    all_data = []
    for region in regions_code:
        for category in categories_urls:
            region_data = scraper.scrape_region_category(category, region)
            all_data.extend(region_data)
    stats = scraper.get_request_stats()
    print(f"Scraping completed. Statistics:\n{stats}")
