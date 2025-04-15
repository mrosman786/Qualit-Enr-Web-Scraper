# Qualit-Enr-Web-Scraper

A web scraper for https://www.qualit-enr.org.

## Usage

```python
from qualit_enr.qualit_enr_scraper import QualitScraper

scraper = QualitScraper()
data = scraper.scrape_region_category(category="installateurs-photovoltaique", region="75")
```

## Configuration

Edit the Config class in `qualit_enr_scraper.py` to modify scraper settings.
