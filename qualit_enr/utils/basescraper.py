import csv
import inspect
import json
import logging
import os
import random
import re
import time
from datetime import datetime
from typing import Optional, Dict, Any, Tuple
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup
from curl_cffi import requests as curl_requests
from requests import Session
from retry import retry
from slugify import slugify


class BaseScraper:
    """
    A comprehensive base scraper class with common web scraping functionality.
    Features include:
    - Request management with retries
    - Both regular requests and curl_cffi support
    - Cookie and header management
    - BeautifulSoup integration
    - CSV/JSON file operations
    - Proxy support
    - Rate limiting
    - Common parsing utilities
    - Comprehensive logging
    """

    def __init__(
            self,
            site_name: str,
            base_url: Optional[str] = None,
            use_curl: bool = False,
            default_headers: Optional[Dict] = None,
            default_cookies: Optional[Dict] = None,
            request_delay: Optional[Tuple[float, float]] = (1.0, 3.0),
            max_retries: int = 3,
            log_level: int = logging.INFO,
            log_file: Optional[str] = None
    ):
        """
        Initialize the scraper with common settings.

        Args:
            site_name: Name of the website being scraped
            base_url: Base URL for the website
            use_curl: Whether to use curl_cffi for requests
            default_headers: Default headers to use for requests
            default_cookies: Default cookies to use for requests
            request_delay: Min/max delay between requests in seconds
            max_retries: Maximum number of retry attempts for failed requests
            log_level: Logging level (e.g., logging.INFO, logging.DEBUG)
            log_file: Optional file path to save logs
        """
        self.site_name = site_name
        self.base_url = base_url.rstrip('/') if base_url else None
        self.use_curl = use_curl
        self.session = Session()
        self.default_headers = default_headers or {}
        self.default_cookies = default_cookies or {}
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.request_count = 0

        # Configure logging
        self.logger = logging.getLogger(f"{self.__class__.__name__}_{self.site_name}")
        self.logger.setLevel(log_level)

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Console handler
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

        # File handler if specified
        if log_file:
            fh = logging.FileHandler(log_file)
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)

        # Configure default headers if not provided
        if not self.default_headers.get('User-Agent'):
            self.default_headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
            })

        self.logger.info(f"Initialized scraper for {self.site_name} with base URL: {self.base_url}")

    def _random_delay(self):
        """Sleep for a random time between the configured min/max delay."""
        if self.request_delay and self.request_delay[1] > 0:
            delay = random.uniform(*self.request_delay)
            self.logger.info(f"Sleeping for {delay:.2f} seconds")
            time.sleep(delay)

    def _log_request(self, url: str, method: str, attempt: int):
        """Log request details."""
        caller_frame = inspect.currentframe().f_back.f_back
        caller_info = inspect.getframeinfo(caller_frame)
        caller_name = caller_info.function

        self.logger.debug(
            f"Making {method} request to {url} (attempt {attempt}) "
            f"called from {caller_name} at line {caller_info.lineno}"
        )

    @retry(tries=3, delay=1, backoff=2, logger=None)
    def make_request(
            self,
            url: str,
            method: str = 'GET',
            headers: Optional[Dict] = None,
            cookies: Optional[Dict] = None,
            params: Optional[Dict] = None,
            data: Optional[Dict] = None,
            json_data: Optional[Dict] = None,
            allow_redirects: bool = True,
            timeout: int = 100,
            **kwargs
    ) -> requests.Response:
        """
        Make an HTTP request with retry logic and random delays.

        Args:
            url: URL to request
            method: HTTP method (GET, POST, etc.)
            headers: Additional headers
            cookies: Additional cookies
            params: Query parameters
            data: Form data
            json_data: JSON payload
            allow_redirects: Whether to follow redirects
            timeout: Request timeout in seconds
            **kwargs: Additional arguments for requests

        Returns:
            requests.Response object

        Raises:
            requests.exceptions.RequestException: If the request fails after all retries
        """
        self._random_delay()
        self.request_count += 1

        # Get the current attempt number, defaulting to 1 if not set
        attempt = kwargs.pop('_attempt', 1)

        merged_headers = {**self.default_headers, **(headers or {})}
        merged_cookies = {**self.default_cookies, **(cookies or {})}

        if not url.startswith(('http://', 'https://')) and self.base_url:
            url = urljoin(self.base_url, url)

        self._log_request(url, method, attempt)

        try:
            start_time = time.time()

            if self.use_curl:
                response = curl_requests.request(
                    method,
                    url,
                    headers=merged_headers,
                    cookies=merged_cookies,
                    params=params,
                    data=data,
                    json=json_data,
                    allow_redirects=allow_redirects,
                    timeout=timeout,
                    impersonate="chrome124",
                    **kwargs
                )
            else:
                response = self.session.request(
                    method,
                    url,
                    headers=merged_headers,
                    cookies=merged_cookies,
                    params=params,
                    data=data,
                    json=json_data,
                    allow_redirects=allow_redirects,
                    timeout=timeout,
                    **kwargs
                )

            elapsed = time.time() - start_time
            self.logger.info(
                f"Request #{self.request_count} to {url} completed in {elapsed:.2f}s - "
                f"Status: {response.status_code} - Size: {len(response.content)} bytes"
            )

            response.raise_for_status()
            return response

        except requests.exceptions.RequestException as e:
            self.logger.error(
                f"Request failed (attempt {attempt}): {str(e)} - URL: {url}",
                exc_info=self.logger.level <= logging.DEBUG
            )

            # If we're going to retry, increment the attempt number
            if attempt < self.max_retries:
                kwargs['_attempt'] = attempt + 1
            raise
        except Exception as e:
            self.logger.error(
                f"Unexpected error during request (attempt {attempt}): {str(e)} - URL: {url}",
                exc_info=self.logger.level <= logging.DEBUG
            )
            raise requests.exceptions.RequestException(f"Unexpected error: {str(e)}")

    def get_soup(self, html_content: str, parser: str = 'lxml') -> BeautifulSoup:
        """
        Parse HTML content with BeautifulSoup.

        Args:
            html_content: HTML string to parse
            parser: Parser to use (lxml, html.parser, etc.)

        Returns:
            BeautifulSoup object
        """
        self.logger.debug(f"Parsing HTML content with {parser} parser")
        return BeautifulSoup(html_content, parser)

    from typing import List, Union, Dict, Optional

    def save_to_csv(
            self,
            data: Union[Dict, List[Union[List, Dict]]],
            filename: str,
            fieldnames: Optional[List[str]] = None,
            mode: str = 'a'
    ) -> None:
        """
        Save data to a CSV file.

        Args:
            data: List of rows (as lists) or dicts
            filename: Output file path
            fieldnames: Column names (required if data is dict)
            mode: File mode ('w' for write, 'a' for append)

        Raises:
            ValueError: If data format is invalid
        """
        try:
            if not data:
                self.logger.warning("No data provided to save_to_csv")
                return

            if isinstance(data, dict):
                data = [data]
                self.logger.debug("Converted single dict to list of dicts")

            self.logger.info(f"Saving data to CSV file: {filename}")

            # Determine headers
            if isinstance(data[0], dict):
                fieldnames = fieldnames or list(data[0].keys())
            elif isinstance(data[0], (list, tuple)):
                if not fieldnames:
                    raise ValueError("Fieldnames required for list data")

            header_needed = True
            file_exists = os.path.isfile(filename)
            write_mode = mode

            # Check if we need to add headers when appending
            if mode == 'a' and file_exists:
                # Check if file is empty
                if os.path.getsize(filename) > 0:
                    try:
                        with open(filename, 'r', encoding='utf-8-sig', newline='') as f:
                            reader = csv.reader(f)
                            existing_header = next(reader, None)

                            # If the header exists and matches our fieldnames, no header needed
                            if existing_header and existing_header == fieldnames:
                                header_needed = False
                            # If the header doesn't match or doesn't exist, we need to rewrite the file
                            else:
                                # Read all existing data
                                f.seek(0)  # Go back to beginning of file
                                existing_data = list(reader)

                                # If there was a mismatched header, we need to preserve it as data
                                if existing_header and existing_header != fieldnames:
                                    existing_data.insert(0, existing_header)

                                # We'll rewrite the file with the correct header
                                write_mode = 'w'

                                # After writing the header, we'll add all existing data first
                                temp_data = existing_data.copy()
                                temp_data.extend(data)
                                data = temp_data
                    except Exception as e:
                        self.logger.warning(f"Error checking existing CSV header: {str(e)}")
                        # Default to adding header if there's an issue reading the file
                        header_needed = True
                        write_mode = 'w'
                else:
                    # File exists but is empty, we need a header
                    header_needed = True

            # Open file for append/write
            with open(filename, write_mode, encoding='utf-8-sig', newline='') as f:
                if isinstance(data[0], dict):
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    if write_mode == 'w' or header_needed:
                        writer.writeheader()
                    writer.writerows(data)
                elif isinstance(data[0], (list, tuple)):
                    writer = csv.writer(f)
                    if (write_mode == 'w' or header_needed) and fieldnames:
                        writer.writerow(fieldnames)
                    writer.writerows(data)
                else:
                    raise ValueError("Data must be a list of dictionaries or lists")

            self.logger.debug(f"Successfully saved data to {filename}")
        except Exception as e:
            self.logger.error(f"Failed to save CSV file {filename}: {str(e)}")
            raise

    def save_to_json(
            self,
            data: Union[Dict, List],
            filename: str,
            indent: int = 2,
            mode: str = 'w'
    ) -> None:
        """
        Save data to a JSON file.

        Args:
            data: Data to save (dict or list)
            filename: Output file path
            indent: JSON indentation level
            mode: File mode ('w' for write, 'a' for append)
        """
        try:
            self.logger.info(f"Saving data to JSON file: {filename}")

            with open(filename, mode, encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=indent)

            self.logger.debug(f"Successfully saved data to {filename}")
        except Exception as e:
            self.logger.error(f"Failed to save JSON file {filename}: {str(e)}")
            raise

    def extract_table_data(self, table: BeautifulSoup) -> List[List[str]]:
        """
        Extract data from an HTML table.

        Args:
            table: BeautifulSoup table element

        Returns:
            List of rows with cell data
        """
        rows = []
        for tr in table.find_all('tr'):
            row = [td.get_text(strip=True) for td in tr.find_all(['td', 'th'])]
            if row:
                rows.append(row)

        self.logger.debug(f"Extracted {len(rows)} rows from table")
        return rows

    def extract_json_ld(self, soup: BeautifulSoup) -> Optional[Dict]:
        """
        Extract JSON-LD data from a page.

        Args:
            soup: BeautifulSoup object

        Returns:
            Parsed JSON-LD data or None
        """
        script = soup.find('script', type='application/ld+json')
        if script:
            try:
                data = json.loads(script.string)
                self.logger.debug("Successfully extracted JSON-LD data")
                return data
            except json.JSONDecodeError as e:
                self.logger.warning(f"Failed to parse JSON-LD: {str(e)}")
                return None
        return None

    def paginate(
            self,
            base_url: str,
            page_param: str = 'page',
            start_page: int = 1,
            max_pages: int = 100,
            stop_condition: Optional[callable] = None,
            **kwargs
    ) -> List[Any]:
        """
        Paginate through a series of pages.

        Args:
            base_url: Base URL with optional {page} placeholder
            page_param: Query parameter name for page number
            start_page: First page number
            max_pages: Maximum number of pages to fetch
            stop_condition: Function to determine when to stop paginating
            **kwargs: Additional arguments for make_request

        Returns:
            List of results from all pages
        """
        results = []
        page = start_page
        has_more = True

        self.logger.info(f"Starting pagination from page {start_page} to max {max_pages}")

        while has_more and page <= max_pages:
            self.logger.debug(f"Processing page {page}")

            url = base_url.format(page=page) if '{page}' in base_url else base_url
            params = kwargs.pop('params', {})
            params[page_param] = page

            try:
                response = self.make_request(url, params=params, **kwargs)
                page_results = self.process_page(response)
                results.extend(page_results)

                self.logger.info(f"Page {page} processed - {len(page_results)} items found")

                if stop_condition and stop_condition(response, page_results):
                    self.logger.info(f"Stop condition met at page {page}")
                    has_more = False

                page += 1
            except Exception as e:
                self.logger.error(f"Error processing page {page}: {str(e)}")
                if page >= max_pages:
                    break
                raise

        self.logger.info(f"Pagination completed - {len(results)} total items collected")
        return results

    def process_page(self, response: requests.Response) -> List[Any]:
        """
        Process a page response - to be implemented by subclasses.

        Args:
            response: requests.Response object

        Returns:
            List of extracted items from the page
        """
        raise NotImplementedError("Subclasses must implement process_page")

    def extract_phone_numbers(self, text: str) -> List[str]:
        """
        Extract phone numbers from text using regex.

        Args:
            text: Text to search for phone numbers

        Returns:
            List of found phone numbers
        """
        phone_regex = r'(\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
        numbers = re.findall(phone_regex, text)
        self.logger.debug(f"Extracted {len(numbers)} phone numbers from text")
        return numbers

    def extract_emails(self, text: str) -> List[str]:
        """
        Extract email addresses from text using regex.

        Args:
            text: Text to search for emails

        Returns:
            List of found email addresses
        """
        email_regex = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        emails = re.findall(email_regex, text)
        self.logger.debug(f"Extracted {len(emails)} emails from text")
        return emails

    @staticmethod
    def clean_text(text: str, preserve_newlines: bool = False) -> str:
        """
        Clean and normalize text by removing extra whitespace and special characters.

        Args:
            text: Text to clean
            preserve_newlines: Whether to keep newline characters

        Returns:
            Cleaned text
        """
        if not text:
            return ''

        # Replace multiple spaces with single space
        text = re.sub(r'\s+', ' ', text)

        if not preserve_newlines:
            # Replace newlines with spaces
            text = text.replace('\n', ' ').replace('\r', ' ')

        # Strip leading/trailing whitespace
        return text.strip()

    @staticmethod
    def slugify_text(string):
        """
        Slugify the Given String

        Args:
            string: URL to parse

        Returns:
            Optimized string
        """
        return slugify(string)

    def parse_url_params(self, url: str) -> Dict[str, List[str]]:
        """
        Parse query parameters from a URL.

        Args:
            url: URL to parse

        Returns:
            Dictionary of query parameters
        """
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        self.logger.debug(f"Parsed URL parameters: {params}")
        return params

    def get_absolute_url(self, relative_url: str) -> str:
        """
        Convert a relative URL to absolute using the base_url.

        Args:
            relative_url: Relative URL path

        Returns:
            Absolute URL

        Raises:
            ValueError: If base_url is not set
        """
        if not self.base_url:
            raise ValueError("base_url must be set to get absolute URLs")

        absolute_url = urljoin(self.base_url, relative_url)
        self.logger.debug(f"Converted relative URL {relative_url} to absolute URL {absolute_url}")
        return absolute_url

    def get_request_stats(self) -> Dict[str, Any]:
        """
        Get statistics about requests made by this scraper.

        Returns:
            Dictionary with request statistics
        """
        return {
            'total_requests': self.request_count,
            'timestamp': datetime.now().isoformat(),
            'site_name': self.site_name,
            'base_url': self.base_url
        }
