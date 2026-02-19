import functools
import json
import os
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

import bs4
import requests

from etfpy.client._base_client import BaseClient
from etfpy.exc import InvalidETFException
from etfpy.log import get_logger
from etfpy.utils import (
    _handle_nth_child,
    _handle_spans,
    chunkify,
    get_headers,
    handle_find_all_rows,
    handle_tbody_thead,
)

logger = get_logger("etf_client")


def _load_available_etfs() -> list:
    """Loads all available tickers from etfdb.com

    Returns
    -------
    list of available etf tickers
    """
    root = Path(__file__).parent.parent.resolve()
    path = os.path.join(root, "data", "etfs", "etfs_list.json")

    with open(path, "r") as f:
        data = json.load(f)
    return data


@functools.lru_cache()
def get_available_etfs_list():
    return [etf["symbol"] for etf in _load_available_etfs()]


class ETFDBClient(BaseClient):
    def __init__(self, ticker: str, **kwargs):
        super().__init__(**kwargs)
        if ticker.upper() in get_available_etfs_list():
            self.ticker = ticker.upper()
            self.ticker_url = f"{self._base_url}/etf/{self.ticker}"
        else:
            raise InvalidETFException(f"{ticker} doesn't exist in ETF Database")

        self.asset_class = self._add_meta_information(self.ticker)
        self._soup = self._make_soup_request()

    @staticmethod
    def _add_meta_information(ticker):
        for etf in _load_available_etfs():
            if etf.get("symbol") == ticker:
                return etf.get("asset_class")
        return None

    def __repr__(self):
        return f"{self.__class__.__name__}(ticker={self.ticker})"

    def _prepare_url(
        self,
    ) -> str:
        """Builds url for given ticker."""
        return f"{self._base_url}/etf/{self.ticker}/"

    def _make_soup_request(self) -> bs4.BeautifulSoup:
        """Make GET request to etfdb.com, and put response
        into BeautifulSoup data structure.

        Returns
        -------
        BeautifulSoup object ready to parse with bs4 library
        """
        url = self._prepare_url()
        debug_path = getattr(self, "debug_html_path", None)
        text = self._fetch_html(url, debug_path=debug_path)
        return bs4.BeautifulSoup(text, "html.parser")

    def _fetch_html(self, url: str, debug_path: str = None) -> str:
        # Refresh headers to avoid stale or blocked user agents.
        self._session.headers.update(get_headers())
        # Prime session with homepage to pick up cookies and reduce bot blocks.
        try:
            self._session.get(self._base_url, allow_redirects=True)
        except Exception as exc:
            logger.debug("failed to prefetch homepage: %s", exc)

        response = self._session.get(url, allow_redirects=True)
        if response.status_code != 200:
            raise Exception(f"response {response.status_code}: {response.reason}")
        content = response.content or b""
        text = response.text or ""
        if "<html" not in text.lower():
            # Try a safer decode if requests didn't decode correctly.
            encoding = response.apparent_encoding or "utf-8"
            try:
                text = content.decode(encoding, errors="replace")
            except Exception:
                text = content.decode("utf-8", errors="replace")
        if debug_path:
            try:
                path = Path(debug_path)
                if "<html" in text.lower():
                    path.write_text(text, encoding="utf-8")
                else:
                    # Save raw content for inspection if we couldn't decode HTML.
                    path.write_bytes(content)
            except Exception as exc:
                logger.warning("failed to write debug html: %s", exc)
        # Detect common bot-protection / challenge pages.
        block_markers = [
            "Access Denied",
            "Pardon Our Interruption",
            "verify you are human",
            "captcha",
            "cloudflare",
            "distil",
        ]
        is_blocked = any(marker.lower() in text.lower() for marker in block_markers)
        if is_blocked:
            # Try Cloudflare-aware scraper if available.
            text = self._try_cloudscraper(url, debug_path=debug_path)
            if text:
                return text
            raise Exception(
                "ETFDB returned a bot-protection page. "
                "Install cloudscraper or use a browser-like session."
            )
        return text

    def _try_cloudscraper(self, url: str, debug_path: str = None) -> str:
        try:
            import cloudscraper  # type: ignore
        except Exception:
            return ""

        try:
            scraper = cloudscraper.create_scraper()
            scraper.headers.update(get_headers())
            resp = scraper.get(url, allow_redirects=True, timeout=30)
            if resp.status_code != 200:
                return ""
            text = resp.text or ""
            if debug_path:
                try:
                    Path(debug_path).write_text(text, encoding="utf-8")
                except Exception as exc:
                    logger.warning("failed to write debug html: %s", exc)
            return text
        except requests.RequestException as exc:
            logger.warning("cloudscraper request failed: %s", exc)
            return ""

    def _profile_container(self) -> dict:
        """Parses the profile container into a dictionary.

        Returns:
            A dictionary containing the profile information.
        """
        profile_container = self._soup.find("div", {"class": "profile-container"})
        if not profile_container:
            logger.warning("profile container not found for %s", self.ticker)
            return {}
        results: List[Tuple] = []
        for row in profile_container.find_all("div", class_="row"):
            spans = row.find_all("span")
            record = _handle_spans(spans)
            if record is None:
                continue
            results.append(record)
        return dict(results)

    def _trading_data(self) -> dict:
        """Parses the data-trading bar-charts-table into dictionary.

        Returns:
            A dictionary containing the trading data information.
               {
                   '52 Week Lo': '$24.80',
                   '52 Week Hi': '$30.00',
                   'AUM': '$10.0 M',
                   'Shares': '0.4 M'
               }
        """
        trading_container = self._soup.find(
            "div", {"class": "data-trading bar-charts-table"}
        )
        if not trading_container:
            logger.warning("trading data container not found for %s", self.ticker)
            return {}
        trading_data = trading_container.find_all("li")
        trading_dict = {
            _handle_nth_child(li, 1): _handle_nth_child(li, 2) for li in trading_data
        }
        return {k: v for k, v in trading_dict.items() if v != ""}

    def _asset_categories(self) -> dict:
        """Get asset categories data"""

        ticker_body = self._soup.find("div", {"id": "etf-ticker-body"})
        if not ticker_body:
            ticker_body = self._soup.find("div", id=re.compile("etf-ticker", re.I))
        if not ticker_body:
            logger.warning("asset categories not found for %s", self.ticker)
            return {}
        theme = ticker_body.find_all("div", class_="ticker-assets")
        if not theme or len(theme) < 1:
            return {}
        theme_dict = handle_find_all_rows(theme[1].find_all("div", class_="row"))
        return theme_dict

    def _factset_classification(self) -> dict:
        """Get factset information"""
        factset_container = self._soup.find("div", {"id": "factset-classification"})
        if not factset_container:
            logger.warning("factset classification not found for %s", self.ticker)
            return {}
        factset = factset_container.find_all("tr")
        factset_dict = handle_find_all_rows(factset)
        return factset_dict

    def _number_of_holdings(self) -> dict:
        """Get number of holdings for given etf"""
        return handle_tbody_thead(self._soup, "holdings-table")

    def _size_locations(self) -> dict:
        """Get size allocations of holdings for given etf"""
        return handle_tbody_thead(self._soup, "size-table")

    def _valuation(self) -> dict:
        """Get ETF valuation metrics."""
        valuation_container = self._soup.find(
            "div", {"id": "etf-ticker-valuation-dividend_tab"}
        )
        if not valuation_container:
            logger.warning("valuation container not found for %s", self.ticker)
            return {}
        valuation_section = valuation_container.find("div", {"id": "valuation"})
        if not valuation_section:
            logger.warning("valuation section not found for %s", self.ticker)
            return {}
        valuation = valuation_section.find_all("div", class_="row")
        if not valuation or len(valuation) < 2:
            logger.warning("valuation rows missing for %s", self.ticker)
            return {}
        names = [
            [
                i.text.strip()
                for i in div.find_all("div", {"class": re.compile("h4 center*")})
            ]
            for div in valuation
        ][1]
        values = [
            div.text for div in valuation[1].find_all("div", class_="text-center")
        ]
        results = defaultdict(dict)
        for name, (k, v) in zip(names, chunkify(values, 2)):
            results[k][name] = v
        return dict(results)

    def _dividends(self) -> Dict:
        """Get ETF dividend information."""
        return handle_tbody_thead(self._soup, "dividend-table", tag="div")

    def _holdings(self) -> List[Dict]:
        """Get ETF holdings information."""
        results = []
        try:
            tbody = self._soup.find("div", {"id": "holding_section"}).find("tbody")
            holdings = list(tbody.find_all("tr"))
            for record in holdings:
                record_texts = record.find_all("td")
                try:
                    holding_url = record.find("a")["href"]
                except TypeError:
                    holding_url = ""
                texts = dict(
                    zip(["Symbol", "Holding", "Share"], [x.text for x in record_texts])
                )
                holding_url = (
                    f"{self._base_url}{holding_url}"
                    if self._base_url not in holding_url
                    else holding_url
                )
                texts.update(
                    {"Url": "" if holding_url == self._base_url else holding_url}
                )
                results.append(texts)
        except AttributeError:
            results = []
        return results

    def _performance(self) -> Dict:
        """Get ETF performance."""
        performance = handle_tbody_thead(self._soup, "performance-collapse", tag="div")
        cleaned_dict = {}
        try:
            for outer_key, _ in performance.items():
                cleaned_dict[outer_key] = {}
                for inner_key, inner_value in performance[outer_key].items():
                    new_key = inner_key.replace("\n\n", " ")
                    cleaned_dict[outer_key][new_key] = inner_value
        except (KeyError, AttributeError) as kae:
            logger.warning("couldn't clean performance dict %s", kae)
        return cleaned_dict

    def _technicals(self) -> Dict:
        """Get technical analysis indicators for etf."""
        technicals_container = self._soup.find("div", {"id": "technicals-collapse"})
        if not technicals_container:
            logger.warning("technicals container not found for %s", self.ticker)
            return {}
        sections = list(
            technicals_container.find_all("ul", class_="list-unstyled")
        )

        results = []
        for section in sections:
            try:
                results += [s.text.strip().split("\n") for s in section.find_all("li")]
            except (KeyError, TypeError) as e:
                logger.error(e)
        return dict(results)

    def _volatility(self) -> Dict:
        """Get Volatility  information."""
        technicals_container = self._soup.find("div", {"id": "technicals-collapse"})
        if not technicals_container:
            logger.warning("volatility container not found for %s", self.ticker)
            return {}
        metrics = [
            x.text.strip().split("\n\n\n\n")
            for x in technicals_container.find_all(
                "div", class_=re.compile("row relative-metric")
            )
        ]
        return dict(metrics)

    def _exposure(self) -> Dict:
        """Get ETF exposure information."""
        charts_data = self._soup.find_all("table", class_="chart base-table")
        if not charts_data:
            return {"Data": "Region, country, sector breakdown data not found"}
        parse_data = []
        chart_series = [x.get("data-chart-series") for x in charts_data]
        chart_titles = [x.get("data-title").replace("<br>", " ") for x in charts_data]
        chart_series_dicts = [json.loads(series) for series in chart_series]
        for chart_dict in chart_series_dicts:
            parse_data.append({x["name"]: x["data"][0] for x in chart_dict})

        return dict(zip(chart_titles, parse_data))

    def _prepare_esg_urls(self) -> List[str]:
        base = f"{self._base_url}/etf/{self.ticker}/"
        return [
            f"{base}esg/",
            f"{base}esg",
        ]

    def _esg_soup(self) -> bs4.BeautifulSoup:
        debug_path = getattr(self, "debug_esg_path", None)
        if self._soup.find(id=re.compile("esg", re.I)) or self._soup.find(
            class_=re.compile("esg", re.I)
        ):
            return self._soup
        for url in self._prepare_esg_urls():
            try:
                text = self._fetch_html(url, debug_path=debug_path)
            except Exception as exc:
                logger.debug("failed to fetch esg url %s: %s", url, exc)
                continue
            soup = bs4.BeautifulSoup(text, "html.parser")
            if soup.find(id=re.compile("esg", re.I)) or soup.find(
                class_=re.compile("esg", re.I)
            ):
                return soup
        logger.warning("esg page not found for %s", self.ticker)
        return self._soup

    def _parse_esg_blocks(self, soup: bs4.BeautifulSoup) -> Dict:
        esg_tab = soup.find(id=re.compile(r"esg(_tab)?", re.I)) or soup.find(
            class_=re.compile("esg", re.I)
        )
        if not esg_tab:
            return {}

        results: Dict[str, Dict] = {}

        score_blocks = esg_tab.select(".general-list .score-block")
        scores: Dict[str, str] = {}
        for block in score_blocks:
            name_el = block.select_one(".score-name")
            value_el = block.select_one(".score")
            if not name_el or not value_el:
                continue
            name = name_el.get_text(strip=True)
            value = value_el.get_text(strip=True)
            if name:
                scores[name] = value
        if scores:
            results["scores"] = scores

        theme_content = esg_tab.select_one(".esg-theme-content") or esg_tab
        theme_ids = {
            "environmental-issues": "Environmental",
            "social-issues": "Social",
            "governance-issues": "Governance",
        }
        themes: Dict[str, Dict[str, Dict[str, str]]] = {}
        for theme_id, theme_name in theme_ids.items():
            theme_section = theme_content.find(id=theme_id) if theme_content else None
            if not theme_section:
                continue
            theme_data: Dict[str, Dict[str, str]] = {}
            for header in theme_section.find_all(
                "div", class_=re.compile(r"\bclick-show-hide\b")
            ):
                title_el = header.find("a")
                title = title_el.get_text(strip=True) if title_el else ""
                if not title:
                    continue
                detail_list = header.find_next_sibling(
                    "ul", class_=re.compile(r"\blist-indent\b")
                )
                metrics: Dict[str, str] = {}
                if detail_list:
                    for item in detail_list.select("div.data-column-esg"):
                        row = item.select_one(".esg-colum-row")
                        if not row:
                            continue
                        label_el = row.find("span")
                        value_el = row.select_one(".pull-right span")
                        label = label_el.get_text(strip=True) if label_el else ""
                        value = value_el.get_text(strip=True) if value_el else ""
                        if label:
                            metrics[label] = value
                if metrics:
                    theme_data[title] = metrics
            if theme_data:
                themes[theme_name] = theme_data

        if themes:
            results["themes"] = themes

        return results

    def _esg(self) -> Dict:
        """Get ESG information for given ETF."""
        soup = self._esg_soup()
        results: Dict[str, Dict[str, str]] = {}

        container = soup.find(id=re.compile("esg", re.I)) or soup.find(
            class_=re.compile("esg", re.I)
        )
        tables = []
        if container:
            tables = container.find_all("table")
        else:
            tables = soup.find_all("table", id=re.compile("esg", re.I))

        for table in tables:
            table_id = table.get("id")
            if table_id:
                data = handle_find_all_rows(table.find_all("tr"))
                if data:
                    results[table_id] = data

        if not results:
            # Fallback: try known table ids directly on the full soup.
            for table_id in [
                "esg-table",
                "esg-ratings-table",
                "esg-score-table",
                "esg-scores",
            ]:
                if soup.find("table", {"id": table_id}):
                    data = handle_tbody_thead(soup, table_id)
                    if data:
                        results[table_id] = data

        if not results:
            results = self._parse_esg_blocks(soup)

        return results

    def _description(self) -> str:
        """Get textual description for given ETF."""
        soup = self._soup
        candidates = [
            soup.find("div", {"id": "full-content"}),
            soup.find("div", {"id": "etf-description"}),
            soup.find("div", {"id": "etf-desc"}),
            soup.find("div", class_="etf-description"),
            soup.find("div", class_="description"),
            soup.find("section", {"id": "description"}),
            soup.find("div", class_="etf-summary"),
        ]
        for candidate in candidates:
            if candidate:
                text = candidate.get_text(" ", strip=True)
                if text:
                    return text

        for heading in soup.find_all(["h2", "h3", "h4"]):
            title = heading.get_text(" ", strip=True).lower()
            if "description" in title:
                block = heading.find_next(["p", "div"])
                if block:
                    text = block.get_text(" ", strip=True)
                    if text:
                        return text
        meta_candidates = [
            soup.find("meta", attrs={"name": "description"}),
            soup.find("meta", attrs={"property": "og:description"}),
            soup.find("meta", attrs={"name": "twitter:description"}),
        ]
        for meta in meta_candidates:
            if meta and meta.get("content"):
                content = meta.get("content", "").strip()
                if content:
                    return content
        return ""

    def _basic_info(self) -> Dict:
        """Gets basic information about ETF.
        Like profile information, trading data, valuation, assets etc.
        """
        ticker_body = self._soup.find("div", {"id": "etf-ticker-body"})
        if not ticker_body:
            ticker_body = self._soup.find("div", id=re.compile("etf-ticker", re.I))
        basic_information = {"Symbol": self.ticker, "Url": self.ticker_url}

        if not ticker_body:
            logger.warning("etf ticker body not found for %s", self.ticker)
        else:
            etf_ticker_body = ticker_body.find("div", class_="row")
            if not etf_ticker_body:
                logger.warning("etf ticker rows not found for %s", self.ticker)
            else:
                for row in etf_ticker_body.find_all("div", class_="row"):
                    key = _handle_nth_child(row, 1)
                    value = row.select_one(":nth-child(2)")
                    try:
                        href = value.find("a")["href"]
                        if href and key != "ETF Home Page":
                            value_text = (
                                href
                                if href.startswith(self._base_url)
                                else self._base_url + href
                            )
                        else:
                            value_text = href
                    except (KeyError, TypeError, AttributeError):
                        value_text = value.text.strip() if value else ""

                    if key == "ETF Home Page" and value_text.startswith(self._base_url):
                        value_text.replace(self._base_url, "")

                    if key:
                        basic_information.update({key: value_text})

        basic_information.update(self._profile_container())
        basic_information.update(self._valuation())
        basic_information.update(self._trading_data())
        basic_information.update(self._asset_categories())
        basic_information.update(self._factset_classification())
        return basic_information
