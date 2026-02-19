from typing import Any, Dict

import pandas as pd
import requests
from requests import HTTPError
from requests.exceptions import JSONDecodeError

from etfpy.log import get_logger
from etfpy.utils import get_headers, get_retry_session

logger = get_logger(__name__)


class BaseClient:
    """Base client for interacting with the etfdb API.

    Parameters
    ----------
    kwargs: Any
        Additional keyword arguments to pass to the client.

    Attributes
    ----------
    _base_url: str
        The base URL for the etfdb API.
    _api_url: str
        The URL for the etfdb screener API.
    _request_session: requests.Session
        A session object used to make all requests.
    """

    def __init__(self, **kwargs: Any):
        self._base_url = "https://etfdb.com"
        self._api_url = f"{self._base_url}/api/screener/"
        self._quotes_url = (
            "https://etfflows.websol.barchart.com/proxies/timeseries/queryeod.ashx"
        )
        self._requests_session = get_retry_session()

        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def _session(self) -> requests.Session:
        """Returns the request session object."""
        return self._requests_session

    @staticmethod
    def _prepare_request_body(
        page: int = 1, page_size: int = 250, **kwargs: Any
    ) -> Dict:
        """Prepares the request body for a screener request.

        Parameters
        ----------
        page: int, default=1
            The page number to request.
        page_size: int, default=250
            The number of results per page to request.
        kwargs: Any
            Additional keyword arguments to pass to the request.

        Returns
        -------
        Dict
            The request body.

        Raises
        ------
        ValueError
            If the page number is less than 1.
        """

        if page < 1:
            raise ValueError("page param needs to be positive number")
        body = {
            "page": page,
            "per_page": page_size,
            "only": ["meta", "data", None],
        }
        body.update(**kwargs)
        return body

    def post_request(self, request_body: Dict) -> requests.Response:
        """Posts a request to the ETFDB screener API.

        Parameters
        ----------
        request_body: Dict
            The request body.

        Returns
        -------
        requests.Response
            The response object.
        """
        return self._session.post(
            self._api_url, json=request_body, headers=get_headers()
        )

    @staticmethod
    def _looks_blocked(text: str) -> bool:
        if not text:
            return False
        markers = [
            "Access Denied",
            "Pardon Our Interruption",
            "verify you are human",
            "captcha",
            "cloudflare",
            "distil",
        ]
        text_lower = text.lower()
        return any(marker.lower() in text_lower for marker in markers)

    def _post_request_json(self, request_body: Dict) -> Dict:
        response = self.post_request(request_body)
        try:
            return response.json()
        except (ValueError, JSONDecodeError):
            if not self._looks_blocked(response.text):
                logger.warning("failed to decode ETFDB response JSON")
            return self._post_request_json_cloudscraper(request_body)

    def _post_request_json_cloudscraper(self, request_body: Dict) -> Dict:
        try:
            import cloudscraper  # type: ignore
        except Exception:
            return {}

        try:
            scraper = cloudscraper.create_scraper()
            scraper.headers.update(get_headers())
            resp = scraper.post(self._api_url, json=request_body, timeout=30)
            if resp.status_code != 200:
                return {}
            return resp.json()
        except Exception as exc:
            logger.warning("cloudscraper post failed: %s", exc)
            return {}

    def get_metadata(self) -> Dict:
        """Gets the metadata for the ETFDB screener API.

        Returns
        -------
        Dict
            The metadata dictionary.
        """

        try:
            return self._post_request_json(self._prepare_request_body())
        except HTTPError as he:
            logger.error(str(he))
        except AttributeError as ae:
            logger.error(str(ae))
        return {}

    def _get_quotes(self, ticker: str, interval="daily", periods=360, order="asc"):
        assert interval in [
            "monthly",
            "daily",
            "yearly",
            "quarterly",
        ], "interval should be on of these: daily, monthly, yearly, quarterly"
        if order not in ["asc", "desc"]:
            logger.warning(
                "order should be one of these: asc, desc - defaulting to asc"
            )
            order = "asc"

        query_params = {
            "symbol": ticker,
            "data": interval,
            "maxrecords": periods,
            "volume": "contract",
            "order": order,
            "dividends": "false",
            "backadjust": "false",
            "daystoexpiration": 1,
            "contractroll": "expiration",
        }

        r = self._session.get(self._quotes_url, params=query_params)

        headers = ["symbol", "date", "open", "high", "low", "close", "volume"]
        try:
            data = list(x.split(",") for x in r.text.split("\n") if len(x) > 1)
        except (AttributeError, TypeError) as ate:
            logger.error("couldn't convert response do dataframe: %s", str(ate))
            return pd.DataFrame(columns=headers)

        df = pd.DataFrame(data, columns=headers)
        df = df.astype(
            {
                "symbol": "str",
                "volume": int,
                "open": "float64",
                "close": "float64",
                "high": "float64",
                "low": "float64",
            }
        )
        df["date"] = pd.to_datetime(df["date"]).dt.date
        return df
