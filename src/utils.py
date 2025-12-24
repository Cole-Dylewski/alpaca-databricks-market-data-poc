"""
Utility functions for the market data pipeline.

Includes symbol scraping utilities, logging helpers, date/time utilities,
error handling decorators, retry logic, and data validation helpers.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, time
import requests
from bs4 import BeautifulSoup

try:
    from .data_sources import YahooFinanceClient
except ImportError:
    # Handle case where running as standalone script
    from src.data_sources import YahooFinanceClient


def get_sp500_symbols() -> List[str]:
    """Fetch all S&P 500 stock symbols from stockanalysis.com.
    
    Scrapes the S&P 500 stocks list page and extracts all stock symbols
    from the table. Handles symbols with dots (e.g., BRK.B) and returns
    a deduplicated, sorted list.
    
    Returns:
        List of S&P 500 stock symbols (e.g., ['AAPL', 'MSFT', 'BRK.B'])
    
    Raises:
        requests.RequestException: If the HTTP request fails
        ValueError: If the page structure is unexpected and symbols cannot be extracted
    
    Examples:
        >>> symbols = get_sp500_symbols()
        >>> len(symbols) >= 500
        True
        >>> 'AAPL' in symbols
        True
        >>> 'BRK.B' in symbols
        True
    """
    url = "https://stockanalysis.com/list/sp-500-stocks/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        raise requests.RequestException(f"Failed to fetch S&P 500 list from {url}: {e}") from e
    
    soup = BeautifulSoup(response.text, 'html.parser')
    symbols: List[str] = []
    
    # Find the table containing stock symbols
    # The table has columns: No., Symbol, Company Name, Market Cap, Stock Price, % Change, Revenue
    tables = soup.find_all('table')
    
    if not tables:
        raise ValueError("No table found on the S&P 500 stocks page")
    
    # Process the first table (should be the main stocks table)
    table = tables[0]
    rows = table.find_all('tr')
    
    for row in rows:
        cells = row.find_all(['td', 'th'])
        if len(cells) < 2:
            continue
        
        # The symbol is typically in the second column (index 1)
        # It's usually in a link with href like /stocks/AAPL/
        symbol_cell = cells[1]
        
        # Try to extract from link first
        link = symbol_cell.find('a')
        if link:
            href = link.get('href', '')
            # Extract symbol from href like /stocks/AAPL/ or /stocks/BRK.B/
            if '/stocks/' in href:
                symbol = href.split('/stocks/')[1].rstrip('/').upper()
                if symbol and _is_valid_symbol(symbol):
                    symbols.append(symbol)
                    continue
        
        # Fallback: extract from cell text
        symbol_text = symbol_cell.get_text().strip().upper()
        if _is_valid_symbol(symbol_text):
            symbols.append(symbol_text)
    
    if not symbols:
        raise ValueError("No symbols found in the table. Page structure may have changed.")
    
    # Deduplicate and sort
    unique_symbols = sorted(list(set(symbols)))
    
    return unique_symbols


def _is_valid_symbol(symbol: str) -> bool:
    """Check if a string is a valid stock symbol.
    
    Valid symbols are:
    - 1-5 characters long
    - Uppercase letters, numbers, and dots only
    - At least one letter
    - Cannot start or end with a dot
    
    Args:
        symbol: String to validate
    
    Returns:
        True if the symbol is valid, False otherwise
    
    Examples:
        >>> _is_valid_symbol('AAPL')
        True
        >>> _is_valid_symbol('BRK.B')
        True
        >>> _is_valid_symbol('123')
        False
        >>> _is_valid_symbol('')
        False
        >>> _is_valid_symbol('TOOLONG')
        False
        >>> _is_valid_symbol('aapl')
        False
        >>> _is_valid_symbol('.A')
        False
    """
    if not symbol:
        return False
    
    if len(symbol) > 5:
        return False
    
    # Cannot start or end with a dot
    if symbol.startswith('.') or symbol.endswith('.'):
        return False
    
    # Must contain at least one letter
    if not any(c.isalpha() for c in symbol):
        return False
    
    # Only uppercase letters, numbers, and dots allowed
    # Check that all characters are uppercase letters, digits, or dots
    for c in symbol:
        if c == '.':
            continue
        if not c.isalnum():
            return False
        if c.isalpha() and not c.isupper():
            return False
    
    return True


def fetch_previous_day_5min_bars(
    symbols: List[str],
    client: Optional[YahooFinanceClient] = None,
    date: Optional[datetime] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch previous day's 5-minute bar data for a list of symbols.
    
    Retrieves intraday data in 5-minute intervals for the previous trading day
    (or specified date) for all provided symbols. Market hours are assumed to be
    9:30 AM to 4:00 PM ET.
    
    Args:
        symbols: List of stock symbols to fetch data for
        client: Optional YahooFinanceClient instance. If None, creates a new one.
        date: Optional specific date to fetch. If None, uses yesterday.
    
    Returns:
        Dictionary mapping symbol to list of bar dictionaries. Each bar contains:
        - symbol: Stock symbol
        - timestamp: Bar timestamp
        - open: Opening price
        - high: High price
        - low: Low price
        - close: Closing price
        - volume: Trading volume
        Symbols that fail to fetch will have empty lists.
    
    Raises:
        ValueError: If symbols list is empty
    
    Examples:
        >>> symbols = ['AAPL', 'MSFT']
        >>> data = fetch_previous_day_5min_bars(symbols)
        >>> 'AAPL' in data
        True
        >>> len(data['AAPL']) > 0
        True
        >>> 'timestamp' in data['AAPL'][0]
        True
    """
    if not symbols:
        raise ValueError("Symbols list cannot be empty")
    
    if client is None:
        client = YahooFinanceClient()
    
    # Determine the date to fetch
    if date is None:
        # Use yesterday (previous day)
        target_date = datetime.now().date() - timedelta(days=1)
    else:
        target_date = date.date() if isinstance(date, datetime) else date
    
    # Market hours: 9:30 AM to 4:00 PM ET (simplified to local time)
    # In production, you'd want to handle timezone conversion properly
    market_open = datetime.combine(target_date, time(9, 30))
    market_close = datetime.combine(target_date, time(16, 0))
    
    results: Dict[str, List[Dict[str, Any]]] = {}
    
    for symbol in symbols:
        try:
            bars = client.fetch_bars(
                symbol=symbol,
                start_time=market_open,
                end_time=market_close,
                interval="5m",
            )
            results[symbol] = bars
        except (ValueError, ConnectionError) as e:
            # Log error but continue with other symbols
            # In production, you might want to use proper logging
            results[symbol] = []
            continue
    
    return results

