"""
Tests for utility functions.
"""

import pytest
from unittest.mock import Mock, patch
from typing import List

from src.utils import get_sp500_symbols, _is_valid_symbol


class TestGetSp500Symbols:
    """Test suite for get_sp500_symbols function."""

    @patch('src.utils.requests.get')
    def test_get_sp500_symbols_success(self, mock_get: Mock) -> None:
        """Test successful extraction of S&P 500 symbols."""
        # Mock HTML response with sample S&P 500 table
        mock_html = """
        <html>
        <body>
        <table>
        <tr>
            <th>No.</th>
            <th>Symbol</th>
            <th>Company Name</th>
        </tr>
        <tr>
            <td>1</td>
            <td><a href="/stocks/AAPL/">AAPL</a></td>
            <td>Apple Inc.</td>
        </tr>
        <tr>
            <td>2</td>
            <td><a href="/stocks/MSFT/">MSFT</a></td>
            <td>Microsoft Corporation</td>
        </tr>
        <tr>
            <td>3</td>
            <td><a href="/stocks/BRK.B/">BRK.B</a></td>
            <td>Berkshire Hathaway Inc.</td>
        </tr>
        <tr>
            <td>4</td>
            <td>GOOG</td>
            <td>Alphabet Inc.</td>
        </tr>
        </table>
        </body>
        </html>
        """
        
        mock_response = Mock()
        mock_response.text = mock_html
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        symbols = get_sp500_symbols()
        
        assert isinstance(symbols, list)
        assert len(symbols) == 4
        assert 'AAPL' in symbols
        assert 'MSFT' in symbols
        assert 'BRK.B' in symbols
        assert 'GOOG' in symbols
        assert symbols == sorted(symbols)  # Should be sorted

    @patch('src.utils.requests.get')
    def test_get_sp500_symbols_deduplication(self, mock_get: Mock) -> None:
        """Test that duplicate symbols are removed."""
        mock_html = """
        <html>
        <body>
        <table>
        <tr>
            <th>No.</th>
            <th>Symbol</th>
        </tr>
        <tr>
            <td>1</td>
            <td><a href="/stocks/AAPL/">AAPL</a></td>
        </tr>
        <tr>
            <td>2</td>
            <td><a href="/stocks/AAPL/">AAPL</a></td>
        </tr>
        </table>
        </body>
        </html>
        """
        
        mock_response = Mock()
        mock_response.text = mock_html
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        symbols = get_sp500_symbols()
        
        assert len(symbols) == 1
        assert symbols == ['AAPL']

    @patch('src.utils.requests.get')
    def test_get_sp500_symbols_no_table(self, mock_get: Mock) -> None:
        """Test that ValueError is raised when no table is found."""
        mock_html = "<html><body><p>No table here</p></body></html>"
        
        mock_response = Mock()
        mock_response.text = mock_html
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        with pytest.raises(ValueError, match="No table found"):
            get_sp500_symbols()

    @patch('src.utils.requests.get')
    def test_get_sp500_symbols_no_symbols(self, mock_get: Mock) -> None:
        """Test that ValueError is raised when no symbols are found."""
        mock_html = """
        <html>
        <body>
        <table>
        <tr>
            <th>No.</th>
            <th>Symbol</th>
        </tr>
        <tr>
            <td>1</td>
            <td>Invalid Symbol Here</td>
        </tr>
        </table>
        </body>
        </html>
        """
        
        mock_response = Mock()
        mock_response.text = mock_html
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        with pytest.raises(ValueError, match="No symbols found"):
            get_sp500_symbols()

    @patch('src.utils.requests.get')
    def test_get_sp500_symbols_request_exception(self, mock_get: Mock) -> None:
        """Test that RequestException is raised when HTTP request fails."""
        mock_get.side_effect = Exception("Connection error")
        
        with pytest.raises(Exception, match="Connection error"):
            get_sp500_symbols()


class TestIsValidSymbol:
    """Test suite for _is_valid_symbol function."""

    def test_is_valid_symbol_valid(self) -> None:
        """Test valid symbols."""
        assert _is_valid_symbol('AAPL') is True
        assert _is_valid_symbol('MSFT') is True
        assert _is_valid_symbol('BRK.B') is True
        assert _is_valid_symbol('GOOGL') is True
        assert _is_valid_symbol('A') is True
        assert _is_valid_symbol('ABC12') is True

    def test_is_valid_symbol_invalid(self) -> None:
        """Test invalid symbols."""
        assert _is_valid_symbol('') is False
        assert _is_valid_symbol('123') is False  # No letters
        assert _is_valid_symbol('TOOLONG') is False  # Too long
        assert _is_valid_symbol('aapl') is False  # Lowercase (though we uppercase before calling)
        assert _is_valid_symbol('AAP-L') is False  # Invalid character
        assert _is_valid_symbol('AAP L') is False  # Space not allowed

    def test_is_valid_symbol_edge_cases(self) -> None:
        """Test edge cases for symbol validation."""
        assert _is_valid_symbol('A1') is True
        assert _is_valid_symbol('1A') is True
        assert _is_valid_symbol('A.B') is True
        assert _is_valid_symbol('.A') is False  # Starts with dot
        assert _is_valid_symbol('A.') is False  # Ends with dot

