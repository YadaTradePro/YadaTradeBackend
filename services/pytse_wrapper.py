# services/pytse_wrapper.py
# Wrapper for pytse_client with error handling, retries, and safe fallbacks.

import requests
import time
import logging
import pandas as pd
import pytse_client as tse
from requests.exceptions import RequestException, Timeout, ConnectionError, HTTPError

# Setting up logging for this module
logger = logging.getLogger(__name__)


# ---------------------------
# Helper to enforce HTTPS
# ---------------------------
def force_https(url: str) -> str:
    """Ensures the URL uses HTTPS."""
    if url.startswith("http://"):
        url = url.replace("http://", "https://", 1)
    return url


# ---------------------------
# HTTP GET with retries
# ---------------------------
def http_get(url, headers=None, max_retries=5, initial_delay=1, timeout=30):
    """Performs an HTTP GET request with retry logic for network robustness."""
    url = force_https(url)
    retries, delay = 0, initial_delay
    while retries < max_retries:
        try:
            resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            resp.raise_for_status()
            return resp
        except (Timeout, ConnectionError) as e:
            logger.error(f"Network error for {url}: {e}. Retrying...")
        except HTTPError as e:
            logger.error(f"HTTP {e.response.status_code} for {url}. Retrying...")
        except RequestException as e:
            logger.error(f"Request failed for {url}: {e}. Retrying...")
        retries += 1
        time.sleep(delay)
        delay *= 2
    logger.error(f"Failed to fetch {url} after {max_retries} retries.")
    return None


# ---------------------------
# pytse_client Wrappers
# ---------------------------
def Ticker(symbol_name):
    """Safe wrapper for tse.Ticker object creation."""
    try:
        return tse.Ticker(symbol_name)
    except Exception as e:
        logger.error(f"Error creating Ticker object for {symbol_name}: {e}")
        return None


def download(symbols, write_to_csv=False, adjust=True, days_limit=None):
    """Safe wrapper for tse.download with result truncation."""
    MAX_ROWS = 2000
    try:
        df = tse.download(symbols=symbols, write_to_csv=write_to_csv, adjust=adjust)
        if df is None:
            return pd.DataFrame()
        limit = days_limit if days_limit is not None else MAX_ROWS
        if isinstance(df, dict):
            return {sym: data.tail(limit) for sym, data in df.items() if isinstance(data, pd.DataFrame)}
        elif isinstance(df, pd.DataFrame):
            return df.tail(limit)
        logger.warning(f"Unexpected type from tse.download: {type(df)}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error downloading {symbols}: {e}")
        return pd.DataFrame()


def safe_download_batch(symbols, batch_size=10, days_limit=None, write_to_csv=False, output_filename="all_symbols_data.csv"):
    """
    Downloads data for a list of symbols in batches to manage memory usage.
    """
    all_data = []
    symbol_chunks = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]

    logger.info(f"Starting batch download for {len(symbols)} symbols in {len(symbol_chunks)} chunks.")

    for i, chunk in enumerate(symbol_chunks):
        logger.info(f"Processing batch {i+1}/{len(symbol_chunks)} with {len(chunk)} symbols.")
        try:
            chunk_data = download(symbols=chunk, days_limit=days_limit)
            if isinstance(chunk_data, dict) and chunk_data:
                all_data.append(pd.concat(chunk_data.values(), ignore_index=True))
            else:
                logger.warning(f"Batch {i+1} returned empty or invalid data. Skipping...")
        except Exception as e:
            logger.error(f"Error in batch {i+1}: {e}. Skipping this batch.")

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        if write_to_csv and not final_df.empty:
            try:
                final_df.to_csv(output_filename, index=False)
                logger.info(f"Successfully wrote all data to {output_filename}.")
            except Exception as e:
                logger.error(f"Failed to write data to CSV file {output_filename}: {e}")
        return final_df
    else:
        logger.error("No valid data was downloaded. Returning an empty DataFrame.")
        return pd.DataFrame()


def all_tickers():
    """
    Retrieves all tickers using tse.download("all"), with retry and timeout handling.
    """
    try:
        df = download(symbols="all", days_limit=1)
        if isinstance(df, pd.DataFrame) and not df.empty:
            tickers = {}
            for sym in df["symbol"].unique():
                t = tse.Ticker(sym)
                if t:
                    tickers[sym] = t
            logger.info(f"Initial population: Retrieved {len(tickers)} tickers.")
            return tickers
        else:
            logger.warning("download('all') returned empty")
    except Exception as e:
        logger.error(f"Error in all_tickers download: {e}")
    return {}


def download_financial_indexes_safe(symbols):
    """
    Safe and compatible wrapper for fetching financial indexes.
    It uses the original 'download_financial_indexes' function and then truncates
    the data to the last 5 rows to ensure compatibility and provide enough data
    for calculations.
    """
    # --- MODIFICATION START ---
    # Reverting to the original function call that is compatible with your library version.
    try:
        logger.info(f"Calling tse.download_financial_indexes for symbols: {symbols}")
        all_indexes_data = tse.download_financial_indexes(symbols)

        if not all_indexes_data:
            logger.warning("tse.download_financial_indexes returned no data.")
            return {}

        # Truncate each DataFrame to the last 5 rows post-download.
        # This is a safe way to limit data while ensuring we have the previous day.
        truncated_indexes = {}
        for symbol, df in all_indexes_data.items():
            if isinstance(df, pd.DataFrame):
                truncated_indexes[symbol] = df.tail(5)
            else:
                truncated_indexes[symbol] = pd.DataFrame() # Ensure it's always a DataFrame

        return truncated_indexes

    except Exception as e:
        # This will catch the AttributeError if the function is missing, or any other error.
        logger.error(f"Critical error in download_financial_indexes_safe for symbols {symbols}: {e}", exc_info=True)
        return {} # Return an empty dict on failure to prevent crashes downstream.
    # --- MODIFICATION END ---