"""
API Client for fetching vehicle message data from the Upstream API.
"""
import requests
import logging
from typing import List, Dict
from datetime import datetime


class APIClient:
    """Client for fetching data from the Upstream vehicle messages API."""
    
    def __init__(self, base_url: str = "http://localhost:9900", timeout: int = 30):
        """
        Initialize the API client.
        
        Args:
            base_url: Base URL of the API
            timeout: Request timeout in seconds
        """
        self.base_url = base_url
        self.timeout = timeout
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def fetch_vehicle_messages(self, amount: int = 10000) -> List[Dict]:
        """
        Fetch vehicle messages from the API.
        
        Args:
            amount: Number of messages to fetch
            
        Returns:
            List of vehicle message dictionaries
            
        Raises:
            requests.RequestException: If API request fails
            ValueError: If response data is invalid
        """
        url = f"{self.base_url}/upstream/vehicle_messages"
        params = {"amount": amount}
        
        self.logger.info(f"Fetching {amount} vehicle messages from {url}")
        
        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            
            if not isinstance(data, list):
                raise ValueError("Expected list of messages from API")
                
            self.logger.info(f"Successfully fetched {len(data)} messages")
            
            # Add fetch timestamp to each record
            fetch_timestamp = datetime.utcnow().isoformat()
            for record in data:
                record['fetch_timestamp'] = fetch_timestamp
                
            return data
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch data from API: {e}")
            raise
        except ValueError as e:
            self.logger.error(f"Invalid response data: {e}")
            raise
    
    def fetch_vehicle_messages_paged(self, total_amount: int, batch_size: int = 1000) -> List[Dict]:
        """
        Fetch vehicle messages using simple client-side pagination.
        
        Args:
            total_amount: Total number of messages to fetch (upper bound)
            batch_size: Number of messages per API request
        
        Returns:
            Aggregated list of vehicle message dictionaries
        """
        if total_amount <= 0:
            return []
        if batch_size <= 0:
            batch_size = 1
        
        aggregated: List[Dict] = []
        remaining = total_amount
        
        while remaining > 0:
            current_batch = min(batch_size, remaining)
            self.logger.info(f"Pagination fetch: requesting {current_batch} messages (remaining {remaining})")
            batch = self.fetch_vehicle_messages(amount=current_batch)
            aggregated.extend(batch)
            # If API returns fewer than requested, assume no more data available
            if len(batch) < current_batch:
                self.logger.info("Received fewer records than requested; stopping pagination")
                break
            remaining -= len(batch)
        
        return aggregated

    def iter_vehicle_messages(self, total_amount: int, batch_size: int = 1000):
        """
        Generator that yields records in batches using simple pagination.
        
        Args:
            total_amount: Total number of messages to fetch
            batch_size: Number of messages per API request
        
        Yields:
            Lists of vehicle message dictionaries
        """
        if total_amount <= 0:
            return
        if batch_size <= 0:
            batch_size = 1
        
        remaining = total_amount
        while remaining > 0:
            current_batch = min(batch_size, remaining)
            self.logger.info(f"Pagination fetch (generator): requesting {current_batch} messages (remaining {remaining})")
            batch = self.fetch_vehicle_messages(amount=current_batch)
            if not batch:
                break
            yield batch
            if len(batch) < current_batch:
                break
            remaining -= len(batch)
            
    def health_check(self) -> bool:
        """
        Check if the API is healthy and responsive.
        
        Returns:
            True if API is healthy, False otherwise
        """
        try:
            # Try to fetch a small sample to test connectivity
            self.fetch_vehicle_messages(amount=1)
            return True
        except Exception as e:
            self.logger.warning(f"API health check failed: {e}")
            return False
