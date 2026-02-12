import requests
import time
from config import HEADERS

def get_api_data(url, max_retries=5):
    """Fetch JSON with built-in retries, 429 detection, and exponential backoff."""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=45)
            
            # Dynamic Rate Limit Detection (429 is standard, 430 is Blockchair's custom code)
            if response.status_code in [429, 430]:
                # 1. Respect "Retry-After" header if the server provides it
                retry_after = response.headers.get("Retry-After")
                wait_time = int(retry_after) if retry_after and retry_after.isdigit() else (2 ** attempt * 5)
                
                print(f"\n   ⚠️ Rate Limited ({response.status_code}). Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
                continue # Retry the loop
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            print(f"\n   ❌ HTTP Error [{url}]: {e}")
        except (requests.exceptions.JSONDecodeError, ValueError) as e:
            print(f"\n   ❌ JSON Error [{url}]: Failed to parse response. (Body: {response.text[:100] if 'response' in locals() else 'N/A'})")
        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            print(f"\n   ❌ Connection Error [{url}]: {e}")
            
        # Standard backoff for other errors
        if attempt < max_retries - 1:
            wait_time = (attempt + 1) * 3
            time.sleep(wait_time)
            
    return None

