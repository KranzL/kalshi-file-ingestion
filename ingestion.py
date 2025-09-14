"""
Kalshi Atomic API Ingestion - Reliable Complete Data Processing
Strategy: Process each endpoint atomically from start to finish with comprehensive logging
"""

import json
import os
import requests
import time
import hashlib
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class KalshiAtomicIngestion:
    def __init__(self, verify_ssl=False):
        self.base_url = "https://api.elections.kalshi.com/trade-api/v2"
        self.verify_ssl = verify_ssl
        self.available_endpoints = {}
        self.output_folder = self.create_output_folder()
        self.retry_attempts = 5
        self.adaptive_delay = 0.1
        self.requests_this_minute = 0
        self.minute_start = time.time()
        self.max_timeout_retries = 10
        self.timeout_backoff_base = 60
        self.setup_logging()
        self.load_discovered_endpoints()
    
    def create_output_folder(self) -> str:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        folder_name = f"kalshi_atomic_ingestion_{timestamp}"
        
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)
            print(f"Created output folder: {folder_name}")
        
        return folder_name
        
    def setup_logging(self):
        log_filename = os.path.join(self.output_folder, 'atomic_ingestion.log')
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"ATOMIC INGESTION SESSION START - Output: {self.output_folder}")
        self.logger.info(f"Timeout handling: {self.max_timeout_retries} recovery attempts with up to {self.timeout_backoff_base * self.max_timeout_retries}s waits")
    
    def get_organized_path(self, endpoint: str, record_index: int = None) -> str:
        safe_endpoint = endpoint.replace('/', '_').replace('{', '').replace('}', '').strip('_')
        
        endpoint_folder = os.path.join(self.output_folder, safe_endpoint)
        if not os.path.exists(endpoint_folder):
            os.makedirs(endpoint_folder)
        
        if record_index is not None:
            batch_size = 1000
            batch_number = record_index // batch_size
            batch_folder = os.path.join(endpoint_folder, f"batch_{batch_number:04d}")
            if not os.path.exists(batch_folder):
                os.makedirs(batch_folder)
            return batch_folder
        
        return endpoint_folder
        
    def load_discovered_endpoints(self):
        discovery_files = [f for f in os.listdir('.') if f.startswith('kalshi_api_discovery_') and f.endswith('.json')]
        
        if not discovery_files:
            self.logger.warning("No API discovery file found. Using default endpoints.")
            self.available_endpoints = self.get_default_endpoints()
            return
        
        latest_file = max(discovery_files)
        
        try:
            with open(latest_file, 'r') as f:
                discovery_data = json.load(f)
            
            for endpoint_data in discovery_data.get('public_endpoints', []):
                self.available_endpoints[endpoint_data['endpoint']] = {
                    'description': endpoint_data['description'],
                    'capabilities': endpoint_data.get('capabilities', []),
                    'method': endpoint_data.get('method', 'GET')
                }
            
            self.logger.info(f"Loaded {len(self.available_endpoints)} endpoints from {latest_file}")
            
        except Exception as e:
            self.logger.error(f"Error loading discovery file: {e}")
            self.available_endpoints = self.get_default_endpoints()
    
    def get_default_endpoints(self):
        return {
            '/markets': {'description': 'Markets', 'capabilities': ['Supports pagination'], 'method': 'GET'},
            '/events': {'description': 'Events', 'capabilities': ['Supports pagination'], 'method': 'GET'},
            '/series': {'description': 'Series', 'capabilities': ['JSON response'], 'method': 'GET'}
        }
    
    def adaptive_rate_limit(self):
        current_time = time.time()
        
        if current_time - self.minute_start >= 60:
            self.requests_this_minute = 0
            self.minute_start = current_time
            
        if self.requests_this_minute >= 50:
            time.sleep(1)
        else:
            time.sleep(self.adaptive_delay)
            
        self.requests_this_minute += 1
    
    def _handle_timeout_recovery(self, url: str, query_params: Optional[Dict]) -> Optional[Dict]:
        self.logger.warning(f"TIMEOUT RECOVERY MODE: Entering extended retry sequence")
        
        for recovery_attempt in range(self.max_timeout_retries):
            wait_time = self.timeout_backoff_base * (recovery_attempt + 1)
            self.logger.info(f"Timeout recovery attempt {recovery_attempt + 1}/{self.max_timeout_retries}")
            self.logger.info(f"Waiting {wait_time}s for network recovery...")
            print(f"   Network timeout recovery - waiting {wait_time}s (attempt {recovery_attempt + 1}/{self.max_timeout_retries})")
            
            time.sleep(wait_time)
            
            try:
                response = requests.get(
                    url,
                    params=query_params or {},
                    verify=self.verify_ssl,
                    timeout=90
                )
                
                if response.status_code == 200:
                    self.logger.info(f"TIMEOUT RECOVERY SUCCESSFUL after {wait_time}s wait")
                    print(f"   Network recovered - continuing ingestion")
                    return response.json()
                    
            except requests.exceptions.Timeout:
                self.logger.warning(f"Recovery timeout on attempt {recovery_attempt + 1}")
                continue
            except Exception as e:
                self.logger.error(f"Recovery error: {e}")
                continue
                
        self.logger.error(f"TIMEOUT RECOVERY FAILED after {self.max_timeout_retries} attempts")
        total_wait = sum(self.timeout_backoff_base * (i + 1) for i in range(self.max_timeout_retries))
        self.logger.error(f"Total recovery time attempted: {total_wait}s ({total_wait/60:.1f} minutes)")
        return None
    
    def make_request(self, endpoint: str, query_params: Optional[Dict] = None) -> Optional[Dict]:
        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(self.retry_attempts):
            try:
                self.adaptive_rate_limit()
                
                response = requests.get(
                    url,
                    params=query_params or {},
                    verify=self.verify_ssl,
                    timeout=60
                )
                
                if response.status_code == 200:
                    self.adaptive_delay = max(0.05, self.adaptive_delay * 0.95)
                    return response.json()
                elif response.status_code == 429:
                    self.adaptive_delay = min(2.0, self.adaptive_delay * 2)
                    wait_time = min(2 ** attempt * 3, 30)
                    self.logger.warning(f"Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    self.logger.warning(f"API returned status {response.status_code}")
                    if attempt < self.retry_attempts - 1:
                        time.sleep(5)
                        continue
                    return None
                    
            except requests.exceptions.Timeout as e:
                self.logger.warning(f"Timeout on attempt {attempt + 1}: {e}")
                if attempt < self.retry_attempts - 1:
                    wait_time = min(10 * (attempt + 1), 60)
                    self.logger.info(f"Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    continue
                else:
                    self.logger.warning(f"All normal retries failed with timeouts - starting recovery mode")
                    return self._handle_timeout_recovery(url, query_params)
                    
            except Exception as e:
                self.logger.error(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt < self.retry_attempts - 1:
                    time.sleep(min(2 ** attempt, 10))
                    continue
                return None
                
        return None
    
    def process_endpoint_atomically(self, endpoint: str) -> Dict[str, Any]:
        start_time = time.time()
        self.logger.info(f"STARTING ATOMIC PROCESSING: {endpoint}")
        print(f"\\nProcessing {endpoint} atomically...")
        
        endpoint_info = self.available_endpoints.get(endpoint, {})
        supports_pagination = 'Supports pagination' in endpoint_info.get('capabilities', [])
        
        if endpoint == '/events':
            limit = 100
        elif endpoint == '/series':
            limit = 200
        else:
            limit = 1000
            
        all_records = []
        seen_hashes = set()
        total_duplicates = 0
        page = 0
        cursor = None
        
        print(f"   Strategy: {'Paginated' if supports_pagination else 'Single request'}")
        print(f"   Limit per page: {limit}")
        print(f"   Deduplication: Content hash based")
        
        if supports_pagination:
            while True:
                page += 1
                query_params = {'limit': limit}
                if cursor:
                    query_params['cursor'] = cursor
                
                self.logger.info(f"Fetching page {page} for {endpoint}")
                data = self.make_request(endpoint, query_params)
                
                if not data:
                    self.logger.error(f"Failed to fetch page {page}")
                    print(f"   NETWORK FAILURE at page {page} - endpoint may be incomplete")
                    print(f"   Retrieved {len(all_records):,} records before failure")
                    break
                
                if 'markets' in data:
                    page_items = data['markets']
                elif 'events' in data:
                    page_items = data['events']
                elif 'series' in data:
                    page_items = data['series']
                else:
                    self.logger.warning(f"Unexpected data structure in {endpoint}")
                    break
                
                if not page_items:
                    self.logger.info(f"Empty page {page} - ending pagination")
                    break
                
                page_duplicates = 0
                for item in page_items:
                    item_hash = hashlib.sha256(json.dumps(item, sort_keys=True).encode()).hexdigest()[:16]
                    if item_hash not in seen_hashes:
                        all_records.append(item)
                        seen_hashes.add(item_hash)
                    else:
                        page_duplicates += 1
                        total_duplicates += 1
                
                self.logger.info(f"Page {page}: {len(page_items)} items, {page_duplicates} duplicates, {len(all_records)} total unique")
                print(f"   Page {page}: {len(page_items)} items, {page_duplicates} duplicates -> {len(all_records):,} total unique")
                
                cursor = data.get('cursor')
                if not cursor:
                    self.logger.info(f"No more pages - pagination complete")
                    break
                    
                if page % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = len(all_records) / elapsed if elapsed > 0 else 0
                    print(f"   Progress: {page} pages, {len(all_records):,} unique records, {rate:.0f} rec/sec")
        else:
            self.logger.info(f"Single request for {endpoint}")
            data = self.make_request(endpoint)
            
            if data and 'series' in data:
                all_records = data['series']
                self.logger.info(f"Retrieved {len(all_records)} records")
            else:
                self.logger.error(f"Failed to retrieve data from {endpoint}")
        
        saved_count = 0
        if all_records:
            saved_count = self.save_endpoint_records(endpoint, all_records)
        
        end_time = time.time()
        duration = end_time - start_time
        
        result = {
            'endpoint': endpoint,
            'success': len(all_records) > 0,
            'records_processed': len(all_records),
            'files_saved': saved_count,
            'duplicates_found': total_duplicates,
            'pages_processed': page,
            'duration_seconds': duration,
            'records_per_second': len(all_records) / duration if duration > 0 else 0
        }
        
        self.logger.info(f"ATOMIC PROCESSING COMPLETE: {endpoint}")
        self.logger.info(f"Results: {len(all_records)} unique records, {saved_count} files saved, {duration:.1f}s")
        
        print(f"\\n{endpoint} COMPLETE:")
        print(f"   Unique records: {len(all_records):,}")
        print(f"   Files saved: {saved_count:,}")
        print(f"   Duplicates: {total_duplicates:,}")
        print(f"   Pages: {page}")
        print(f"   Duration: {duration:.1f}s")
        print(f"   Speed: {result['records_per_second']:.0f} records/sec")
        
        return result
    
    def save_endpoint_records(self, endpoint: str, records: List[Dict]) -> int:
        if not records:
            return 0
            
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_endpoint = endpoint.replace('/', '_').replace('{', '').replace('}', '').strip('_')
        saved_count = 0
        
        self.logger.info(f"SAVING {len(records)} records for {endpoint}")
        print(f"   Saving {len(records):,} records to files...")
        
        for i, record in enumerate(records):
            record_hash = hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()[:16]
            record_id = record.get('ticker') or record.get('id') or f"record_{i+1:04d}"
            filename = f"kalshi_{safe_endpoint}_{record_id}_{timestamp}.json"
            filename = filename.replace('/', '_').replace('__', '_')
            
            batch_folder = self.get_organized_path(endpoint, i)
            filepath = os.path.join(batch_folder, filename)
            
            output_data = {
                'endpoint': endpoint,
                'ingestion_time': timestamp,
                'record_id': record_id,
                'record_hash': record_hash,
                'batch_info': {
                    'record_index': i,
                    'batch_number': i // 1000,
                    'total_records': len(records)
                },
                'data': record
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            saved_count += 1
            
            if (i + 1) % 1000 == 0:
                self.logger.info(f"Saved batch {i // 1000}: records {i - 999}-{i}")
                print(f"   Batch {i // 1000} saved: records {i - 999}-{i}")
        
        self.logger.info(f"SAVE COMPLETE: {endpoint} - {saved_count} files")
        print(f"   All {saved_count:,} files saved")
        return saved_count
    
    def run_atomic_bulk_ingestion(self):
        session_start = time.time()
        self.logger.info(f"ATOMIC BULK INGESTION SESSION START")
        print(f"KALSHI ATOMIC BULK INGESTION")
        print(f"=" * 60)
        print(f"Strategy: Atomic endpoint processing")
        print(f"Endpoints: {len(self.available_endpoints)}")
        print(f"Output folder: {self.output_folder}")
        print(f"=" * 60)
        
        results = []
        successful_endpoints = []
        failed_endpoints = []
        total_records = 0
        total_files = 0
        
        for i, (endpoint, endpoint_info) in enumerate(self.available_endpoints.items(), 1):
            print(f"\\nENDPOINT {i}/{len(self.available_endpoints)}: {endpoint}")
            print(f"   Description: {endpoint_info['description']}")
            
            if '{' in endpoint:
                self.logger.info(f"Skipping parameterized endpoint: {endpoint}")
                print(f"   Skipping (requires parameters)")
                continue
            
            try:
                result = self.process_endpoint_atomically(endpoint)
                results.append(result)
                
                if result['success']:
                    successful_endpoints.append(endpoint)
                    total_records += result['records_processed']
                    total_files += result['files_saved']
                else:
                    failed_endpoints.append(endpoint)
                    
            except Exception as e:
                self.logger.error(f"Fatal error processing {endpoint}: {e}")
                print(f"   Fatal error: {e}")
                failed_endpoints.append(endpoint)
        
        session_duration = time.time() - session_start
        
        summary = {
            'session_start': datetime.fromtimestamp(session_start).isoformat(),
            'session_duration_seconds': session_duration,
            'total_endpoints_processed': len(results),
            'successful_endpoints': successful_endpoints,
            'failed_endpoints': failed_endpoints,
            'total_unique_records': total_records,
            'total_files_saved': total_files,
            'endpoint_results': results
        }
        
        summary_file = os.path.join(self.output_folder, f'atomic_session_summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\\nATOMIC INGESTION SESSION COMPLETE")
        print(f"=" * 60)
        print(f"Total records processed: {total_records:,}")
        print(f"Total files saved: {total_files:,}")
        print(f"Successful endpoints: {len(successful_endpoints)}")
        print(f"Failed endpoints: {len(failed_endpoints)}")
        print(f"Total session time: {session_duration:.1f} seconds")
        print(f"Average processing rate: {total_records/session_duration:.0f} records/second")
        print(f"Output folder: {self.output_folder}")
        print(f"Session summary: {summary_file}")
        
        self.logger.info(f"ATOMIC INGESTION SESSION COMPLETE")
        self.logger.info(f"Final stats: {total_records} records, {total_files} files, {session_duration:.1f}s")
        
        return summary

def main():
    ingestion = KalshiAtomicIngestion()
    ingestion.run_atomic_bulk_ingestion()

if __name__ == "__main__":
    main()