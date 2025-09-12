"""
Kalshi Flexible API Ingestion
Allows users to select and ingest data from any discovered Kalshi API endpoint
"""

import json
import os
import requests
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class KalshiFlexibleIngestion:
    def __init__(self, verify_ssl=False):
        self.base_url = "https://api.elections.kalshi.com/trade-api/v2"
        self.verify_ssl = verify_ssl
        self.available_endpoints = {}
        self.output_folder = self.create_output_folder()
        self.load_discovered_endpoints()
    
    def create_output_folder(self) -> str:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        folder_name = f"kalshi_ingestion_{timestamp}"
        
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)
            print(f"📁 Created output folder: {folder_name}")
        
        return folder_name
    
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
            print("⚠️ No API discovery file found. Run api_discovery.py first.")
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
            
            print(f"✅ Loaded {len(self.available_endpoints)} endpoints from {latest_file}")
            
        except Exception as e:
            print(f"⚠️ Error loading discovery file: {e}")
            self.available_endpoints = self.get_default_endpoints()
    
    def get_default_endpoints(self):
        return {
            '/markets': {
                'description': 'Retrieve all available markets with pagination support',
                'capabilities': ['Supports pagination', 'JSON response'],
                'method': 'GET'
            },
            '/events': {
                'description': 'Retrieve all events',
                'capabilities': ['Supports pagination', 'JSON response'], 
                'method': 'GET'
            },
            '/series': {
                'description': 'Retrieve all market series',
                'capabilities': ['JSON response'],
                'method': 'GET'
            },
            '/markets/{ticker}': {
                'description': 'Get detailed information for a specific market',
                'capabilities': ['JSON response'],
                'method': 'GET'
            },
            '/markets/{ticker}/orderbook': {
                'description': 'Get order book data for a specific market',
                'capabilities': ['JSON response'],
                'method': 'GET'
            },
            '/events/{event_ticker}': {
                'description': 'Get specific event details and markets',
                'capabilities': ['JSON response'],
                'method': 'GET'
            },
            '/series/{series_ticker}': {
                'description': 'Get specific series details',
                'capabilities': ['JSON response'],
                'method': 'GET'
            }
        }
    
    def display_available_endpoints(self):
        print("\n📋 AVAILABLE ENDPOINTS")
        print("=" * 50)
        
        for i, (endpoint, info) in enumerate(self.available_endpoints.items(), 1):
            print(f"{i:2d}. {endpoint}")
            print(f"    {info['description']}")
            if 'Supports pagination' in info.get('capabilities', []):
                print(f"    💡 Supports pagination for large datasets")
            print()
    
    def get_sample_identifiers(self, param_type: str) -> List[str]:
        if param_type == 'ticker':
            result = self.make_request('/markets', {'limit': 5})
            if result and 'markets' in result:
                return [market['ticker'] for market in result['markets'] if market.get('ticker')]
        
        elif param_type == 'event_ticker':
            result = self.make_request('/events', {'limit': 5})
            if result and 'events' in result:
                return [event['ticker'] for event in result['events'] if event.get('ticker')]
        
        elif param_type == 'series_ticker':
            result = self.make_request('/series', {'limit': 5})
            if result and 'series' in result:
                return [series['ticker'] for series in result['series'] if series.get('ticker')]
        
        return []
    
    def get_user_parameters(self, endpoint: str) -> Dict[str, str]:
        params = {}
        
        if '{ticker}' in endpoint:
            print("\n🔍 Need a market ticker...")
            tickers = self.get_sample_identifiers('ticker')
            if tickers:
                print("Available sample tickers:")
                for i, ticker in enumerate(tickers[:5], 1):
                    print(f"  {i}. {ticker}")
                print("  c. Custom ticker")
                
                try:
                    choice = input("Select ticker (1-5 or 'c' for custom): ").strip().lower()
                    
                    if choice == 'c':
                        params['ticker'] = input("Enter ticker: ").strip()
                    elif choice.isdigit() and 1 <= int(choice) <= len(tickers):
                        params['ticker'] = tickers[int(choice) - 1]
                    else:
                        params['ticker'] = tickers[0] if tickers else input("Enter ticker: ").strip()
                except EOFError:
                    params['ticker'] = tickers[0] if tickers else "UNDEFINED"
            else:
                try:
                    params['ticker'] = input("Enter market ticker: ").strip()
                except EOFError:
                    params['ticker'] = "UNDEFINED"
        
        if '{event_ticker}' in endpoint:
            print("\n🎯 Need an event ticker...")
            event_tickers = self.get_sample_identifiers('event_ticker')
            if event_tickers:
                print("Available sample event tickers:")
                for i, ticker in enumerate(event_tickers[:5], 1):
                    print(f"  {i}. {ticker}")
                print("  c. Custom event ticker")
                
                choice = input("Select event ticker (1-5 or 'c' for custom): ").strip().lower()
                
                if choice == 'c':
                    params['event_ticker'] = input("Enter event ticker: ").strip()
                elif choice.isdigit() and 1 <= int(choice) <= len(event_tickers):
                    params['event_ticker'] = event_tickers[int(choice) - 1]
                else:
                    params['event_ticker'] = event_tickers[0] if event_tickers else input("Enter event ticker: ").strip()
            else:
                params['event_ticker'] = input("Enter event ticker: ").strip()
        
        if '{series_ticker}' in endpoint:
            print("\n📊 Need a series ticker...")
            series_tickers = self.get_sample_identifiers('series_ticker')
            if series_tickers:
                print("Available sample series tickers:")
                for i, ticker in enumerate(series_tickers[:5], 1):
                    print(f"  {i}. {ticker}")
                print("  c. Custom series ticker")
                
                choice = input("Select series ticker (1-5 or 'c' for custom): ").strip().lower()
                
                if choice == 'c':
                    params['series_ticker'] = input("Enter series ticker: ").strip()
                elif choice.isdigit() and 1 <= int(choice) <= len(series_tickers):
                    params['series_ticker'] = series_tickers[int(choice) - 1]
                else:
                    params['series_ticker'] = series_tickers[0] if series_tickers else input("Enter series ticker: ").strip()
            else:
                params['series_ticker'] = input("Enter series ticker: ").strip()
        
        return params
    
    def build_endpoint_url(self, endpoint: str, params: Dict[str, str]) -> str:
        url = endpoint
        for param_name, param_value in params.items():
            placeholder = f"{{{param_name}}}"
            if placeholder in url:
                url = url.replace(placeholder, param_value)
        return url
    
    def make_request(self, endpoint: str, query_params: Optional[Dict] = None) -> Optional[Dict]:
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = requests.get(
                url,
                params=query_params or {},
                verify=self.verify_ssl,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"⚠️ API returned status {response.status_code}")
                if response.status_code == 404:
                    print("   Endpoint not found or parameters invalid")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON response: {e}")
            return None
    
    def ingest_with_pagination(self, endpoint: str, max_pages: int = None) -> List[Dict]:
        all_data = []
        cursor = None
        page = 0
        
        print(f"\n📥 Starting paginated ingestion...")
        print(f"Endpoint: {endpoint}")
        if max_pages:
            print(f"Max pages: {max_pages}")
        else:
            print("Max pages: unlimited (will fetch all available data)")
        
        while max_pages is None or page < max_pages:
            page += 1
            query_params = {}
            
            if cursor:
                query_params['cursor'] = cursor
            
            print(f"  Fetching page {page}...")
            data = self.make_request(endpoint, query_params)
            
            if not data:
                print(f"  ❌ Failed to fetch page {page}")
                break
            
            page_items = []
            
            if 'markets' in data:
                page_items = data['markets']
                all_data.extend(page_items)
            elif 'events' in data:
                page_items = data['events']
                all_data.extend(page_items)
            elif 'series' in data:
                page_items = data['series']
                all_data.extend(page_items)
            else:
                all_data.append(data)
                break
            
            print(f"  ✅ Page {page}: {len(page_items)} items (total: {len(all_data)})")
            
            cursor = data.get('cursor')
            if not cursor:
                print(f"  ℹ️ No more pages available")
                break
            
            time.sleep(0.1)
        
        return all_data
    
    def save_individual_records(self, data: Any, endpoint: str, params: Dict[str, str] = None) -> List[str]:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_endpoint = endpoint.replace('/', '_').replace('{', '').replace('}', '')
        saved_files = []
        
        if isinstance(data, list):
            print(f"    📁 Organizing {len(data)} records into batches of 1000...")
            for i, record in enumerate(data):
                record_id = record.get('ticker') or record.get('id') or f"record_{i+1:04d}"
                filename = f"kalshi_{safe_endpoint}_{record_id}_{timestamp}.json"
                filename = filename.replace('/', '_').replace('__', '_')
                
                batch_folder = self.get_organized_path(endpoint, i)
                filepath = os.path.join(batch_folder, filename)
                
                output_data = {
                    'endpoint': endpoint,
                    'parameters': params or {},
                    'ingestion_time': timestamp,
                    'record_id': record_id,
                    'batch_info': {
                        'record_index': i,
                        'batch_number': i // 1000,
                        'records_in_batch': min(1000, len(data) - (i // 1000) * 1000)
                    },
                    'data': record
                }
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(output_data, f, indent=2, ensure_ascii=False)
                
                saved_files.append(filepath)
                
                if (i + 1) % 1000 == 0:
                    print(f"    ✅ Saved batch {i // 1000}: records {i - 999}-{i}")
        else:
            record_id = data.get('ticker') or data.get('id') or 'single_record'
            filename = f"kalshi_{safe_endpoint}_{record_id}_{timestamp}.json"
            filename = filename.replace('/', '_').replace('__', '_')
            
            endpoint_folder = self.get_organized_path(endpoint)
            filepath = os.path.join(endpoint_folder, filename)
            
            output_data = {
                'endpoint': endpoint,
                'parameters': params or {},
                'ingestion_time': timestamp,
                'record_id': record_id,
                'data': data
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
                
            saved_files.append(filepath)
        
        return saved_files
    
    def save_bulk_summary(self, total_records: int, endpoints_processed: List[str], failed_endpoints: List[str]) -> str:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"kalshi_bulk_ingestion_summary_{timestamp}.json"
        filepath = os.path.join(self.output_folder, filename)
        
        summary_data = {
            'bulk_ingestion_time': timestamp,
            'total_records_saved': total_records,
            'endpoints_processed': len(endpoints_processed),
            'successful_endpoints': endpoints_processed,
            'failed_endpoints': failed_endpoints,
            'success_rate': f"{len(endpoints_processed)/(len(endpoints_processed)+len(failed_endpoints))*100:.1f}%" if (endpoints_processed or failed_endpoints) else "0%"
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(summary_data, f, indent=2, ensure_ascii=False)
        
        return filepath
    
    def run_bulk_ingestion(self):
        print("🚀 KALSHI BULK API INGESTION")
        print("=" * 50)
        print("Ingesting ALL available data from ALL endpoints...")
        print()
        
        if not self.available_endpoints:
            print("❌ No endpoints available")
            return
        
        total_records = 0
        successful_endpoints = []
        failed_endpoints = []
        
        for i, (endpoint, endpoint_info) in enumerate(self.available_endpoints.items(), 1):
            print(f"\n📊 Processing endpoint {i}/{len(self.available_endpoints)}: {endpoint}")
            print(f"    {endpoint_info['description']}")
            
            try:
                if '{' in endpoint:
                    print(f"    ⚠️ Skipping parameterized endpoint (requires specific identifiers)")
                    continue
                
                supports_pagination = 'Supports pagination' in endpoint_info.get('capabilities', [])
                
                if supports_pagination:
                    print(f"    📄 Using pagination (unlimited - will fetch all data)")
                    data = self.ingest_with_pagination(endpoint)
                else:
                    print(f"    📥 Fetching single page")
                    data = self.make_request(endpoint)
                
                if data:
                    if isinstance(data, list):
                        record_count = len(data)
                    elif isinstance(data, dict):
                        if 'markets' in data:
                            data = data['markets']
                            record_count = len(data)
                        elif 'events' in data:
                            data = data['events']
                            record_count = len(data)
                        elif 'series' in data:
                            data = data['series']
                            record_count = len(data)
                        else:
                            record_count = 1
                    else:
                        record_count = 1
                    
                    saved_files = self.save_individual_records(data, endpoint)
                    total_records += record_count
                    successful_endpoints.append(endpoint)
                    
                    print(f"    ✅ Success: {record_count:,} records → {len(saved_files)} files")
                else:
                    failed_endpoints.append(endpoint)
                    print(f"    ❌ Failed to retrieve data")
                
                time.sleep(0.5)
                
            except Exception as e:
                failed_endpoints.append(endpoint)
                print(f"    ❌ Error: {e}")
        
        summary_file = self.save_bulk_summary(total_records, successful_endpoints, failed_endpoints)
        
        print(f"\n🎉 BULK INGESTION COMPLETE")
        print("=" * 50)
        print(f"Total records saved: {total_records:,}")
        print(f"Successful endpoints: {len(successful_endpoints)}")
        print(f"Failed endpoints: {len(failed_endpoints)}")
        print(f"Summary saved to: {summary_file}")
    
    def run_interactive_ingestion(self):
        print("🚀 KALSHI INTERACTIVE API INGESTION")
        print("=" * 50)
        
        if not self.available_endpoints:
            print("❌ No endpoints available")
            return
        
        self.display_available_endpoints()
        
        while True:
            try:
                choice = input("Select endpoint (1-{}) or 'q' to quit: ".format(len(self.available_endpoints))).strip().lower()
                
                if choice == 'q':
                    print("👋 Goodbye!")
                    return
                
                if not choice.isdigit():
                    print("⚠️ Please enter a number or 'q'")
                    continue
                
                endpoint_index = int(choice) - 1
                if not (0 <= endpoint_index < len(self.available_endpoints)):
                    print("⚠️ Invalid selection")
                    continue
                
                selected_endpoint = list(self.available_endpoints.keys())[endpoint_index]
                endpoint_info = self.available_endpoints[selected_endpoint]
                
                print(f"\n✅ Selected: {selected_endpoint}")
                print(f"Description: {endpoint_info['description']}")
                
                params = self.get_user_parameters(selected_endpoint)
                actual_endpoint = self.build_endpoint_url(selected_endpoint, params)
                
                supports_pagination = 'Supports pagination' in endpoint_info.get('capabilities', [])
                
                if supports_pagination:
                    print(f"\n📄 This endpoint supports pagination")
                    paginate = input("Use pagination? (y/n, default=y): ").strip().lower()
                    
                    if paginate != 'n':
                        max_pages_input = input("Maximum pages to fetch (default=10): ").strip()
                        max_pages = int(max_pages_input) if max_pages_input.isdigit() else 10
                        
                        data = self.ingest_with_pagination(actual_endpoint, max_pages)
                    else:
                        print(f"\n📥 Fetching single page...")
                        data = self.make_request(actual_endpoint)
                else:
                    print(f"\n📥 Fetching data...")
                    data = self.make_request(actual_endpoint)
                
                if data:
                    saved_files = self.save_individual_records(data, selected_endpoint, params)
                    
                    count = len(data) if isinstance(data, list) else 1
                    print(f"\n✅ Success!")
                    print(f"Data points collected: {count:,}")
                    print(f"Files saved: {len(saved_files)}")
                    
                    continue_choice = input("\nIngest another endpoint? (y/n): ").strip().lower()
                    if continue_choice != 'y':
                        break
                else:
                    print("❌ Failed to retrieve data")
                    
            except EOFError:
                print("\n\n👋 EOF encountered, exiting")
                break
            except KeyboardInterrupt:
                print("\n\n👋 Interrupted by user")
                break
            except Exception as e:
                print(f"❌ Error: {e}")
                break

def display_ingestion_menu():
    print("🚀 KALSHI API INGESTION")
    print("=" * 50)
    print("Choose your ingestion type:")
    print()
    print("1. 📊 Bulk Ingestion")
    print("   - Automatically ingests ALL available data from ALL endpoints")
    print("   - No user interaction required")
    print("   - Comprehensive data collection")
    print()
    print("2. 🎯 Interactive Ingestion") 
    print("   - Select specific endpoints to ingest")
    print("   - Control pagination and parameters")
    print("   - Targeted data collection")
    print()

def get_ingestion_choice():
    while True:
        try:
            choice = input("Select ingestion type (1-2) or 'q' to quit: ").strip().lower()
            
            if choice == 'q':
                print("👋 Goodbye!")
                return None
            elif choice == '1':
                return 'bulk'
            elif choice == '2':
                return 'interactive'
            else:
                print("⚠️ Please enter 1, 2, or 'q'")
                continue
                
        except (EOFError, KeyboardInterrupt):
            print("\n👋 Goodbye!")
            return None

def main():
    import sys
    
    ingestion = KalshiFlexibleIngestion()
    
    if len(sys.argv) > 1:
        if sys.argv[1] == '--interactive':
            ingestion.run_interactive_ingestion()
        elif sys.argv[1] == '--bulk':
            ingestion.run_bulk_ingestion()
        else:
            print("⚠️ Unknown argument. Use --interactive or --bulk, or run without arguments for menu.")
            return
    else:
        display_ingestion_menu()
        choice = get_ingestion_choice()
        
        if choice == 'bulk':
            ingestion.run_bulk_ingestion()
        elif choice == 'interactive':
            ingestion.run_interactive_ingestion()

if __name__ == "__main__":
    main()