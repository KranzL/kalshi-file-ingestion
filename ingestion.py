"""
Kalshi Market Data Fetcher
Efficiently downloads all market data from Kalshi's public API
"""

import json
import os
import time
from datetime import datetime
from collections import defaultdict
import concurrent.futures
from threading import Lock
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class KalshiMarketFetcher:
    def __init__(self, max_workers=10, batch_size=1000, verify_ssl=False):
        self.base_url = "https://api.elections.kalshi.com/trade-api/v2/markets"
        self.max_workers = max_workers
        self.batch_size = min(batch_size, 1000)
        self.verify_ssl = verify_ssl
        self.stats = {
            'total_markets': 0,
            'pages_fetched': 0,
            'start_time': None,
            'errors': []
        }
        self.stats_lock = Lock()
        self.output_dir = None
        
    def create_output_directory(self):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.output_dir = f"kalshi_markets_{timestamp}"
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "chunks"), exist_ok=True)
        return self.output_dir
    
    def fetch_page(self, page_num, cursor=None, status=None):
        params = {'limit': self.batch_size}
        if cursor:
            params['cursor'] = cursor
        if status:
            params['status'] = status
        
        try:
            response = requests.get(
                self.base_url, 
                params=params,
                verify=self.verify_ssl,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                with self.stats_lock:
                    self.stats['errors'].append(f"Page {page_num}: HTTP {response.status_code}")
                return None
                
        except Exception as e:
            with self.stats_lock:
                self.stats['errors'].append(f"Page {page_num}: {str(e)[:100]}")
            return None
    
    def fetch_sequential(self, status=None):
        print(f"\n📊 Fetching markets{' (status=' + status + ')' if status else ''}...")
        
        cursor = None
        page = 0
        
        while True:
            page += 1
            data = self.fetch_page(page, cursor, status)
            
            if not data:
                break
            
            markets = data.get('markets', [])
            if not markets:
                break
            
            with self.stats_lock:
                self.stats['total_markets'] += len(markets)
                self.stats['pages_fetched'] += 1
            
            chunk_file = os.path.join(self.output_dir, "chunks", f"chunk_{page:06d}.json")
            with open(chunk_file, 'w') as f:
                json.dump(markets, f)
            
            if page % 5 == 0:
                elapsed = time.time() - self.stats['start_time']
                rate = self.stats['total_markets'] / elapsed if elapsed > 0 else 0
                print(f"  Page {page}: {self.stats['total_markets']:,} markets "
                      f"({rate:.0f} markets/sec)")
            
            cursor = data.get('cursor')
            if not cursor:
                break
    
    def fetch_parallel(self, status=None):
        print(f"\n🔍 Phase 1: Discovering pages...")
        
        cursors = [None]
        cursor = None
        page = 0
        
        while True:
            page += 1
            data = self.fetch_page(page, cursor, status)
            
            if not data:
                break
                
            next_cursor = data.get('cursor')
            if next_cursor:
                cursors.append(next_cursor)
                cursor = next_cursor
            else:
                break
            
            if page % 10 == 0:
                print(f"  Found {page} pages...")
        
        total_pages = len(cursors)
        print(f"  ✅ Found {total_pages} pages to fetch")
        
        print(f"\n⚡ Phase 2: Parallel fetch with {self.max_workers} workers...")
        
        def fetch_and_save(args):
            page_num, cursor = args
            data = self.fetch_page(page_num, cursor, status)
            
            if data:
                markets = data.get('markets', [])
                
                with self.stats_lock:
                    self.stats['total_markets'] += len(markets)
                    self.stats['pages_fetched'] += 1
                    
                    if self.stats['pages_fetched'] % 10 == 0:
                        elapsed = time.time() - self.stats['start_time']
                        rate = self.stats['total_markets'] / elapsed if elapsed > 0 else 0
                        percent = (self.stats['pages_fetched'] / total_pages) * 100
                        print(f"  Progress: {percent:.1f}% - {self.stats['total_markets']:,} markets "
                              f"({rate:.0f} markets/sec)")
                
                chunk_file = os.path.join(self.output_dir, "chunks", f"chunk_{page_num:06d}.json")
                with open(chunk_file, 'w') as f:
                    json.dump(markets, f)
        
        work_items = list(enumerate(cursors, 1))
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            list(executor.map(fetch_and_save, work_items))
    
    def run(self, method='parallel', status_filter=None):
        self.stats['start_time'] = time.time()
        self.create_output_directory()
        
        print("=" * 60)
        print("KALSHI MARKET DATA FETCHER")
        print("=" * 60)
        print(f"Method: {method}")
        print(f"Workers: {self.max_workers if method == 'parallel' else 1}")
        print(f"Output directory: {self.output_dir}")
        print("-" * 60)
        
        if method == 'parallel':
            self.fetch_parallel(status_filter)
        else:
            self.fetch_sequential(status_filter)
        
        elapsed = time.time() - self.stats['start_time']
        
        print("\n" + "=" * 60)
        print("DOWNLOAD COMPLETE")
        print("=" * 60)
        print(f"Total markets: {self.stats['total_markets']:,}")
        print(f"Pages fetched: {self.stats['pages_fetched']:,}")
        print(f"Time elapsed: {elapsed:.1f} seconds")
        
        if self.stats['total_markets'] > 0:
            print(f"Rate: {self.stats['total_markets']/elapsed:.0f} markets/second")
        
        if self.stats['errors']:
            print(f"\n⚠️ Errors encountered: {len(self.stats['errors'])}")
            for err in self.stats['errors'][:5]:
                print(f"  - {err}")
        
        print(f"\n📁 Data saved in: {self.output_dir}/")
        
        return self.stats
    
    def merge_chunks(self, group_by='status'):
        if not self.output_dir:
            print("No output directory set")
            return
        
        chunks_dir = os.path.join(self.output_dir, "chunks")
        if not os.path.exists(chunks_dir):
            print("No chunks directory found")
            return
        
        print("\n📦 Merging chunks...")
        
        chunk_files = sorted([f for f in os.listdir(chunks_dir) if f.endswith('.json')])
        total_files = len(chunk_files)
        print(f"  Found {total_files} chunk files")
        
        if group_by:
            grouped = defaultdict(list)
            
            for i, filename in enumerate(chunk_files):
                if i % 50 == 0:
                    print(f"  Processing chunk {i}/{total_files}...")
                
                filepath = os.path.join(chunks_dir, filename)
                try:
                    with open(filepath, 'r') as f:
                        markets = json.load(f)
                        
                        for market in markets:
                            if group_by == 'status':
                                key = market.get('status', 'unknown')
                            elif group_by == 'series':
                                ticker = market.get('ticker', '')
                                key = ticker.split('-')[0] if ticker and '-' in ticker else 'unknown'
                            elif group_by == 'event':
                                key = market.get('event_ticker', 'unknown')
                            else:
                                key = 'all'
                            
                            grouped[key].append(market)
                except Exception as e:
                    print(f"  ⚠️ Error reading {filename}: {e}")
            
            group_dir = os.path.join(self.output_dir, f"by_{group_by}")
            os.makedirs(group_dir, exist_ok=True)
            
            print(f"\n💾 Saving grouped files...")
            for key, markets in grouped.items():
                safe_key = "".join(c if c.isalnum() or c in '-_' else '_' for c in key)
                filepath = os.path.join(group_dir, f"{safe_key}.json")
                
                with open(filepath, 'w') as f:
                    json.dump({
                        'group': key,
                        'count': len(markets),
                        'markets': markets
                    }, f)
                
                print(f"  ✅ {safe_key}: {len(markets):,} markets")
            
            print(f"\n📁 Grouped files saved in: {group_dir}/")
        
        else:
            all_markets = []
            
            for i, filename in enumerate(chunk_files):
                if i % 50 == 0:
                    print(f"  Reading chunk {i}/{total_files}...")
                
                filepath = os.path.join(chunks_dir, filename)
                try:
                    with open(filepath, 'r') as f:
                        markets = json.load(f)
                        all_markets.extend(markets)
                except Exception as e:
                    print(f"  ⚠️ Error reading {filename}: {e}")
            
            output_file = os.path.join(self.output_dir, "all_markets.json")
            with open(output_file, 'w') as f:
                json.dump(all_markets, f)
            
            print(f"  💾 Saved {len(all_markets):,} markets to {output_file}")

def test_connection():
    print("Testing connection to Kalshi API...")
    try:
        test_url = "https://api.elections.kalshi.com/trade-api/v2/markets"
        response = requests.get(test_url, params={'limit': 1}, verify=False, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Connection successful!")
            if data.get('cursor'):
                print("📊 Multiple pages detected - large dataset available")
            return True
        else:
            print(f"⚠️ API returned status {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def main():
    print("\n⚡ KALSHI MARKET DATA FETCHER")
    print("=" * 60)
    
    if not test_connection():
        return
    
    print("\nDownload Options:")
    print("  1. Fast parallel (recommended)")
    print("  2. Sequential (slower but stable)")
    
    method_choice = input("\nSelect method (1-2, default=1): ").strip() or '1'
    
    print("\nMarket Status Filter:")
    print("  a. All markets (500K+)")
    print("  b. Open markets only (~3K)")
    print("  c. Closed markets")
    print("  d. Settled markets")
    
    status_choice = input("Select filter (a-d, default=b): ").strip().lower() or 'b'
    
    method = 'parallel' if method_choice == '1' else 'sequential'
    status_map = {'a': None, 'b': 'open', 'c': 'closed', 'd': 'settled'}
    status = status_map.get(status_choice, 'open')
    
    workers = 10
    if method == 'parallel':
        custom = input("\nNumber of parallel workers (1-30, default=10): ").strip()
        if custom.isdigit():
            workers = max(1, min(int(custom), 30))
    
    print("\nStarting download...")
    fetcher = KalshiMarketFetcher(max_workers=workers, verify_ssl=False)
    stats = fetcher.run(method=method, status_filter=status)
    
    if stats['total_markets'] > 0:
        merge = input("\nOrganize data into grouped files? (y/n): ").strip().lower()
        if merge == 'y':
            print("\nGroup by:")
            print("  1. Status")
            print("  2. Series")
            print("  3. Event")
            print("  4. Single file (no grouping)")
            
            group_choice = input("Select (1-4, default=1): ").strip() or '1'
            group_map = {'1': 'status', '2': 'series', '3': 'event', '4': None}
            
            fetcher.merge_chunks(group_by=group_map.get(group_choice, 'status'))
    
    print("\n✅ Complete!")
    print(f"📁 Data location: {fetcher.output_dir}/")

if __name__ == "__main__":
    main()
