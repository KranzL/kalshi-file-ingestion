#!/usr/bin/env python3
"""
Kalshi Fast Ingestion Script - Simplified version with SSL fixes
Works on macOS without certificate issues
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

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class KalshiSimpleIngester:
    """Fast parallel ingestion for Kalshi markets using threading."""
    
    def __init__(self, max_workers=10, batch_size=1000, verify_ssl=False):
        """
        Initialize the ingester.
        
        Args:
            max_workers: Number of parallel workers
            batch_size: Markets per API request (max 1000)
            verify_ssl: Whether to verify SSL certificates
        """
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
        self.all_cursors = []
        
    def create_output_directory(self):
        """Create timestamped output directory."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.output_dir = f"kalshi_markets_{timestamp}"
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "chunks"), exist_ok=True)
        return self.output_dir
    
    def fetch_page(self, page_num, cursor=None, status=None):
        """Fetch a single page of markets."""
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
    
    def fetch_sequential_with_progress(self, status=None, save_chunks=True):
        """Fetch all pages sequentially with progress display."""
        print(f"\n📊 Fetching markets{' (status=' + status + ')' if status else ''}...")
        
        cursor = None
        page = 0
        all_markets = []
        
        while True:
            page += 1
            
            # Fetch page
            data = self.fetch_page(page, cursor, status)
            
            if not data:
                break
            
            markets = data.get('markets', [])
            if not markets:
                break
            
            # Update stats
            with self.stats_lock:
                self.stats['total_markets'] += len(markets)
                self.stats['pages_fetched'] += 1
            
            # Save chunk immediately to avoid memory issues
            if save_chunks and self.output_dir:
                chunk_file = os.path.join(self.output_dir, "chunks", f"chunk_{page:06d}.json")
                with open(chunk_file, 'w') as f:
                    json.dump(markets, f)
            else:
                all_markets.extend(markets)
            
            # Progress update
            if page % 5 == 0:
                elapsed = time.time() - self.stats['start_time']
                rate = self.stats['total_markets'] / elapsed if elapsed > 0 else 0
                print(f"  Page {page}: {self.stats['total_markets']:,} markets "
                      f"({rate:.0f} markets/sec)")
            
            # Get next cursor
            cursor = data.get('cursor')
            if not cursor:
                break
        
        return all_markets if not save_chunks else None
    
    def fetch_parallel_smart(self, status=None, save_chunks=True):
        """
        Smart parallel fetching - first get all cursors, then fetch in parallel.
        """
        print(f"\n🔍 Phase 1: Discovering pages...")
        
        # First pass: collect all cursors
        cursors = [None]  # Start with None for first page
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
        
        # Second pass: fetch all pages in parallel
        print(f"\n⚡ Phase 2: Parallel fetch with {self.max_workers} workers...")
        
        def fetch_and_save(args):
            page_num, cursor = args
            data = self.fetch_page(page_num, cursor, status)
            
            if data:
                markets = data.get('markets', [])
                
                # Update stats
                with self.stats_lock:
                    self.stats['total_markets'] += len(markets)
                    self.stats['pages_fetched'] += 1
                    
                    # Progress update
                    if self.stats['pages_fetched'] % 10 == 0:
                        elapsed = time.time() - self.stats['start_time']
                        rate = self.stats['total_markets'] / elapsed if elapsed > 0 else 0
                        percent = (self.stats['pages_fetched'] / total_pages) * 100
                        print(f"  Progress: {percent:.1f}% - {self.stats['total_markets']:,} markets "
                              f"({rate:.0f} markets/sec)")
                
                # Save chunk
                if save_chunks and self.output_dir:
                    chunk_file = os.path.join(self.output_dir, "chunks", f"chunk_{page_num:06d}.json")
                    with open(chunk_file, 'w') as f:
                        json.dump(markets, f)
                    return None
                else:
                    return markets
            
            return None
        
        # Create work items (page number, cursor)
        work_items = list(enumerate(cursors, 1))
        
        # Execute in parallel
        all_markets = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            results = executor.map(fetch_and_save, work_items)
            
            if not save_chunks:
                for result in results:
                    if result:
                        all_markets.extend(result)
        
        return all_markets if not save_chunks else None
    
    def run(self, method='parallel', status_filter=None):
        """
        Main entry point for ingestion.
        
        Args:
            method: 'parallel' or 'sequential'
            status_filter: Filter by market status
        """
        self.stats['start_time'] = time.time()
        self.create_output_directory()
        
        print("=" * 60)
        print("KALSHI FAST INGESTION")
        print("=" * 60)
        print(f"Method: {method}")
        print(f"Workers: {self.max_workers if method == 'parallel' else 1}")
        print(f"Batch size: {self.batch_size}")
        print(f"SSL verification: {self.verify_ssl}")
        print(f"Output directory: {self.output_dir}")
        print("-" * 60)
        
        if method == 'parallel':
            markets = self.fetch_parallel_smart(status_filter, save_chunks=True)
        else:
            markets = self.fetch_sequential_with_progress(status_filter, save_chunks=True)
        
        # Show results
        elapsed = time.time() - self.stats['start_time']
        
        print("\n" + "=" * 60)
        print("INGESTION COMPLETE")
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
        
        print(f"\n📁 Data saved in: {self.output_dir}/chunks/")
        
        return self.stats
    
    def merge_chunks(self, group_by='status'):
        """Merge chunk files into organized structure."""
        if not self.output_dir:
            print("No output directory set")
            return
        
        chunks_dir = os.path.join(self.output_dir, "chunks")
        if not os.path.exists(chunks_dir):
            print("No chunks directory found")
            return
        
        print("\n📦 Merging chunks...")
        
        # Count chunk files
        chunk_files = sorted([f for f in os.listdir(chunks_dir) if f.endswith('.json')])
        total_files = len(chunk_files)
        print(f"  Found {total_files} chunk files")
        
        if group_by:
            grouped = defaultdict(list)
            
            # Read and group markets
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
            
            # Save grouped files
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
            # Save as single file
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

def main():
    """Main entry point."""
    print("\n⚡ KALSHI FAST INGESTION (SSL SAFE VERSION)")
    print("=" * 60)
    
    # Test connection
    print("Testing connection to Kalshi API...")
    try:
        test_url = "https://api.elections.kalshi.com/trade-api/v2/markets"
        response = requests.get(test_url, params={'limit': 1}, verify=False, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            market_count = len(data.get('markets', []))
            print(f"✅ Connection successful! API is responding.")
            if data.get('cursor'):
                print("📊 Multiple pages detected - there are MANY markets to fetch")
        else:
            print(f"⚠️ API returned status {response.status_code}")
            
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return
    
    print("\nIngestion Options:")
    print("  1. Fast parallel (recommended)")
    print("  2. Sequential (slower but more stable)")
    
    method_choice = input("\nSelect method (1-2, default=1): ").strip() or '1'
    
    print("\nStatus Filters:")
    print("  a. All markets (500K+, will take time)")
    print("  b. Open markets only")
    print("  c. Closed markets only")
    print("  d. Settled markets only")
    
    status_choice = input("Select filter (a-d, default=b for testing): ").strip().lower() or 'b'
    
    # Map choices
    method = 'parallel' if method_choice == '1' else 'sequential'
    status_map = {'a': None, 'b': 'open', 'c': 'closed', 'd': 'settled'}
    status = status_map.get(status_choice, 'open')
    
    # Worker count for parallel
    workers = 10
    if method == 'parallel':
        custom = input("\nNumber of parallel workers (default=10, max=30): ").strip()
        if custom.isdigit():
            workers = min(int(custom), 30)
    
    # Create and run ingester
    print("\nStarting ingestion...")
    ingester = KalshiSimpleIngester(max_workers=workers, verify_ssl=False)
    stats = ingester.run(method=method, status_filter=status)
    
    # Offer to merge if successful
    if stats['total_markets'] > 0:
        merge = input("\nMerge chunks into grouped files? (y/n): ").strip().lower()
        if merge == 'y':
            print("\nGroup by:")
            print("  1. Status (open/closed/settled)")
            print("  2. Series (INX, HIGHNYC, etc)")
            print("  3. Event ticker")
            print("  4. No grouping (single file)")
            
            group_choice = input("Select (1-4, default=1): ").strip() or '1'
            group_map = {'1': 'status', '2': 'series', '3': 'event', '4': None}
            
            ingester.merge_chunks(group_by=group_map.get(group_choice, 'status'))
    
    print("\n✅ Complete!")
    print(f"📁 All data saved in: {ingester.output_dir}/")

if __name__ == "__main__":
    main()