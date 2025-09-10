import requests
import time
from datetime import datetime
from collections import defaultdict

def count_kalshi_markets(detailed_breakdown=True):
    """
    Quickly count all Kalshi markets without downloading full data.
    
    Args:
        detailed_breakdown: If True, shows breakdown by status and estimates
    
    Returns:
        Dictionary with count statistics
    """
    url = "https://api.elections.kalshi.com/trade-api/v2/markets"
    
    print("=" * 60)
    print("KALSHI MARKET COUNTER")
    print("=" * 60)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 60)
    
    # First, get counts for different statuses
    statuses = ['open', 'closed', 'settled', 'unopened']
    status_counts = {}
    
    if detailed_breakdown:
        print("\n📊 Counting markets by status...")
        for status in statuses:
            count = count_by_status(url, status)
            status_counts[status] = count
            print(f"  • {status:10}: {count:,} markets")
            time.sleep(0.1)  # Be nice to the API
    
    # Count total markets (all statuses)
    print("\n📊 Counting total markets...")
    total_count = count_total_markets(url)
    
    # Get sample to estimate data size
    print("\n📊 Analyzing sample for size estimates...")
    sample_stats = get_sample_statistics(url)
    
    # Calculate estimates
    stats = {
        'total_markets': total_count,
        'status_breakdown': status_counts,
        'sample_stats': sample_stats,
        'estimates': calculate_estimates(total_count, sample_stats)
    }
    
    # Display results
    display_results(stats)
    
    return stats

def count_by_status(base_url, status):
    """Count markets with a specific status."""
    cursor = None
    total = 0
    page = 0
    
    while True:
        page += 1
        params = {
            'limit': 1000,
            'status': status
        }
        if cursor:
            params['cursor'] = cursor
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            markets = data.get('markets', [])
            total += len(markets)
            
            cursor = data.get('cursor')
            if not cursor:
                break
                
        except Exception as e:
            print(f"    ⚠️ Error counting {status}: {e}")
            break
    
    return total

def count_total_markets(base_url):
    """Count all markets regardless of status."""
    cursor = None
    total = 0
    page = 0
    
    print("  Counting pages: ", end="", flush=True)
    
    while True:
        page += 1
        params = {'limit': 1000}
        if cursor:
            params['cursor'] = cursor
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            markets = data.get('markets', [])
            total += len(markets)
            
            # Show progress
            if page % 10 == 0:
                print(f"{page}", end="", flush=True)
            else:
                print(".", end="", flush=True)
            
            cursor = data.get('cursor')
            if not cursor:
                print(f" Total: {total:,}")
                break
                
        except Exception as e:
            print(f"\n  ❌ Error on page {page}: {e}")
            break
        
        time.sleep(0.05)  # Small delay
    
    return total

def get_sample_statistics(base_url):
    """Get a sample of markets to estimate data characteristics."""
    params = {'limit': 100}
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        
        markets = data.get('markets', [])
        
        if not markets:
            return {}
        
        # Analyze the sample
        stats = {
            'sample_size': len(markets),
            'avg_size_bytes': 0,
            'unique_events': set(),
            'unique_series': set(),
            'categories': defaultdict(int),
            'date_range': {'earliest': None, 'latest': None}
        }
        
        total_size = 0
        earliest_date = None
        latest_date = None
        
        for market in markets:
            # Estimate size of market data
            market_str = str(market)
            total_size += len(market_str.encode('utf-8'))
            
            # Extract unique identifiers
            if 'event_ticker' in market:
                stats['unique_events'].add(market['event_ticker'])
            
            ticker = market.get('ticker', '')
            if ticker and '-' in ticker:
                series = ticker.split('-')[0]
                stats['unique_series'].add(series)
            
            # Track dates
            if 'close_time' in market:
                close_time = market['close_time']
                if not earliest_date or close_time < earliest_date:
                    earliest_date = close_time
                if not latest_date or close_time > latest_date:
                    latest_date = close_time
        
        stats['avg_size_bytes'] = total_size // len(markets)
        stats['unique_events'] = len(stats['unique_events'])
        stats['unique_series'] = len(stats['unique_series'])
        stats['date_range']['earliest'] = earliest_date
        stats['date_range']['latest'] = latest_date
        
        return stats
        
    except Exception as e:
        print(f"  ⚠️ Error getting sample: {e}")
        return {}

def calculate_estimates(total_count, sample_stats):
    """Calculate storage and processing estimates."""
    if not sample_stats or not sample_stats.get('avg_size_bytes'):
        return {}
    
    avg_size = sample_stats['avg_size_bytes']
    
    estimates = {
        'total_size_mb': (total_count * avg_size) / (1024 * 1024),
        'total_size_gb': (total_count * avg_size) / (1024 * 1024 * 1024),
        'download_time_estimate': {
            'fast_connection': f"{(total_count / 1000) * 0.5:.1f} seconds",
            'average_connection': f"{(total_count / 1000) * 2:.1f} seconds",
            'slow_connection': f"{(total_count / 1000) * 5:.1f} seconds"
        },
        'memory_required_mb': (total_count * avg_size * 1.5) / (1024 * 1024),  # 1.5x for processing overhead
        'estimated_unique_events': sample_stats.get('unique_events', 0) * (total_count / sample_stats.get('sample_size', 100)),
        'estimated_unique_series': sample_stats.get('unique_series', 0) * (total_count / sample_stats.get('sample_size', 100))
    }
    
    return estimates

def display_results(stats):
    """Display the results in a formatted way."""
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    
    total = stats['total_markets']
    print(f"\n🎯 TOTAL MARKETS: {total:,}")
    
    if stats['status_breakdown']:
        print("\n📊 Status Breakdown:")
        for status, count in stats['status_breakdown'].items():
            if count > 0:
                percentage = (count / total) * 100 if total > 0 else 0
                print(f"  • {status:10}: {count:8,} ({percentage:5.1f}%)")
    
    if stats.get('sample_stats'):
        sample = stats['sample_stats']
        print(f"\n📈 Sample Analysis (based on {sample.get('sample_size', 0)} markets):")
        if sample.get('avg_size_bytes'):
            print(f"  • Average market size: {sample['avg_size_bytes']:,} bytes")
        if sample.get('date_range', {}).get('earliest'):
            print(f"  • Date range: {sample['date_range']['earliest'][:10]} to {sample['date_range']['latest'][:10]}")
    
    if stats.get('estimates'):
        est = stats['estimates']
        print("\n💾 Storage Estimates:")
        print(f"  • Total JSON size: ~{est['total_size_mb']:.1f} MB ({est['total_size_gb']:.2f} GB)")
        print(f"  • Memory required: ~{est['memory_required_mb']:.1f} MB")
        
        print("\n⏱️ Download Time Estimates (at 1000 markets/page):")
        print(f"  • Fast connection:    ~{est['download_time_estimate']['fast_connection']}")
        print(f"  • Average connection: ~{est['download_time_estimate']['average_connection']}")
        print(f"  • Slow connection:    ~{est['download_time_estimate']['slow_connection']}")
        
        print("\n📁 Expected File Counts (if grouped):")
        print(f"  • Unique events: ~{int(est['estimated_unique_events']):,}")
        print(f"  • Unique series: ~{int(est['estimated_unique_series']):,}")
    
    print("\n" + "=" * 60)
    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

def quick_check():
    """Ultra-fast check - just get first page to see what we're dealing with."""
    url = "https://api.elections.kalshi.com/trade-api/v2/markets"
    
    print("\n🚀 QUICK CHECK (first page only)")
    print("-" * 40)
    
    try:
        response = requests.get(url, params={'limit': 1})
        data = response.json()
        
        if data.get('markets'):
            market = data['markets'][0]
            print("\nFirst market example:")
            print(f"  Ticker: {market.get('ticker')}")
            print(f"  Status: {market.get('status')}")
            print(f"  Title:  {market.get('title', 'N/A')[:60]}")
            
            # Estimate total based on pagination
            response2 = requests.get(url, params={'limit': 1000})
            data2 = response2.json()
            
            if data2.get('cursor'):
                print("\n⚠️ Multiple pages detected!")
                print("  There are MANY markets (likely 100K+)")
                print("  Run full count for exact numbers.")
            else:
                print(f"\n✅ Total markets: {len(data2.get('markets', []))}")
        
    except Exception as e:
        print(f"Error: {e}")

def main():
    """Main function with menu options."""
    print("\n🎯 KALSHI MARKET COUNTER")
    print("=" * 60)
    print("Options:")
    print("  1. Quick check (5 seconds)")
    print("  2. Full count with breakdown (1-5 minutes)")
    print("  3. Count by status only (30 seconds)")
    
    choice = input("\nSelect option (1-3, default=1): ").strip()
    
    if choice == '2':
        count_kalshi_markets(detailed_breakdown=True)
    elif choice == '3':
        url = "https://api.elections.kalshi.com/trade-api/v2/markets"
        print("\n📊 Counting by status...")
        for status in ['open', 'closed', 'settled', 'unopened']:
            count = count_by_status(url, status)
            print(f"  {status}: {count:,}")
    else:
        quick_check()
        print("\n💡 Run option 2 for complete statistics")

if __name__ == "__main__":
    main()