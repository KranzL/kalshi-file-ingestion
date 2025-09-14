# Kalshi Data Ingestion Suite

Comprehensive tools for downloading market data from Kalshi's public prediction market API with atomic processing and network resilience.

## Overview

This suite provides reliable data ingestion from Kalshi, a prediction market platform with over 500,000 markets covering politics, sports, economics, weather, and more. The system uses atomic endpoint processing to ensure complete data collection even with network interruptions.

## Core Features

### Atomic Processing Engine
- Each endpoint processed completely from start to finish
- No cross-endpoint state contamination
- Automatic recovery from network timeouts
- Extended retry sequences with exponential backoff

### Network Resilience
- 60-second request timeouts with 90-second recovery timeouts
- Up to 10 timeout recovery attempts per failure
- Total recovery time up to 55 minutes per network issue
- Adaptive rate limiting prevents API throttling

### Smart Organization
- Hierarchical folder structure prevents file system crashes
- 1000 files per batch folder maximum
- Endpoint-specific organization (markets, events, series)
- Content-hash based deduplication within endpoints

### Comprehensive Logging
- Real-time progress tracking with pages per second metrics
- Detailed network failure reporting and recovery attempts
- Complete session summaries with success rates
- Individual endpoint completion verification

## Installation

```bash
git clone https://github.com/yourusername/kalshi-file-ingestion.git
cd kalshi-file-ingestion
pip3 install requests urllib3
```

## Quick Start

### Standard Usage
```bash
python3 ingestion.py
```

### With API Discovery
```bash
python3 api_discovery.py
python3 ingestion.py
```

The ingestion automatically loads discovered endpoints if available, otherwise uses defaults for markets, events, and series endpoints.

## Data Structure

### Markets
Individual tradeable contracts on specific outcomes. Each market contains:
- ticker: Unique market identifier  
- title: Market question or description
- status: Current trading status
- yes_price/no_price: Current contract prices
- volume: Total contracts traded
- close_time: Market expiration timestamp

### Events  
Collections of related markets grouped around single real-world events. Contains:
- event_ticker: Event identifier
- title: Event description
- markets: List of associated market tickers
- close_time: Event resolution date

### Series
Higher-level categories containing multiple related events over time. Includes:
- series_ticker: Series identifier
- title: Series description  
- frequency: Recurring pattern information
- events: Associated event list

## Performance Characteristics

### Request Optimization
- Markets endpoint: 1000 records per request
- Events endpoint: 100 records per request (API limit)
- Series endpoint: 200 records per request
- Adaptive delays prevent rate limiting

### Expected Processing Times
- Small datasets (under 50K records): 5-15 minutes
- Medium datasets (100K-500K records): 15-45 minutes  
- Large datasets (1M+ records): 45-90 minutes
- Network issues extend times proportionally

### Output Organization
```
kalshi_atomic_ingestion_20240914_105307/
├── atomic_ingestion.log
├── atomic_session_summary_20240914_111603.json
├── markets/
│   ├── batch_0000/ (records 0-999)
│   ├── batch_0001/ (records 1000-1999)
│   └── batch_NNNN/ (continuing...)
├── events/
│   └── batch_0000/
└── series/
    └── batch_0000/
```

## Network Resilience Features

### Timeout Handling
Normal request flow uses 5 retry attempts with 60-second timeouts. When timeouts persist, the system enters extended recovery mode with:

1. Initial 60-second wait and retry
2. 120-second wait and retry  
3. 180-second wait and retry
4. Continue up to 10 attempts (total 55+ minutes)
5. Only abort after exhaustive recovery attempts

### Failure Recovery
The system handles network issues gracefully:
- Logs exact failure points with record counts
- Saves all successfully retrieved data
- Reports incomplete vs complete endpoints
- Provides detailed failure analysis in session summary

### Data Integrity
- Content-hash based deduplication prevents duplicates within endpoints
- No cross-endpoint duplicate detection to maintain data integrity
- Each record includes hash verification for consistency checking
- Batch processing with automatic checkpoint saves

## File Format

Each saved record follows this structure:
```json
{
  "endpoint": "/markets",
  "ingestion_time": "20240914_111603", 
  "record_id": "TRUMPWIN-24NOV05",
  "record_hash": "a1b2c3d4e5f6789",
  "batch_info": {
    "record_index": 1500,
    "batch_number": 1,
    "total_records": 1030000
  },
  "data": {
    // Original API response data
  }
}
```

## Troubleshooting

### Network Timeouts
The system automatically handles network issues with extended retry sequences. Monitor logs for:
- "TIMEOUT RECOVERY MODE" messages indicate extended retry attempts
- "Network recovered" messages confirm successful recovery
- "TIMEOUT RECOVERY FAILED" indicates network issues exceeded recovery limits

### Incomplete Datasets  
If network issues prevent complete ingestion:
- Check logs for failure point and record count before failure
- Session summary shows successful vs failed endpoints
- Re-run ingestion to attempt completion from beginning
- Each run is independent with no state carried over

### Memory Usage
The system processes data in streaming fashion:
- Records saved individually as processed
- No large objects held in memory
- Batch folders prevent directory size issues
- Memory usage remains constant regardless of dataset size

## API Information

- Base URL: https://api.elections.kalshi.com/trade-api/v2
- Authentication: Not required for public endpoints
- Rate limiting: Handled automatically with adaptive delays
- SSL verification: Disabled for compatibility (public data only)

## Session Summary

Each ingestion generates a comprehensive session summary containing:
- Total records processed and files saved
- Success rates and failure analysis
- Processing speed and duration metrics
- Individual endpoint results with timing data
- Network issue reports and recovery statistics

This summary enables analysis of ingestion quality and identification of any data collection issues.