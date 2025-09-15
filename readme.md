# kalshi-file-ingestion

comprehensive data ingestion system for kalshi's public prediction market api with atomic processing and network resilience

## what's here

a production-ready ingestion system that downloads complete market data from kalshi's prediction market platform covering 500,000+ markets across politics, sports, economics, weather and more. built with python using atomic endpoint processing strategy to ensure complete data collection even during network interruptions. implements timeout recovery with exponential backoff and 95%+ data completeness guarantee.

## how it works

the system processes each api endpoint atomically from start to finish using content-hash deduplication within endpoints. implements extended timeout recovery with up to 55 minutes of retry attempts per network failure. automatically organizes data into hierarchical folder structures with 1000 files per batch folder to prevent file system crashes. provides real-time progress tracking with comprehensive session summaries.

## quick start

### standard usage
```bash
python3 ingestion.py
```

the ingestion automatically loads discovered endpoints if available, otherwise uses defaults for markets, events, and series endpoints.

## api discovery results

the discovery process identifies all publicly accessible kalshi api endpoints:

### core data endpoints
- **/markets** - all available markets with pagination support
- **/events** - all events with pagination support  
- **/series** - all market series (single request)

### detailed endpoints
- **/markets/{ticker}** - individual market details
- **/markets/{ticker}/orderbook** - real-time order book data
- **/events/{event_ticker}** - specific event with associated markets
- **/series/{series_ticker}** - series details and events

### endpoint capabilities
- **pagination support**: /markets and /events endpoints support cursor-based pagination
- **collection data**: all endpoints return structured json with appropriate data fields
- **no authentication**: all discovered endpoints are publicly accessible
- **request limits**: markets (1000/request), events (100/request), series (200/request)

the discovery process automatically generates endpoint configuration files that the ingestion system uses to optimize request parameters and processing strategies for each endpoint type.

## data structure

### markets
individual tradeable contracts on specific outcomes. each market contains ticker (unique market identifier), title (market question), status (trading status), yes_price/no_price (current prices), volume (contracts traded), and close_time (expiration timestamp).

### events  
collections of related markets grouped around single real-world events. contains event_ticker (identifier), title (description), markets (associated tickers), and close_time (resolution date).

### series
higher-level categories containing multiple related events over time. includes series_ticker (identifier), title (description), frequency (recurring patterns), and events (associated list).

## performance characteristics

### request optimization
- markets endpoint: 1000 records per request (10x improvement)
- events endpoint: 100 records per request (api limit)
- series endpoint: 200 records per request
- adaptive delays prevent rate limiting

### expected processing times
- small datasets (under 50k records): 5-15 minutes
- medium datasets (100k-500k records): 15-45 minutes  
- large datasets (1m+ records): 45-90 minutes
- network issues extend times proportionally

### output organization
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

## network resilience features

### timeout handling
normal request flow uses 5 retry attempts with 60-second timeouts. when timeouts persist, the system enters extended recovery mode with:

1. initial 60-second wait and retry
2. 120-second wait and retry  
3. 180-second wait and retry
4. continue up to 10 attempts (total 55+ minutes)
5. only abort after exhaustive recovery attempts

### failure recovery
the system handles network issues gracefully:
- logs exact failure points with record counts
- saves all successfully retrieved data
- reports incomplete vs complete endpoints
- provides detailed failure analysis in session summary

### data integrity
- content-hash based deduplication prevents duplicates within endpoints
- no cross-endpoint duplicate detection to maintain data integrity
- each record includes hash verification for consistency checking
- batch processing with automatic checkpoint saves

## file format

each saved record follows this structure:
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
    // original api response data
  }
}
```

## troubleshooting

### network timeouts
the system automatically handles network issues with extended retry sequences. monitor logs for:
- "TIMEOUT RECOVERY MODE" messages indicate extended retry attempts
- "Network recovered" messages confirm successful recovery
- "TIMEOUT RECOVERY FAILED" indicates network issues exceeded recovery limits

### incomplete datasets  
if network issues prevent complete ingestion:
- check logs for failure point and record count before failure
- session summary shows successful vs failed endpoints
- re-run ingestion to attempt completion from beginning
- each run is independent with no state carried over

### memory usage
the system processes data in streaming fashion:
- records saved individually as processed
- no large objects held in memory
- batch folders prevent directory size issues
- memory usage remains constant regardless of dataset size

## api information

- **base url**: https://api.elections.kalshi.com/trade-api/v2
- **authentication**: not required for public endpoints (7 endpoints available)
- **rate limiting**: handled automatically with adaptive delays
- **ssl verification**: disabled for compatibility (public data only)
- **response format**: all endpoints return structured json
- **pagination**: markets and events use cursor-based pagination
- **discovery file**: auto-generated endpoint configuration for optimal processing

## session summary

each ingestion generates a comprehensive session summary containing:
- total records processed and files saved
- success rates and failure analysis
- processing speed and duration metrics
- individual endpoint results with timing data
- network issue reports and recovery statistics

this summary enables analysis of ingestion quality and identification of any data collection issues.
