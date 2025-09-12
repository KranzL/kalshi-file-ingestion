# kalshi data ingestion suite

fast, efficient tools for downloading market data from kalshi's public api with flexible endpoint discovery and organized storage.

## what it does

this suite includes two complementary tools:
- **api_discovery.py** - automatically discovers all available kalshi api endpoints
- **flexible_ingestion.py** - intelligently ingests data from any discovered endpoint with smart folder organization

downloads market data from kalshi, a prediction market platform with 500,000+ markets covering politics, sports, economics, weather, and more.

## features

### flexible ingestion (flexible_ingestion.py)
- **interactive menu system** - choose between bulk or targeted ingestion
- **unlimited pagination** - fetches all available data automatically
- **smart folder organization** - prevents crashes with 600k+ files
- **hierarchical batching** - groups files into manageable 1,000-file batches
- **endpoint-specific folders** - separates markets, events, series data
- **progress tracking** - real-time batch completion updates
- **no authentication needed** - uses kalshi's public api

### api discovery (api_discovery.py)
- **automatic endpoint discovery** - finds all available api endpoints
- **capability detection** - identifies pagination and parameter requirements
- **fast parallel downloading** - up to 30x faster than sequential fetching
- **handles massive datasets** - efficiently processes 500k+ markets
- **memory efficient** - saves data in chunks to avoid crashes

## installation

```bash
# clone the repo or download the script
git clone https://github.com/yourusername/kalshi-fetcher.git
cd kalshi-fetcher

# install required packages
pip3 install requests urllib3
```

## quick start

### flexible ingestion (recommended)
```bash
python3 flexible_ingestion.py
```

follow the interactive menu:
1. choose ingestion type: bulk (all data) or interactive (specific endpoints)
2. script automatically creates organized folder structure
3. data saves in real-time with batch progress tracking

### api discovery + legacy fetcher
```bash
python3 api_discovery.py    # discover endpoints first
python3 kalshi_fetcher.py   # then use legacy fetcher
```

## usage options

### download methods

- **parallel (recommended)** - uses multiple workers to fetch data simultaneously
- **sequential** - fetches one page at a time, slower but more stable

### market filters

- **all markets** - downloads everything (500k+ markets, ~1-2gb)
- **open markets** - only active markets (~3k markets)
- **closed markets** - completed but not settled markets
- **settled markets** - finished markets with results

### parallel workers

more workers = faster download (default: 10, max: 30)
- 10 workers: ~15-30 minutes for all markets
- 20 workers: ~10-20 minutes for all markets
- 30 workers: ~5-15 minutes for all markets

## output structure

### flexible ingestion (new organized structure)
```
kalshi_ingestion_20250912_143052/
├── markets/                   # markets endpoint data
│   ├── batch_0000/           # records 0-999 (1000 files max)
│   │   ├── kalshi_markets_TRUMPWIN_20250912.json
│   │   ├── kalshi_markets_BIDENWIN_20250912.json
│   │   └── ... (998 more files)
│   ├── batch_0001/           # records 1000-1999
│   ├── batch_0002/           # records 2000-2999
│   └── ... (organized into manageable chunks)
├── events/                    # events endpoint data
│   ├── batch_0000/
│   └── batch_0001/
├── series/                    # series endpoint data
│   └── batch_0000/
└── kalshi_bulk_ingestion_summary_20250912.json
```

### legacy fetcher structure
```
kalshi_markets_20250109_120000/
├── chunks/                    # raw data chunks
├── by_status/                 # organized by status
├── by_series/                 # organized by series
└── all_markets.json          # single consolidated file
```

## ingestion options

### flexible ingestion modes

**bulk ingestion** - automatically ingests all available data from all endpoints
- discovers and processes every api endpoint
- unlimited pagination (fetches everything available)  
- smart folder organization prevents file system crashes
- ideal for comprehensive data collection

**interactive ingestion** - targeted data collection with user control
- select specific endpoints to ingest
- control pagination limits and parameters
- choose sample data or custom identifiers
- perfect for focused research or testing

### folder organization features

- **endpoint separation** - markets, events, series in separate folders
- **batch management** - maximum 1,000 files per folder
- **vs code friendly** - prevents editor crashes with large datasets
- **progress tracking** - real-time batch completion updates
- **metadata inclusion** - each file contains batch and index information

### legacy data organization

after downloading with kalshi_fetcher.py, organize by:
1. **status** - groups markets by open/closed/settled  
2. **series** - groups by market series
3. **event** - groups by specific events
4. **single file** - combines everything into one json

## market data structure

each market object contains:
- `ticker` - unique market identifier
- `title` - market question/title
- `status` - open, closed, or settled
- `yes_price` / `no_price` - current trading prices
- `volume` - total contracts traded
- `close_time` - when market closes
- `event_ticker` - parent event identifier
- and more...

## example workflow

### quick test (3k markets, ~1 minute)
```
1. run script
2. select "1" for parallel
3. select "b" for open markets only
4. enter to use default 10 workers
5. wait ~1 minute
6. select "y" to organize
7. select "1" to group by status
```

### full download (500k+ markets, ~20 minutes)
```
1. run script
2. select "1" for parallel
3. select "a" for all markets
4. enter "20" for 20 workers
5. wait ~20 minutes
6. select "y" to organize
7. select "2" to group by series
```

## performance expectations

### flexible ingestion performance
| data type | record count | ingestion time | storage size | folder structure |
|-----------|--------------|----------------|--------------|------------------|
| sample test | ~1k records | 30-60 seconds | ~5mb | 1 batch folder |
| single endpoint | ~50k records | 5-15 minutes | ~200mb | 50 batch folders |
| bulk ingestion | 600k+ records | 30-60 minutes | ~2-3gb | 600+ batch folders |

*times vary based on api response speed and network conditions*

### legacy fetcher performance  
| market count | download time (10 workers) | download time (20 workers) | file size |
|--------------|---------------------------|---------------------------|-----------|
| ~3k (open)   | 30-60 seconds            | 15-30 seconds            | ~10mb     |
| ~50k (type)  | 2-5 minutes              | 1-3 minutes              | ~150mb    |
| 500k+ (all)  | 15-30 minutes            | 10-20 minutes            | ~1.5gb    |

## tips

### flexible ingestion tips
- start with interactive mode to test specific endpoints
- bulk ingestion handles 600k+ files without crashing editors
- each batch folder contains max 1,000 files for optimal performance  
- progress tracking shows real-time batch completion
- organized structure makes data analysis much easier

### legacy fetcher tips
- start with "open markets" to test everything works
- use more workers if you have good internet
- the chunks folder can be deleted after merging
- organized files are easier to work with than one giant file
- data includes historical markets going back to 2021

## troubleshooting

### ssl certificate errors
the script automatically bypasses ssl verification for compatibility with macos. this is safe for public data but don't use for sensitive operations.

### memory issues
flexible ingestion automatically handles memory efficiently:
- saves files individually as they're processed
- organizes into small batch folders (1,000 files max)
- no large objects held in memory

legacy fetcher memory issues:
- the script saves chunks automatically, so data isn't lost
- try organizing into grouped files instead of one large file
- process chunks separately if needed

### slow downloads
- increase worker count (up to 30)
- check your internet connection
- try during off-peak hours

## api notes

- uses kalshi's public api at `api.elections.kalshi.com`
- no authentication required
- rate limiting is handled automatically
- api returns max 1000 markets per request

## use cases

- **data analysis** - analyze prediction market trends
- **trading bots** - build automated trading strategies
- **research** - study market efficiency and predictions
- **monitoring** - track specific events or categories
- **archival** - preserve historical market data

## license

mit - feel free to use, modify, and distribute

## contributing

improvements welcome! please submit pull requests or open issues for bugs/features.

## disclaimer

this tool downloads public market data only. always respect kalshi's terms of service and api usage guidelines.