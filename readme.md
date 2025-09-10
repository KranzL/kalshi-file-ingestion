# kalshi market data fetcher

a fast, efficient tool for downloading all market data from kalshi's public api.

## what it does

this script downloads market data from kalshi, a prediction market platform with 500,000+ markets covering politics, sports, economics, weather, and more. it can fetch everything from current election odds to weather predictions to economic indicators.

## features

- **fast parallel downloading** - up to 30x faster than sequential fetching
- **handles massive datasets** - efficiently processes 500k+ markets
- **smart organization** - automatically groups markets by status, series, or event
- **memory efficient** - saves data in chunks to avoid crashes
- **resume friendly** - data saved immediately as it downloads
- **no authentication needed** - uses kalshi's public api

## installation

```bash
# clone the repo or download the script
git clone https://github.com/yourusername/kalshi-fetcher.git
cd kalshi-fetcher

# install required packages
pip3 install requests urllib3
```

## quick start

```bash
python3 kalshi_fetcher.py
```

follow the prompts:
1. choose download method (parallel recommended)
2. select market filter (start with "open" for testing)
3. optionally organize the data after download

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

```
kalshi_markets_20250109_120000/
├── chunks/                    # raw data chunks (one per api page)
│   ├── chunk_000001.json
│   ├── chunk_000002.json
│   └── ...
├── by_status/                 # organized by market status
│   ├── open.json
│   ├── closed.json
│   └── settled.json
├── by_series/                 # organized by series (inx, highnyc, etc)
│   ├── inx.json
│   ├── highnyc.json
│   └── ...
└── all_markets.json          # single file with all data (if requested)
```

## data organization options

after downloading, you can organize markets by:

1. **status** - groups markets by open/closed/settled
2. **series** - groups by market series (e.g., all s&p 500 markets together)
3. **event** - groups by specific events
4. **single file** - combines everything into one large json file

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

| market count | download time (10 workers) | download time (20 workers) | file size |
|--------------|---------------------------|---------------------------|-----------|
| ~3k (open)   | 30-60 seconds            | 15-30 seconds            | ~10mb     |
| ~50k (type)  | 2-5 minutes              | 1-3 minutes              | ~150mb    |
| 500k+ (all)  | 15-30 minutes            | 10-20 minutes            | ~1.5gb    |

## tips

- start with "open markets" to test everything works
- use more workers if you have good internet
- the chunks folder can be deleted after merging
- organized files are easier to work with than one giant file
- data includes historical markets going back to 2021

## troubleshooting

### ssl certificate errors
the script automatically bypasses ssl verification for compatibility with macos. this is safe for public data but don't use for sensitive operations.

### memory issues
if you run out of memory:
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