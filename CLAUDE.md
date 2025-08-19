# CS2 K-line Data Repository

## Database Overview

This repository contains a SQLite database (`kline.db`) that stores K-line data for CS2 (Counter-Strike 2) items.

### Database Schema

The `kline_data` table contains the following fields:
- `id`: Primary key
- `market_hash_name`: Item name (TEXT)
- `type_val`: Unique identifier for the item (TEXT)
- `timestamp`: Data timestamp in seconds (INTEGER)
- `open_price`: Opening price (REAL)
- `close_price`: Closing price (REAL)
- `high_price`: Highest price (REAL)
- `low_price`: Lowest price (REAL)
- `volume`: Trading volume (REAL)
- `turnover`: Trading turnover (REAL)
- `created_at`: Record creation timestamp (DATETIME)

### Data Content

The database currently contains:
- **27 unique CS2 items** with K-line data
- **Daily K-line data** from 2024-12-24 to 2025-08-18
- **6,345 total records** across all items
- **All timestamps are normalized to Beijing midnight (00:00:00 UTC+8)**

### Sample Items
- AK-47 | Asiimov (Minimal Wear)
- AK-47 | Bloodsport (Factory New)
- AK-47 | Fuel Injector (Factory New)
- AWP | Chrome Cannon (Factory New)
- And 23 other CS2 items

## Data Collection and Maintenance

**Important**: The data collection and maintenance for this database is handled manually through human operations. 

The process involves:
- Running `get_kline.py` to fetch new K-line data from the Steam API
- Manual verification of data integrity
- Periodic updates to maintain current data

**No automated maintenance is required** - the data collection process is managed manually and does not need to be considered for future development work.

## Key Files

- `get_kline.py`: Main data collection script (run manually)
- `kline.db`: SQLite database with all K-line data
- `watchlist.txt`: List of items to track
- `typeVal_matching_watchlist.txt`: Item name to typeVal mappings

## Data Usage

The K-line data can be used for:
- Price analysis and trend identification
- Market research for CS2 items
- Trading strategy development
- Historical price reference

All timestamps in the database are in UTC and should be converted to Beijing time (UTC+8) for proper interpretation as daily K-line data.