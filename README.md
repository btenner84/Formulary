# Medicare Part D Intelligence Platform

A comprehensive web application for analyzing Medicare Part D prescription drug plans, formularies, and pricing data.

## Features

- **Year-over-Year Comparison**: Toggle between 2025 and 2026 plan data
- **Organization Intelligence**: Browse plans by parent organization sorted by enrollment
- **Formulary Analysis**: Detailed drug coverage, restrictions, and cost structures
- **Pricing Insights**: Member cost calculations (copay vs coinsurance)
- **Geographic Coverage**: State and county-level plan availability

## Technology Stack

- **Backend**: FastAPI + DuckDB
- **Data Storage**: AWS S3 (Parquet files)
- **Frontend**: Vanilla JavaScript with modern UI
- **Deployment**: Railway

## Data Sources

- CMS SPUF (Standard Plan Utility Files)
- CMS Monthly Enrollment Reports
- Updated quarterly with latest Medicare Part D data

## Deployment

### Railway Deployment

1. Connect this repository to Railway
2. Set environment variables:
   - `AWS_ACCESS_KEY_ID`
   - `AWS_SECRET_ACCESS_KEY`
   - `S3_BUCKET=formulary2026`
   - `USE_S3=true`
3. Railway will automatically detect and deploy using the Procfile

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret

# Run the app
uvicorn webapp.main:app --reload
```

## License

For educational and research purposes.

