# AI Screenshot Module

FastAPI service for capturing webpage screenshots with PostgreSQL caching and S3 storage.

## ⚡ Performance Optimizations

- **50 concurrent requests** support (5 browsers × 10 tabs each)
- **Non-blocking database caching** with 500ms timeout
- **Fast database timeouts** (5s connection, 2s queries)
- **Configurable caching** - can be disabled for max performance

## Project Structure

```
├── app/
│   ├── core/           # Configuration and logging
│   ├── db/             # Database connection and operations
│   ├── main.py         # FastAPI application and routes
│   └── models/         # Pydantic models and schemas
├── run.py              # Application entry point
├── requirements.txt    # Python dependencies
└── README.md          # This file
```

## Setup

```bash
# Install dependencies
pip install -r requirements.txt
playwright install chromium

# Setup PostgreSQL
createdb blink_ai_db
psql -d blink_ai_db -c "CREATE TABLE screenshots (id SERIAL PRIMARY KEY, url VARCHAR(255) UNIQUE, output_path VARCHAR(255), slices JSONB, timestamp_column TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"

# Create .env file
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=your_password
DB_NAME=blink_ai_db
S3_BUCKET_NAME=your-bucket
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
BASE_PATH=https://your-cdn.com

# Run server
python run.py
```

## ⚡ Performance Tuning

### Database Caching Configuration

Add these optional settings to your `.env` file for performance optimization:

```env
# Performance tuning (optional)
DB_ENABLE_CACHING=true          # Set to false to disable all DB operations (max performance)
DB_CACHE_TIMEOUT=0.5            # Cache check timeout in seconds (default: 0.5)
DB_STORAGE_TIMEOUT=1.0          # Storage timeout in seconds (default: 1.0)
```

### Performance Optimizations Applied

- **Non-blocking database operations**: Database cache checks timeout after 500ms
- **Fast database timeouts**: 5s connection timeout, 2s query timeout
- **Configurable caching**: Can be disabled entirely for maximum performance
- **50 concurrent request support**: 5 browsers × 10 tabs configuration

## API Endpoints

**POST /screenshot/**
```json
{
  "url": "https://example.com",
  "ux_type": 1,
  "ss_width": 1920,
  "ss_height": 915
}
```

**GET /health** - Health check  
**GET /screenshot-status** - Queue status  
**GET /screenshot-health** - Detailed diagnostics
