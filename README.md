# AI Screenshot Module

FastAPI service for capturing webpage screenshots with PostgreSQL caching and S3 storage.

## Project Structure

```
├── app/
│   ├── api/            # API routes
│   ├── core/           # Config and logging
│   ├── db/             # Database connection
│   ├── models/         # Pydantic models
│   ├── services/       # Business logic
│   └── main.py         # FastAPI app
├── run.py              # Application entry point
├── requirements.txt
└── README.md
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
