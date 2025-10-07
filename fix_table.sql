-- Drop the existing tables if they have wrong column types
DROP TABLE IF EXISTS trends_now;
DROP TABLE IF EXISTS query_results;

-- Create the trends_now table with correct column types
CREATE TABLE trends_now (
    id SERIAL PRIMARY KEY,
    trends_data JSONB NOT NULL,
    location VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- Create the query_results table for image search caching
CREATE TABLE query_results (
    idx SERIAL PRIMARY KEY,
    query TEXT,
    searchType VARCHAR(512),
    location VARCHAR(255) DEFAULT 'US',
    results JSONB,
    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Verify the table structures
\d trends_now
\d query_results

