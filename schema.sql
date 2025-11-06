-- Database schema for AI Search Screenshot Module
-- PostgreSQL compatible

-- Create database (optional - uncomment if you want to create database)
-- CREATE DATABASE blinksearch_db;
-- \c blinksearch_db;

-- Create screenshots table
CREATE TABLE IF NOT EXISTS screenshots (
    id SERIAL PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
    output_path TEXT,
    slices JSONB,
    timestamp_column TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create index on URL for faster lookups
CREATE INDEX IF NOT EXISTS idx_screenshots_url ON screenshots(url);

-- Create index on timestamp for ordering
CREATE INDEX IF NOT EXISTS idx_screenshots_timestamp ON screenshots(timestamp_column DESC);

-- Create index on created_at for analytics
CREATE INDEX IF NOT EXISTS idx_screenshots_created_at ON screenshots(created_at DESC);

-- Optional: Create a partial index for recent entries (last 30 days)
-- CREATE INDEX IF NOT EXISTS idx_screenshots_recent ON screenshots(url, timestamp_column DESC)
-- WHERE timestamp_column > CURRENT_TIMESTAMP - INTERVAL '30 days';

-- Grant permissions (adjust as needed for your setup)
-- GRANT SELECT, INSERT, UPDATE ON screenshots TO your_app_user;
-- GRANT USAGE ON SEQUENCE screenshots_id_seq TO your_app_user;
