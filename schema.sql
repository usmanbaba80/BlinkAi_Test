@@ -1,34 +0,0 @@
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