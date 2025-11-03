-- BlinkAi Merged Project Database Schema
-- This migration consolidates all tables from both original projects

-- Table: users
-- Used by: Search project
-- Stores: User and device information
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255),
    device_id UUID,
    platform_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Table: system_prompt
-- Used by: Search project
-- Stores: AI system prompts for different search types and platforms
CREATE TABLE IF NOT EXISTS system_prompt (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    search_type VARCHAR(50),
    prompt_text TEXT,
    version VARCHAR(20),
    platform VARCHAR(50),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Table: searches
-- Used by: Search project
-- Stores: Search request history for users
CREATE TABLE IF NOT EXISTS searches (
    search_id UUID PRIMARY KEY,
    user_id UUID REFERENCES users(id),
    system_prompt_id UUID REFERENCES system_prompt(id),
    keywords JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Table: search_response
-- Used by: Search project
-- Stores: Search responses and enriched content
CREATE TABLE IF NOT EXISTS search_response (
    id SERIAL PRIMARY KEY,
    search_id UUID REFERENCES searches(search_id),
    response_data JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Table: web_urls
-- Used by: Search project
-- Stores: Web search results
CREATE TABLE IF NOT EXISTS web_urls (
    id SERIAL PRIMARY KEY,
    search_id UUID REFERENCES searches(search_id),
    url_data JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Table: video_urls
-- Used by: Search project
-- Stores: Video search results
CREATE TABLE IF NOT EXISTS video_urls (
    id SERIAL PRIMARY KEY,
    search_id UUID REFERENCES searches(search_id),
    video_data JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Table: web_metadata
-- Used by: Search project
-- Stores: Web page metadata
CREATE TABLE IF NOT EXISTS web_metadata (
    id SERIAL PRIMARY KEY,
    search_id UUID REFERENCES searches(search_id),
    metadata JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Table: image_metadata
-- Used by: Search project
-- Stores: Image metadata from search results
CREATE TABLE IF NOT EXISTS image_metadata (
    id SERIAL PRIMARY KEY,
    search_id UUID REFERENCES searches(search_id),
    image_data JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Table: video_metadata
-- Used by: Search project
-- Stores: Video metadata from search results
CREATE TABLE IF NOT EXISTS video_metadata (
    id SERIAL PRIMARY KEY,
    search_id UUID REFERENCES searches(search_id),
    video_data JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Table: trends_now
-- Used by: Trends project
-- Stores: Cached trending data
CREATE TABLE IF NOT EXISTS trends_now (
    id SERIAL PRIMARY KEY,
    trends_data JSONB NOT NULL,
    location VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- Table: query_results
-- Used by: Images project
-- Stores: Cached image search results
CREATE TABLE IF NOT EXISTS query_results (
    idx SERIAL PRIMARY KEY,
    query TEXT,
    searchType VARCHAR(512),
    location VARCHAR(255) DEFAULT 'US',
    results JSONB,
    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_users_id ON users(id);
CREATE INDEX IF NOT EXISTS idx_users_device_id ON users(device_id);
CREATE INDEX IF NOT EXISTS idx_searches_user_id ON searches(user_id);
CREATE INDEX IF NOT EXISTS idx_searches_created_at ON searches(created_at);
CREATE INDEX IF NOT EXISTS idx_system_prompt_active ON system_prompt(is_active);
CREATE INDEX IF NOT EXISTS idx_system_prompt_type ON system_prompt(search_type);
CREATE INDEX IF NOT EXISTS idx_trends_location ON trends_now(location);
CREATE INDEX IF NOT EXISTS idx_query_results_query ON query_results(query);
CREATE INDEX IF NOT EXISTS idx_query_results_location ON query_results(location);

