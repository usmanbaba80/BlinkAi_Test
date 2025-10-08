# System Prompts Configuration

## Overview
The system now loads active prompts from the `system_prompt` table and saves them to a literal text file (`prompts.txt`) on app startup.

## File Structure
```
BlinkAi_SearchEngineApps_Productivity_Backend/
├── database/
│   └── prompts.go          # Database query logic
├── config/
│   └── prompts.go          # File-based prompt management
├── prompts.txt             # Generated prompts file (created on startup)
└── ...
```

## How It Works

### 1. **Database Query Logic** (`database/prompts.go`)
- Contains `LoadActivePromptsFromDB()` function
- Queries the `system_prompt` table for active prompts
- Returns a map of `search_type -> prompt_text`

### 2. **File-Based Storage** (`config/prompts.go`)
- Loads prompts from database on startup
- Saves prompts to `prompts.txt` file
- Stores prompts in memory for fast access
- Thread-safe with mutex locks

### 3. **Generated File Format** (`prompts.txt`)
```
# System Prompts Configuration
# Format: search_type=prompt_text
# Generated on app startup

all="You are a helpful search assistant that provides comprehensive information."
video="You are a video search specialist that finds relevant video content."
image="You are an image search expert that locates visual content."
default="You are a general-purpose AI assistant."
```

## Usage in Search Handler

When a search request comes in:
1. Handler calls `config.SystemPrompts.GetPrompt(req.SearchType)`
2. Returns the appropriate prompt for the search type
3. Falls back to "default" prompt if specific type not found
4. Includes the prompt in the response

## Example Response
```json
{
  "success": true,
  "message": "Search request received and processed successfully",
  "keywords": ["what is ai"],
  "user_id": "user123",
  "device_id": "device456",
  "platform_name": "ios",
  "search_type": "video",
  "system_prompt": "You are a video search specialist that finds relevant video content.",
  "timestamp": "2024-01-15T10:30:45Z"
}
```

## Benefits
- ✅ **Persistent Storage**: Prompts saved to file for reference
- ✅ **Fast Access**: In-memory storage for quick retrieval
- ✅ **Thread-Safe**: Concurrent access handled safely
- ✅ **Fallback Support**: Default prompt when specific type not found
- ✅ **Separation of Concerns**: Database logic separated from config logic
