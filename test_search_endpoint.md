# Search Endpoint Test Guide

## Endpoint Details
- **URL**: `POST http://localhost:8080/search`
- **Content-Type**: `application/json`

## Request Body Format
```json
{
  "keywords": ["what is ai", "kindly tell me about llm"],
  "user_id": "user123",
  "device_id": "device456",
  "platform_name": "ios",
  "search_type": "all"
}
```

## Example Response
```json
{
  "success": true,
  "message": "Search request received and processed successfully",
  "keywords": ["what is ai", "kindly tell me about llm"],
  "user_id": "user123",
  "device_id": "device456",
  "platform_name": "ios",
  "search_type": "all",
  "timestamp": "2024-01-15T10:30:45Z"
}
```

## Testing with curl
```bash
curl -X POST http://localhost:8080/search \
  -H "Content-Type: application/json" \
  -d '{
    "keywords": ["what is ai", "kindly tell me about llm"],
    "user_id": "user123",
    "device_id": "device456",
    "platform_name": "ios",
    "search_type": "all"
  }'
```

## Testing with PowerShell (Windows)
```powershell
$body = @{
    keywords = @("what is ai", "kindly tell me about llm")
    user_id = "user123"
    device_id = "device456"
    platform_name = "ios"
    search_type = "all"
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8080/search" -Method POST -Body $body -ContentType "application/json"
```

## Running the Server
1. Start the server: `go run main.go`
2. The server will start on port 8080
3. You'll see console output when requests are received
4. The server will print all received data to the console
