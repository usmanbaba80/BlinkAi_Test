from pydantic import BaseModel, HttpUrl, Field, validator
from typing import Optional, Literal
import re

class ScreenshotRequest(BaseModel):
    """Request model for screenshot endpoint with proper validation."""
    
    url: HttpUrl = Field(..., description="Valid URL to take screenshot of")
    ux_type: int = Field(ge=0, le=10, description="UX type identifier (0-10)")
    ss_width: int = Field(ge=100, le=4000, description="Screenshot width in pixels")
    ss_height: int = Field(ge=100, le=4000, description="Screenshot height in pixels")
    output_base_path: str = Field(
        default='https://usc1.contabostorage.com/gsdatasync/',
        description="Base path for output files"
    )
    browser_type: Literal['chromium', 'firefox', 'webkit'] = Field(
        default='chromium',
        description="Browser type to use"
    )
    full_page: bool = Field(default=True, description="Take full page screenshot")
    executable_path: Optional[str] = Field(None, description="Custom browser executable path")

    @validator('url')
    def validate_url_scheme(cls, v):
        """Ensure URL has valid scheme."""
        if str(v).startswith(('http://', 'https://')):
            return v
        raise ValueError('URL must start with http:// or https://')

# class SearchRequest(BaseModel):
#     """Request model for search endpoint with proper validation."""
    
#     query: str = Field(
#         ..., 
#         min_length=1, 
#         max_length=500,
#         description="Search query (1-500 characters)"
#     )
#     searchType: Literal['general', 'nws', 'isch', 'shop'] = Field(
#         ...,
#         description="Type of search: general, nws (news), isch (images), shop (shopping)"
#     )
#     start: int = Field(
#         default=0, 
#         ge=0, 
#         le=1000,
#         description="Start index for pagination (0-1000)"
#     )
#     limit: int = Field(
#         default=5, 
#         ge=1, 
#         le=100,
#         description="Limit of results per page (1-100)"
#     )

#     @validator('query')
#     def validate_query(cls, v):
#         """Validate search query for basic safety."""
#         # Remove potentially dangerous characters
#         if re.search(r'[<>"\']', v):
#             raise ValueError('Query contains invalid characters')
#         return v.strip()

class LinkRequest(BaseModel):
    """Request model for links endpoint with proper validation."""
    
    url: HttpUrl = Field(..., description="Valid URL to extract links from")

    @validator('url')
    def validate_url_scheme(cls, v):
        """Ensure URL has valid scheme."""
        if str(v).startswith(('http://', 'https://')):
            return v
        raise ValueError('URL must start with http:// or https://')



# Response models for better API documentation
class SuccessResponse(BaseModel):
    """Standard success response model."""
    
    status_code: int = Field(200, description="HTTP status code")
    success: bool = Field(True, description="Operation success flag")
    message: str = Field(..., description="Success message")

class ErrorResponse(BaseModel):
    """Standard error response model."""
    
    status_code: int = Field(..., description="HTTP status code")
    success: bool = Field(False, description="Operation success flag")
    message: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error information") 