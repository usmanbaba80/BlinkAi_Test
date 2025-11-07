import asyncio
import sys

# Set WindowsSelectorEventLoopPolicy for Windows + Python 3.13 (supports subprocess)
if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import os
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from playwright.async_api import async_playwright
import io
import time
import random
import json
import cv2
import threading
import uuid
import numpy as np
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from datetime import datetime
from urllib.parse import urlparse
from typing import Tuple, List, Dict, Any, Optional
from contextlib import asynccontextmanager
from botocore.config import Config

# Additional imports for queue management
from collections import deque
from dataclasses import dataclass, field

# Import our custom modules
from app.core.config import settings
from app.models.schemas import ScreenshotRequest, SuccessResponse, ErrorResponse
from app.core.logging import logger

# Import database functions (PostgreSQL)
from app.db.database import (
    init_connection_pool,
    get_db_connection,
    execute_query,
    execute_update,
    close_connection_pool,
    connection_pool
)

# Note: Database connection pool functions (init_connection_pool, get_db_connection, etc.) 
# are now imported from database.py (PostgreSQL support)


class ScreenshotRequestQueue:
    """
    Manages screenshot request queuing, rate limiting, and performance metrics.
    Similar to SearchRequestQueue but optimized for screenshot operations.
    """
    
    def __init__(self):
        self.semaphore = asyncio.Semaphore(settings.screenshot_max_concurrent)
        self.active_requests = 0
        self.lock = asyncio.Lock()
        self.stats_lock = threading.Lock()  # For synchronous stats updates
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'queue_rejections': 0,
            'timeout_errors': 0,
            'avg_response_time': 0.0,
            'cache_hits': 0,
            'cache_misses': 0,
            'browser_creation_count': 0,
            'browser_reuse_count': 0
        }
        self.request_times = deque(maxlen=1000)
        
    async def acquire_screenshot_slot(self) -> bool:
        """Try to acquire a screenshot slot immediately (non-blocking)."""
        try:
            await asyncio.wait_for(self.semaphore.acquire(), timeout=0.001)
            with self.stats_lock:
                self.active_requests += 1
            return True
        except asyncio.TimeoutError:
            return False
    
    async def acquire_screenshot_slot_with_wait(self, timeout: int = None) -> bool:
        """Acquire screenshot slot with optional timeout (for queuing)."""
        if timeout is None:
            timeout = settings.screenshot_queue_timeout
        try:
            await asyncio.wait_for(self.semaphore.acquire(), timeout=timeout)
            with self.stats_lock:
                self.active_requests += 1
            return True
        except asyncio.TimeoutError:
            return False
    
    def release_screenshot_slot(self):
        """Release a screenshot processing slot."""
        try:
            with self.stats_lock:
                self.active_requests = max(0, self.active_requests - 1)
            self.semaphore.release()
        except Exception as e:
            logger.warning(f"⚠️ Error releasing screenshot slot: {e}")
    
    async def record_request_metrics(self, response_time: float, cache_hit: bool, success: bool, browser_created: bool = False):
        """Record metrics for a screenshot request."""
        async with self.lock:
            self.stats['total_requests'] += 1
            
            if success:
                self.stats['successful_requests'] += 1
            else:
                self.stats['failed_requests'] += 1
            
            if cache_hit:
                self.stats['cache_hits'] += 1
            else:
                self.stats['cache_misses'] += 1
            
            if browser_created:
                self.stats['browser_creation_count'] += 1
            else:
                self.stats['browser_reuse_count'] += 1
            
            # Update response time
            self.request_times.append(response_time)
            if self.request_times:
                self.stats['avg_response_time'] = sum(self.request_times) / len(self.request_times)
    
    async def get_status(self) -> Dict[str, Any]:
        """Get comprehensive status of screenshot queue and metrics."""
        async with self.lock:
            total_requests = self.stats['total_requests']
            success_rate = (self.stats['successful_requests'] / total_requests * 100) if total_requests > 0 else 0
            cache_hit_rate = (self.stats['cache_hits'] / (self.stats['cache_hits'] + self.stats['cache_misses']) * 100) if (self.stats['cache_hits'] + self.stats['cache_misses']) > 0 else 0
            
            recent_times = list(self.request_times)[-100:] if self.request_times else []
            recent_avg = sum(recent_times) / len(recent_times) if recent_times else 0
            
            return {
                'active_requests': self.active_requests,
                'max_concurrent': settings.screenshot_max_concurrent,
                'queue_utilization_percent': round((self.active_requests / settings.screenshot_max_concurrent) * 100, 2),
                'total_requests': total_requests,
                'successful_requests': self.stats['successful_requests'], 
                'failed_requests': self.stats['failed_requests'],
                'success_rate_percent': round(success_rate, 2),
                'queue_rejections': self.stats['queue_rejections'],
                'timeout_errors': self.stats['timeout_errors'],
                'avg_response_time_seconds': round(self.stats['avg_response_time'], 2),
                'recent_avg_response_time_seconds': round(recent_avg, 2),
                'cache_hits': self.stats['cache_hits'],
                'cache_misses': self.stats['cache_misses'],
                'cache_hit_rate_percent': round(cache_hit_rate, 2),
                'browser_creation_count': self.stats['browser_creation_count'],
                'browser_reuse_count': self.stats['browser_reuse_count'],
                'queue_enabled': settings.screenshot_enable_queuing,
                'queue_timeout_seconds': settings.screenshot_queue_timeout
            }

# Global instances for search optimization
# http_client_pool = HTTPClientPool()
# search_queue = SearchRequestQueue()

# Global instances for screenshot optimization  
screenshot_queue = ScreenshotRequestQueue()

# =====================================================================
# END SEARCH API OPTIMIZATION COMPONENTS
# =====================================================================

# Models are now imported from models.py


app = FastAPI(
    title="GS Backend API",
    description="High-performance web scraping, screenshot generation, and search functionality. Optimized for 1500+ concurrent requests.",
    version="2.1.0",
    docs_url="/docs",
    redoc_url="/redoc"
)


@app.on_event("startup")
async def startup_event():
    """Initialize resources on application startup."""
    try:
        await init_connection_pool()
        logger.info("✅ Database connection pool initialized at startup")
        logger.info("🚀 Starting Screenshot & Links API...")
        logger.info(f"📊 Browser Configuration: {settings.browser_pool_size} browsers × {settings.max_tabs_per_browser} tabs = {settings.browser_pool_size * settings.max_tabs_per_browser} total capacity")
        logger.info(f"⚡ Screenshot Rate limiting: {settings.screenshot_max_concurrent} concurrent screenshots")
        
        
        # Initialize and log screenshot optimization components
        logger.info("📷 Initializing Screenshot API optimization components...")
        
        # Log screenshot configuration
        logger.info(f"📷 Screenshot API Configuration:")
        logger.info(f"   - Max concurrent: {settings.screenshot_max_concurrent}")
        logger.info(f"   - Queue enabled: {settings.screenshot_enable_queuing}")
        logger.info(f"   - Queue size: {settings.screenshot_queue_size}")
        logger.info(f"   - Queue timeout: {settings.screenshot_queue_timeout}s")
        logger.info(f"   - Retry attempts: {settings.screenshot_retry_attempts}")
        logger.info(f"   - Screenshot timeout: {settings.screenshot_timeout}s")
        logger.info(f"   - Browser pool size: {settings.browser_pool_size}")
        logger.info(f"   - Max tabs per browser: {settings.max_tabs_per_browser}")
        logger.info(f"   - Concurrent browser creation: {settings.browser_launch_concurrent}")
        
        # Test screenshot queue functionality
        try:
            screenshot_status = await screenshot_queue.get_status()
            logger.info(f"✅ Screenshot queue manager initialized and ready")
        except Exception as e:
            logger.warning(f"⚠️ Screenshot queue status check failed: {e}")
        
        # Pre-warm the browser pool by initializing playwright
        try:
            logger.info("🔄 Starting browser pool initialization...")
            await browser_pool.initialize()
            logger.info("✅ Browser pool manager initialized")
        except Exception as e:
            logger.error(f"❌ CRITICAL: Browser pool initialization failed: {e}")
            logger.warning("⚠️ Server will start WITHOUT browser pool - screenshots will fail!")
            # Don't crash the server, but log the critical issue
        
        # Verify browser pool status after initialization
        try:
            pool_status = await browser_pool.get_pool_status()
            logger.info(f"📊 Browser pool verification:")
            logger.info(f"   • Total browsers created: {pool_status['total_browsers']}")
            logger.info(f"   • Max browsers configured: {pool_status['max_browsers']}")
            logger.info(f"   • Browser utilization: {pool_status['browser_utilization_percent']}%")
            logger.info(f"   • Browser details: {len(pool_status['browsers'])} browsers ready")
            
            if pool_status['total_browsers'] >= pool_status['max_browsers']:
                logger.info("🚀 Browser pool is FULLY READY for concurrent requests!")
            else:
                logger.warning(f"⚠️ Browser pool INCOMPLETE: {pool_status['total_browsers']}/{pool_status['max_browsers']} browsers")
                
            # Show individual browser status
            for browser in pool_status['browsers'][:5]:  # Show first 5 browsers
                logger.info(f"   • Browser #{browser['browser_id']}: {browser['active_tabs']} active tabs")
                
        except Exception as e:
            logger.error(f"❌ Browser pool verification failed: {e}")
            # Try to get basic info without detailed status
            try:
                logger.info(f"🔍 Basic browser count: {len(browser_pool.browsers)} browsers in pool")
            except Exception as e2:
                logger.error(f"❌ Even basic browser count failed: {e2}")
        
        # Test database connection
        try:
            async with get_db_connection() as conn:
                await execute_query(conn, "SELECT 1")  # PostgreSQL connection test
            logger.info("✅ Database connection verified")
        except Exception as e:
            logger.warning(f"⚠️ Database connection test failed: {e}")
        
        # Test S3 connection
        try:
            if s3_client:
                s3_client.list_objects_v2(Bucket=settings.s3_bucket_name, MaxKeys=1)
                logger.info("✅ S3 storage connection verified")
            else:
                logger.warning("⚠️ S3 client not initialized")
        except Exception as e:
            logger.warning(f"⚠️ S3 storage connection test failed: {e}")
        
        
        # Log total system capacity
        total_screenshot_capacity = settings.browser_pool_size * settings.max_tabs_per_browser
        # total_search_capacity = settings.search_max_concurrent + (settings.search_queue_size if settings.search_enable_queuing else 0)
        logger.info(f"🎯 Total System Capacity:")
        logger.info(f"   - Screenshots: {settings.screenshot_max_concurrent} concurrent ({total_screenshot_capacity} browser tabs)")
        # logger.info(f"   - Search: {settings.search_max_concurrent} concurrent + {settings.search_queue_size if settings.search_enable_queuing else 0} queue = {total_search_capacity} total")
        
        logger.info("🎉 Screenshot & Links API startup completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Critical error during startup: {e}")
        global connection_pool
        connection_pool = None
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on application shutdown."""
    try:
        logger.info("🛑 Shutting down Screenshot & Links API...")
        
        # Clean up browser pool
        await browser_pool.cleanup()
        logger.info("✅ Browser pool cleanup completed")
        
        # Close PostgreSQL connection pool
        await close_connection_pool()
        logger.info("✅ PostgreSQL connection pool closed")
        
        # Log final screenshot metrics
        final_stats = metrics.get_stats()
        logger.info(f"📊 Final screenshot metrics: {final_stats['total_requests']} total requests, "
                   f"{final_stats['success_rate_percent']:.1f}% success rate, "
                   f"{final_stats['average_response_time_seconds']:.2f}s avg response time")
        
        logger.info("👋 Screenshot & Links API shutdown completed")
        
    except Exception as e:
        logger.error(f"❌ Error during shutdown: {e}")

browser = None

class BrowserPoolManager:
    """
    Manages a pool of browser instances with tab limits.
    Optimized for high-load scenarios like 1500+ concurrent requests.
    FIXED: Async locks and parallel browser creation for true concurrency.
    """
    
    def __init__(self, max_tabs_per_browser: int = 10, max_browsers: int = 5, concurrent_browser_creation: int = 2):
        self.max_tabs_per_browser = max_tabs_per_browser
        self.max_browsers = max_browsers
        self.browsers: List[Dict[str, Any]] = []
        self.playwright = None
        self.lock = asyncio.Lock()  # ✅ FIXED: Use async lock instead of threading.Lock
        self.active_requests = 0
        self.max_concurrent_screenshots = settings.screenshot_max_concurrent
        self.semaphore = asyncio.Semaphore(self.max_concurrent_screenshots)
        self.browser_creation_semaphore = asyncio.Semaphore(concurrent_browser_creation)  # ✅ NEW: Configurable concurrent browser creation
        self.creating_browsers = set()  # ✅ NEW: Track browsers being created
        
    async def initialize(self):
        """Initialize the browser pool manager and pre-create browsers for optimal performance."""
        if self.playwright is None:
            self.playwright = await async_playwright().start()
            logger.info("🌐 Browser pool manager initialized")
            
            # Pre-create browsers for immediate parallel processing
            await self._pre_create_browsers()
    
    async def _pre_create_browsers(self):
        """Pre-create browsers during startup to avoid race conditions during concurrent requests."""
        startup_start = time.time()
        logger.info(f"🚀 Pre-creating {self.max_browsers} browsers for optimal performance...")
        logger.info(f"📊 Browser creation config: concurrent_creation={self.browser_creation_semaphore._value}, max_browsers={self.max_browsers}")
        
        # Create browsers in parallel batches to avoid overwhelming the system
        # 🚀 FORCE SINGLE BATCH: Create all browsers simultaneously for maximum speed
        batch_size = self.max_browsers  # Force single batch instead of limiting by semaphore
        logger.info(f"📦 Using batch size: {batch_size} (SINGLE BATCH MODE for maximum speed)")
        
        total_created = 0
        for batch_start in range(0, self.max_browsers, batch_size):
            batch_end = min(batch_start + batch_size, self.max_browsers)
            batch_start_time = time.time()
            logger.info(f"🔄 Creating browser batch {batch_start+1}-{batch_end}...")
            
            tasks = []
            for i in range(batch_start, batch_end):
                browser_id = i + 1
                task = self._create_new_browser_with_safe_id(browser_id)
                tasks.append(task)
            
            # Create browsers in parallel within each batch
            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Count successful creations
                successful_in_batch = 0
                for idx, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error(f"❌ Browser {batch_start + idx + 1} creation failed: {result}")
                    else:
                        successful_in_batch += 1
                        total_created += 1
                
                batch_time = time.time() - batch_start_time
                logger.info(f"✅ Batch {batch_start+1}-{batch_end} completed: {successful_in_batch}/{batch_end-batch_start} browsers in {batch_time:.2f}s")
                
            except Exception as e:
                logger.error(f"⚠️ Critical error in batch {batch_start+1}-{batch_end}: {e}")
        
        total_startup_time = time.time() - startup_start
        logger.info(f"🎉 Browser pool startup complete! Created {total_created}/{self.max_browsers} browsers in {total_startup_time:.2f}s")
        
        if total_created < self.max_browsers:
            logger.warning(f"⚠️ Only {total_created}/{self.max_browsers} browsers created successfully!")
        else:
            logger.info(f"🚀 All {total_created} browsers ready for immediate parallel processing!")
    
    async def get_browser_with_capacity(self) -> Dict[str, Any]:
        """
        Get a browser instance that has capacity for more tabs.
        FIXED: Round-robin selection with fallback to least busy browser.
        
        Returns:
            Dictionary with browser instance and metadata
        """
        async with self.lock:
            if not self.browsers:
                raise Exception("No browsers available - browser pool may not be initialized")
            
            # Initialize round-robin counter if not exists
            if not hasattr(self, '_round_robin_index'):
                self._round_robin_index = 0
            
            # Try round-robin selection first (better distribution)
            for attempt in range(len(self.browsers)):
                browser_index = (self._round_robin_index + attempt) % len(self.browsers)
                browser_info = self.browsers[browser_index]
                
                if browser_info['active_tabs'] < self.max_tabs_per_browser:
                    # Update round-robin index for next request
                    self._round_robin_index = (browser_index + 1) % len(self.browsers)
                    
                    logger.debug(f"🎯 Round-robin selected browser #{browser_info['browser_id']} "
                                f"(index {browser_index}) with {browser_info['active_tabs']}/{self.max_tabs_per_browser} tabs")
                    return browser_info
            
            # All browsers at capacity - fall back to least busy
            sorted_browsers = sorted(self.browsers, key=lambda x: x['active_tabs'])
            least_busy = sorted_browsers[0]
            
            logger.warning(f"⚠️ All browsers at capacity, using least busy browser #{least_busy['browser_id']} "
                          f"with {least_busy['active_tabs']} tabs")
            return least_busy
    
    async def _create_new_browser_with_safe_id(self, safe_id: int) -> Dict[str, Any]:
        """
        Create a new browser instance with safe ID handling.
        FIXED: No browser_id variable references in error handling.
        """
        creation_start = time.time()
        logger.debug(f"🔄 Creating browser #{safe_id}...")
        
        async with self.browser_creation_semaphore:
            try:
                browser_info = await self._create_new_browser()
                browser_info['browser_id'] = safe_id
                async with self.lock:
                    self.browsers.append(browser_info)
                
                creation_time = time.time() - creation_start
                logger.info(f"✅ Browser #{safe_id} created successfully in {creation_time:.2f}s (total browsers: {len(self.browsers)})")
                return browser_info
                
            except Exception as e:
                creation_time = time.time() - creation_start
                logger.error(f"❌ Browser #{safe_id} creation failed after {creation_time:.2f}s: {type(e).__name__}: {e}")
                # Log more details for debugging
                logger.error(f"🔍 Browser creation details: playwright={self.playwright is not None}, semaphore_value={self.browser_creation_semaphore._value}")
                raise e



    async def _create_new_browser(self) -> Dict[str, Any]:
        """Create a new browser instance optimized for high-load scenarios."""
        try:
            # Chrome launch options matching Puppeteer configuration
            browser_args = [
                # Core settings (matching Puppeteer)
                '--no-sandbox',                                # Required for some environments
                '--disable-setuid-sandbox',                    # Disable setuid sandbox
                '--disable-dev-shm-usage',                     # Disable /dev/shm usage
                '--disable-gpu',                               # Disable GPU acceleration
                
                # Lazy Loading and Preload optimizations
                '--disable-lazy-loading',                      # Disable all lazy loading
                '--disable-lazy-image-loading',                # Disable image lazy loading
                '--disable-lazy-frame-loading',                # Disable iframe lazy loading
                '--blink-settings=lazyImageLoadingDistanceThresholdPx=0',  # Force immediate image loading
                '--blink-settings=lazyFrameLoadingDistanceThresholdPx=0',  # Force immediate frame loading
                '--preload-enabled',                           # Enable resource preloading
                '--enable-preload',                            # Reinforce preloading behavior
                '--enable-features=NetworkServiceInProcess',    # Network service in main process
                '--enable-network-service-in-process',         # Keep network ops in main process
                
                # Performance optimizations
                '--disable-background-timer-throttling',       # Prevent timer throttling
                '--disable-backgrounding-occluded-windows',    # Prevent background throttling
                '--disable-renderer-backgrounding',            # Prevent renderer throttling
                '--disable-background-networking',             # Disable background network activity
                '--disable-features=IsolateOrigins,site-per-process', # Disable isolation
                '--disable-site-isolation-trials',             # Disable site isolation
                '--disable-web-security',                      # Disable web security for compatibility
                
                # Memory optimizations
                '--disable-extensions',                        # Disable extensions
                '--disable-component-extensions-with-background-pages', # Disable background extensions
                '--disable-default-apps',                      # Disable default apps
                '--disable-sync',                              # Disable sync
                '--disable-translate',                         # Disable translate
                '--disable-background-downloads',              # Disable background downloads
                '--disable-client-side-phishing-detection',    # Disable phishing detection
                '--disable-component-update',                  # Disable component updates
                '--disable-domain-reliability',                # Disable domain reliability
                '--disable-breakpad',                         # Disable crash reporting
                '--disable-ipc-flooding-protection',          # Disable IPC flooding protection
                
                # Network optimizations
                '--enable-tcp-fast-open',                     # Enable TCP fast open
                '--disable-features=VizDisplayCompositor',     # Disable compositor
                '--force-device-scale-factor=1',              # Force scale factor
                '--max-connections-per-host=6',               # Limit connections per host
                '--use-gl=swiftshader',                       # Use SwiftShader for rendering
                '--disable-blink-features=AutomationControlled', # Hide automation
                
                # Additional stability settings
                '--ignore-certificate-errors',                # Ignore SSL errors
                '--allow-running-insecure-content',           # Allow mixed content
                '--disable-http2',                           # Force HTTP/1.1
                '--disable-popup-blocking',                   # Disable popup blocker
                '--no-default-browser-check',                # Skip default browser check
                '--no-first-run',                           # Skip first run tasks
                '--metrics-recording-only',                  # Minimal metrics
                '--password-store=basic',                    # Basic password store
                '--use-mock-keychain'                       # Mock keychain
            ]
            
            browser = await self.playwright.chromium.launch(
                headless=settings.browser_headless,
                args=browser_args,
                timeout=settings.browser_launch_timeout * 1000  # Convert to milliseconds
            )
            
            browser_info = {
                'browser': browser,
                'active_tabs': 0,
                'created_at': datetime.utcnow(),
                'browser_id': 0  # Will be set by caller
            }
            
            return browser_info
            
        except Exception as e:
            logger.error(f"❌ Failed to create new browser: {e}")
            raise
    
    async def acquire_screenshot_slot(self) -> bool:
        """
        Acquire a slot for screenshot processing with rate limiting.
        
        Returns:
            True if slot acquired, False if at capacity
        """
        try:
            await asyncio.wait_for(self.semaphore.acquire(), timeout=0.001)
            # ✅ FIXED: Use simple counter increment without async lock for performance
            self.active_requests += 1
            logger.debug(f"📊 Screenshot slot acquired. Active requests: {self.active_requests}/{self.max_concurrent_screenshots}")
            return True
        except asyncio.TimeoutError:
            logger.warning(f"⚠️ Screenshot capacity reached. Active requests: {self.active_requests}/{self.max_concurrent_screenshots}")
            return False
    
    def release_screenshot_slot(self):
        """Release a screenshot processing slot."""
        try:
            # ✅ FIXED: Use simple counter decrement without async lock for performance
            self.active_requests = max(0, self.active_requests - 1)
            self.semaphore.release()
            logger.debug(f"📊 Screenshot slot released. Active requests: {self.active_requests}/{self.max_concurrent_screenshots}")
        except Exception as e:
            logger.warning(f"⚠️ Error releasing screenshot slot: {e}")

    async def get_page(self, ss_width: int = 1920, ss_height: int = 1080) -> Tuple[Any, Dict[str, Any]]:
        """
        Get a new page (tab) from an available browser.
        
        Args:
            ss_width: Screenshot width for viewport
            ss_height: Screenshot height for viewport
            
        Returns:
            Tuple of (page, browser_info)
        """
        await self.initialize()
        
        # Retry logic for browser failures
        max_retries = 3
        for attempt in range(max_retries):
            browser_info = await self.get_browser_with_capacity()
            
            try:
                # Check if browser is still connected before using it
                if not await self._is_browser_healthy(browser_info):
                    logger.warning(f"🔄 Browser #{browser_info['browser_id']} is not healthy, removing from pool")
                    await self._remove_browser_from_pool(browser_info)
                    continue
                
                # Create new context (this is like opening a new tab)
                context = await browser_info['browser'].new_context(
                    viewport={"width": ss_width, "height": ss_height},
                    bypass_csp=True,
                    ignore_https_errors=True,
                    java_script_enabled=True,
                    has_touch=False,
                    is_mobile=False,
                    extra_http_headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.9',
                        'Cache-Control': 'no-cache',
                        'Pragma': 'no-cache',
                        'Upgrade-Insecure-Requests': '1',
                        'Connection': 'keep-alive'
                    },
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    locale='en-US',
                    timezone_id='America/New_York',
                    # Force HTTP/1.1 behavior
                    offline=False,
                    http_credentials=None
                )
                page = await context.new_page()
                
                # Set timeouts for high-load scenarios
                page.set_default_navigation_timeout(settings.screenshot_timeout * 1000)
                page.set_default_timeout(settings.screenshot_timeout * 1000)
                
                # ✅ FIXED: Increment active tab count with async lock
                async with self.lock:
                    browser_info['active_tabs'] += 1
                
                # Store context reference for cleanup
                page._gs_context = context
                page._gs_browser_info = browser_info
                
                logger.debug(f"📄 Created new tab in browser #{browser_info['browser_id']} "
                            f"(tabs: {browser_info['active_tabs']}/{self.max_tabs_per_browser})")
                
                return page, browser_info
                
            except Exception as e:
                logger.warning(f"⚠️ Failed to create page in browser #{browser_info['browser_id']} (attempt {attempt + 1}/{max_retries}): {e}")
                
                # Remove the faulty browser from pool
                await self._remove_browser_from_pool(browser_info)
                
                # If this is the last attempt, raise the error
                if attempt == max_retries - 1:
                    logger.error(f"❌ Failed to create page after {max_retries} attempts: {e}")
                    raise
                
                # Wait a moment before retrying
                await asyncio.sleep(0.1)
        
        # This should never be reached, but just in case
        raise Exception("Failed to create page after maximum retries")
    
    async def _is_browser_healthy(self, browser_info: Dict[str, Any]) -> bool:
        """Check if a browser is still healthy and connected."""
        try:
            # Try to get browser contexts - this will fail if browser is closed
            contexts = browser_info['browser'].contexts
            return True
        except Exception:
            return False
    
    async def _remove_browser_from_pool(self, browser_info: Dict[str, Any]):
        """Remove a browser from the pool (when it's closed or unhealthy)."""
        try:
            # ✅ FIXED: Use async lock for removing browser from pool
            async with self.lock:
                if browser_info in self.browsers:
                    self.browsers.remove(browser_info)
                    logger.info(f"🗑️ Removed unhealthy browser #{browser_info['browser_id']} from pool")
            
            # Try to close the browser gracefully
            try:
                await browser_info['browser'].close()
            except:
                pass  # Browser might already be closed
                
        except Exception as e:
            logger.warning(f"⚠️ Error removing browser from pool: {e}")
    
    async def release_page(self, page: Any):
        """
        Release a page and its context, decrementing the tab count.
        FIXED: Enhanced cleanup with better error handling and forced tab count decrement.
        
        Args:
            page: The page to release
        """
        browser_info = None
        context = None
        tab_decremented = False
        
        try:
            # Get browser info and context references
            browser_info = getattr(page, '_gs_browser_info', None)
            context = getattr(page, '_gs_context', None)
            
            # CRITICAL: Always decrement tab count first, even if cleanup fails
            if browser_info:
                async with self.lock:
                    browser_info['active_tabs'] = max(0, browser_info['active_tabs'] - 1)
                    tab_decremented = True
                
                logger.debug(f"🗑️ Decremented tab count for browser #{browser_info['browser_id']} "
                            f"(remaining tabs: {browser_info['active_tabs']})")
            
            # Now attempt to close page and context
            if page:
                try:
                    await page.close()
                    logger.debug("📄 Page closed successfully")
                except Exception as e:
                    logger.warning(f"⚠️ Error closing page: {e}")
            
            if context:
                try:
                    await context.close()
                    logger.debug("🔧 Context closed successfully")
                except Exception as e:
                    logger.warning(f"⚠️ Error closing context: {e}")
        
        except Exception as e:
            logger.error(f"❌ Critical error in release_page: {e}")
            
            # EMERGENCY: If tab count wasn't decremented due to error, force it
            if not tab_decremented and browser_info:
                try:
                    async with self.lock:
                        browser_info['active_tabs'] = max(0, browser_info['active_tabs'] - 1)
                    logger.warning(f"🚨 EMERGENCY: Force decremented tab count for browser #{browser_info['browser_id']}")
                except Exception as emergency_e:
                    logger.error(f"💥 CRITICAL: Could not decrement tab count: {emergency_e}")
        
        finally:
            # Clear references to prevent memory leaks
            if page:
                try:
                    if hasattr(page, '_gs_browser_info'):
                        delattr(page, '_gs_browser_info')
                    if hasattr(page, '_gs_context'):
                        delattr(page, '_gs_context')
                except:
                    pass  # Ignore cleanup errors
    
    async def get_pool_status(self) -> Dict[str, Any]:
        """Get current status of the browser pool."""
        total_tabs = sum(browser_info['active_tabs'] for browser_info in self.browsers)
        max_total_tabs = len(self.browsers) * self.max_tabs_per_browser
        
        # Calculate utilization percentages
        browser_utilization = (len(self.browsers) / self.max_browsers) * 100 if self.max_browsers > 0 else 0
        tab_utilization = (total_tabs / max_total_tabs) * 100 if max_total_tabs > 0 else 0
        request_utilization = (self.active_requests / self.max_concurrent_screenshots) * 100 if self.max_concurrent_screenshots > 0 else 0
        
        return {
            "total_browsers": len(self.browsers),
            "max_browsers": self.max_browsers,
            "browser_utilization_percent": round(browser_utilization, 2),
            "total_active_tabs": total_tabs,
            "max_possible_tabs": max_total_tabs,
            "tab_utilization_percent": round(tab_utilization, 2),
            "max_tabs_per_browser": self.max_tabs_per_browser,
            "active_screenshot_requests": self.active_requests,
            "max_concurrent_screenshots": self.max_concurrent_screenshots,
            "request_utilization_percent": round(request_utilization, 2),
            "browsers": [
                {
                    "browser_id": info['browser_id'],
                    "active_tabs": info['active_tabs'],
                    "tab_utilization_percent": round((info['active_tabs'] / self.max_tabs_per_browser) * 100, 2),
                    "created_at": info['created_at'].isoformat()
                }
                for info in self.browsers
            ]
        }
    
    async def cleanup(self):
        """Clean up all browser instances."""
        logger.info("🧹 Cleaning up browser pool...")
        
        for browser_info in self.browsers:
            try:
                await browser_info['browser'].close()
            except Exception as e:
                logger.warning(f"Error closing browser: {e}")
        
        if self.playwright:
            await self.playwright.stop()
        
        self.browsers.clear()
        logger.info("✅ Browser pool cleanup completed")

# Global browser pool manager - OPTIMIZED (instantiated after class definition)
browser_pool = BrowserPoolManager(
    max_tabs_per_browser=settings.max_tabs_per_browser,
    max_browsers=settings.browser_pool_size,
    concurrent_browser_creation=settings.browser_launch_concurrent
)

# Global performance metrics
class PerformanceMetrics:
    """Track system performance metrics for monitoring."""
    
    def __init__(self):
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.total_processing_time = 0.0
        self.start_time = datetime.utcnow()
        self.lock = threading.Lock()
    
    def increment_total_requests(self):
        with self.lock:
            self.total_requests += 1
    
    def increment_successful_requests(self, processing_time: float):
        with self.lock:
            self.successful_requests += 1
            self.total_processing_time += processing_time
    
    def increment_failed_requests(self):
        with self.lock:
            self.failed_requests += 1
    
    def increment_cache_hits(self):
        with self.lock:
            self.cache_hits += 1
    
    def increment_cache_misses(self):
        with self.lock:
            self.cache_misses += 1
    
    def get_stats(self) -> Dict[str, Any]:
        with self.lock:
            uptime = (datetime.utcnow() - self.start_time).total_seconds()
            avg_response_time = (
                self.total_processing_time / self.successful_requests 
                if self.successful_requests > 0 else 0
            )
            success_rate = (
                (self.successful_requests / self.total_requests * 100) 
                if self.total_requests > 0 else 0
            )
            cache_hit_rate = (
                (self.cache_hits / (self.cache_hits + self.cache_misses) * 100)
                if (self.cache_hits + self.cache_misses) > 0 else 0
            )
            
            return {
                "uptime_seconds": round(uptime, 2),
                "total_requests": self.total_requests,
                "successful_requests": self.successful_requests,
                "failed_requests": self.failed_requests,
                "success_rate_percent": round(success_rate, 2),
                "cache_hits": self.cache_hits,
                "cache_misses": self.cache_misses,
                "cache_hit_rate_percent": round(cache_hit_rate, 2),
                "average_response_time_seconds": round(avg_response_time, 2),
                "requests_per_minute": round((self.total_requests / uptime * 60) if uptime > 0 else 0, 2)
            }

# Global metrics instance
metrics = PerformanceMetrics()

@app.get("/health", response_model=Dict[str, Any])
async def health_check() -> Dict[str, Any]:
    """
    Health check endpoint for monitoring.
    
    Returns:
        Dictionary with service health status
        
    Raises:
        HTTPException: If service is unhealthy
    """
    try:
        # Check database connection
        try:
            if connection_pool is None:
                db_status = "not_configured"
            else:
                async with get_db_connection() as conn:
                    await execute_query(conn, "SELECT 1")  # PostgreSQL connection test
                db_status = "connected"
        except Exception as e:
            logger.debug(f"Database health check failed: {e}")
            db_status = "disconnected"
        
        # Check browser pool status
        browser_pool_status = await browser_pool.get_pool_status()
        
        # Check S3 connection
        try:
            if s3_client:
                s3_client.list_objects_v2(Bucket=settings.s3_bucket_name, MaxKeys=1)
                s3_status = "connected"
            else:
                s3_status = "not_configured"
        except Exception as e:
            s3_status = "disconnected"
        
        # Get performance metrics
        performance_stats = metrics.get_stats()
        
        logger.info("Health check passed")
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "configuration": {
                "max_concurrent_screenshots": settings.screenshot_max_concurrent,
                "browser_pool_size": settings.browser_pool_size,
                "max_tabs_per_browser": settings.max_tabs_per_browser,
                "total_capacity": settings.browser_pool_size * settings.max_tabs_per_browser
            },
            "services": {
                "database": db_status,
                "browser_pool": browser_pool_status,
                "s3_storage": s3_status
            },
            "performance_metrics": performance_stats
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(
            status_code=503, 
            detail=f"Service unhealthy: {str(e)}"
        )


# Initialize S3 client for Contabo storage with optimized concurrency settings
try:
    # 🚀 S3 CONCURRENCY OPTIMIZATION: Configure for better parallel uploads
    s3_config = Config(
        # Connection pool settings for concurrent uploads
        max_pool_connections=50,  # Increase from default 10 to support more concurrent uploads
        
        # Retry configuration for reliability under high load
        retries={
            'max_attempts': 3,
            'mode': 'adaptive'  # Adaptive retry mode for better performance
        },
        
        # Timeout settings optimized for slice uploads
        connect_timeout=10,  # Fast connection establishment
        read_timeout=30,     # Reasonable read timeout for image uploads
        
        # Regional configuration
        region_name='us-east-1'  # Default region for better performance
    )
    
    s3_client = boto3.client(
        's3',
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        config=s3_config  # Apply optimized configuration
    )
    logger.info("✅ S3 client initialized with optimized concurrency settings (max_pool_connections=50)")
except Exception as e:
    logger.warning(f"⚠️ S3 client initialization failed: {e}")
    s3_client = None
    logger.info("💡 App will start without S3 functionality. Configure AWS credentials in .env for full functionality.")



async def setup_stealth_mode(page, url):
    """
    Configure stealth mode settings for the page to bypass bot detection.
    """
    # Set common headers
    await page.set_extra_http_headers({
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Cache-Control': 'no-cache',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'
    })

    # Emulate a real browser environment
    await page.evaluate("""
        Object.defineProperty(navigator, 'webdriver', {
            get: () => false
        });
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5]
        });
        window.chrome = {
            runtime: {}
        };
    """)

    # Add random mouse movements for sites with bot detection
    await page.evaluate("""
        (() => {
            const events = ['mousemove', 'mousedown', 'mouseup', 'click'];
            events.forEach(event => {
                window.addEventListener(event, e => {
                    if (!e.isTrusted) {
                        Object.defineProperty(e, 'isTrusted', {get: () => true});
                    }
                });
            });
        })();
    """)

async def take_screenshot(page, url, output_path, full_page, ss_width, ss_height):
    """
    Take a screenshot of a webpage and process it into slices.
    
    Args:
        page: Playwright page instance
        url: URL to screenshot
        output_path: Output path for the screenshot
        full_page: Whether to take full page screenshot
        ss_width: Screenshot width
        ss_height: Screenshot height
        
    Returns:
        Tuple of (links, image_slices)
    """
    logger.debug(f"Starting screenshot process for URL: {url}")
    
    # SITE-SPECIFIC AGGRESSIVE TIMEOUT STRATEGY
    # Problematic sites get even faster timeouts to fail quickly
    problematic_domains = ['bestbuy.com', 'amazon.com', 'walmart.com', 'target.com']
    is_problematic_site = any(domain in url.lower() for domain in problematic_domains)
    
    # Apply stealth mode for problematic sites
    if is_problematic_site:
        await setup_stealth_mode(page, url)
    
    if is_problematic_site:
        logger.warning(f"🚨 PROBLEMATIC SITE DETECTED: {url} - Using aggressive timeouts")
        # Ultra-aggressive timeouts for problematic sites
        strategies = [
            {"wait_until": "commit", "timeout": 20000, "description": "Initial commit (20s)"},
            {"wait_until": "domcontentloaded", "timeout": 40000, "description": "DOM ready (40s)"},
            {"wait_until": "load", "timeout": 60000, "description": "Full load (60s)"}
        ]
    else:
        # Standard aggressive timeouts for normal sites
        strategies = [
            # {"wait_until": "commit", "timeout": 20000, "description": "Initial commit (20s)"},
            {"wait_until": "domcontentloaded", "timeout": 40000, "description": "DOM ready (40s)"},
            {"wait_until": "load", "timeout": 60000, "description": "Full load (60s)"}
        ]
    
    # Universal navigation strategy that works for all websites
    navigation_successful = False
    last_error = None
    
    # Progressive fallback strategies - OPTIMIZED for speed during testing
    # strategies = [
    #     # {"wait_until": "commit", "timeout": 15000, "description": "Fast commit"},
    #     # {"wait_until": "domcontentloaded", "timeout": 25000, "description": "DOM ready"},
    #     {"wait_until": "load", "timeout": 45000, "description": "Full load"},
    #     {"wait_until": "networkidle", "timeout": 60000, "description": "Network idle"}
    # ]
    
    for i, strategy in enumerate(strategies):
        start_time = time.time()
        try:
            logger.debug(f"Navigation attempt {i+1}/{len(strategies)}: {strategy['description']} (timeout: {strategy['timeout']}ms)")
            
            await page.goto(url, wait_until=strategy["wait_until"], timeout=strategy["timeout"])
            # await page.goto(url, wait_until=strategy["wait_until"])
            
            # Universal post-load wait for dynamic content
            logger.debug("Allowing time for dynamic content to load")
            await asyncio.sleep(0.1)  # Reduced from 1.0s for performance testing
            
            # Try to wait for network idle as a bonus, but don't fail if it times out
            try:
                await page.wait_for_load_state("networkidle", timeout=30000)  # Increased to 30s for large pages
                logger.debug("Network idle achieved")
            except Exception as e:
                logger.debug(f"Network idle timeout - continuing anyway: {e}")
                # Don't fail - network idle is optional
            
            navigation_successful = True
            logger.info(f"✅ Page loaded successfully using strategy: {strategy['description']} ({strategy['timeout']}ms)")
            break
            
        except Exception as e:
            last_error = e
            error_msg = str(e).lower()
            logger.warning(f"❌ Navigation attempt {i+1} failed ({strategy['description']}): {e}")
            
            # Universal error recovery strategies
            if any(keyword in error_msg for keyword in ['http2_protocol_error', 'net::err_http2_protocol_error', 'protocol_error']):
                logger.debug("HTTP/2 protocol error detected - should be prevented by browser config")
                
            elif any(keyword in error_msg for keyword in ['timeout', 'net::err_timed_out']):
                logger.debug("Timeout detected - will try faster strategy next")
                
            elif any(keyword in error_msg for keyword in ['net::err_connection_refused', 'net::err_name_not_resolved']):
                logger.error(f"Network connectivity issue with {url}")
                break  # No point retrying for these errors
                
            # Brief pause before next attempt
            if i < len(strategies) - 1:
                await asyncio.sleep(0.5)
                continue
        logger.info(f"✅ Screenshot completed for URL: {url} in {time.time() - start_time:.2f}s")
    
    if not navigation_successful:
        
        logger.error(f"🚫 All navigation strategies failed for {url}. Last error: {last_error}")
        raise Exception(f"Failed to load page after {len(strategies)} attempts. Last error: {last_error}")
    
    logger.debug("✅ Page navigation completed successfully")

    # Get page dimensions to determine screenshot strategy
    dimensions = await page.evaluate('''() => {
        return {
            width: document.documentElement.scrollWidth,
            height: document.documentElement.scrollHeight
        }
    }''')
    
    logger.debug(f"Page dimensions: {dimensions['width']}x{dimensions['height']}")

    # Handle large pages differently to avoid browser limitations
    if full_page and (dimensions['width'] > 32767 or dimensions['height'] > 32767):
        logger.info("Using large screenshot strategy for oversized page")
        slices = await take_large_screenshot(page, dimensions, s3_client, ss_width, ss_height)
        logger.debug("Large screenshot processing completed")
    else:
        logger.debug("Taking standard full page screenshot")
        screenshot_bytes = await page.screenshot(
            full_page=True, 
            type='png', 
            timeout=settings.screenshot_timeout * 1000
        )
        
        logger.debug("Processing screenshot into slices")
        slices = await slice_and_stretch_image(screenshot_bytes, s3_client, ss_width, ss_height)
        logger.debug("Screenshot slicing completed")

    logger.info(f"Screenshot process completed with {len(slices)} slices generated")
    return slices  # Only return slices (links functionality removed)

async def slice_and_stretch_image(image_path, s3_client, ss_width, ss_height):
    """
    Slice and upload image to S3 storage.
    Optimized for high-load scenarios with better error handling and logging.

    This implementation uploads slices concurrently using a thread pool and asyncio,
    which is much better for concurrency than sequential uploads. However, the actual
    concurrency is limited by the S3 client, network, and the max_workers setting.
    """
    try:
        nparr = np.frombuffer(image_path, np.uint8)
        # Decode image data to OpenCV format
        image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if image is None:
            logger.error("Failed to decode image data")
            return []

        logger.debug("Converted ByteImage to OpenCV Image format")

        height, width, _ = image.shape
        slice_width = ss_width
        slice_height = ss_height

        temp_image_paths = []
        slice_buffers = []
        slice_count = 0
        
        # Optimize PNG compression for high-load scenarios
        png_compression_params = [cv2.IMWRITE_PNG_COMPRESSION, 3]  # Reduced from 6 for maximum speed

        # Prepare all slices and buffers first
        for y in range(0, height, slice_height):
            for x in range(0, width, slice_width):
                try:
                    unique_id = uuid.uuid4().hex
                    end_x = min(x + slice_width, width)
                    end_y = min(y + slice_height, height)
                    temp_image_path = f'{unique_id}_{end_x}_{end_y}.png'
                    
                    # Crop the image slice
                    cropped_slice = image[y:end_y, x:end_x]

                    # Resize if necessary
                    if cropped_slice.shape[1] != slice_width:
                        cropped_slice = cv2.resize(
                            cropped_slice, 
                            (slice_width, cropped_slice.shape[0]), 
                            interpolation=cv2.INTER_LINEAR
                        )

                    # Encode image to PNG with optimized compression
                    is_success, buffer = cv2.imencode('.png', cropped_slice, png_compression_params)
                    
                    if is_success:
                        image_data = buffer.tobytes()
                        image_file_obj = io.BytesIO(image_data)
                        slice_buffers.append((image_file_obj, temp_image_path, slice_count))
                        slice_count += 1
                    else:
                        logger.warning(f"Failed to encode slice {slice_count} to PNG")
                        slice_count += 1
                        continue
                except Exception as e:
                    logger.error(f"Error processing slice at position ({x}, {y}): {e}")
                    slice_count += 1
                    continue

        # Define upload function for thread pool
        def upload_slice(image_file_obj, temp_image_path, slice_idx):
            try:
                logger.debug(f"Uploading slice {slice_idx + 1} to S3")
                s3_client.upload_fileobj(
                    image_file_obj, 
                    settings.s3_bucket_name, 
                    f'Images/{os.path.basename(temp_image_path)}', 
                    ExtraArgs={
                        'ACL': 'public-read', 
                        'ContentType': 'image/png'
                    }
                )
                remote_path = f'Images/{os.path.basename(temp_image_path)}'
                return (True, remote_path)
            except (NoCredentialsError, PartialCredentialsError) as e:
                logger.error(f"S3 credentials error uploading slice {slice_idx}: {e}")
                return (False, None)
            except Exception as e:
                logger.error(f"Error uploading slice {slice_idx} to S3: {e}")
                return (False, None)

        # Upload slices concurrently using ThreadPoolExecutor and asyncio
        from concurrent.futures import ThreadPoolExecutor

        # 🚀 IMPROVED S3 CONCURRENCY: Higher parallel uploads
        max_workers = min(32, len(slice_buffers))  # Increased from 16 to 32 for better S3 concurrency
        if slice_buffers:
            logger.info(f"Uploading {len(slice_buffers)} slices to S3 concurrently (max_workers={max_workers})")
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                upload_futures = [
                    loop.run_in_executor(
                        executor, upload_slice, image_file_obj, temp_image_path, idx
                    )
                    for idx, (image_file_obj, temp_image_path, idx) in enumerate(slice_buffers)
                ]
                upload_results = await asyncio.gather(*upload_futures, return_exceptions=True)
                
                # Process results with better error handling
                successful_uploads = 0
                for idx, result in enumerate(upload_results):
                    if isinstance(result, Exception):
                        logger.error(f"Upload exception for slice {idx}: {result}")
                    elif result and len(result) == 2:
                        success, remote_path = result
                        if success and remote_path:
                            temp_image_paths.append(remote_path)
                            successful_uploads += 1
                        else:
                            logger.warning(f"Upload failed for slice {idx}")
                    else:
                        logger.warning(f"Invalid result format for slice {idx}: {result}")
                
                logger.info(f"S3 upload summary: {successful_uploads}/{len(slice_buffers)} slices uploaded successfully")

        logger.info(f"Successfully uploaded {len(temp_image_paths)} image slices to S3")
        return temp_image_paths
        
    except Exception as e:
        logger.error(f"Critical error in slice_and_stretch_image: {e}")
        return []


async def take_large_screenshot(page, dimensions, s3_client, ss_width, ss_height):
    """
    Take large screenshot by scrolling and capturing viewport sections.
    Optimized for high-load scenarios with better error handling and logging.
    """
    try:
        x = 0
        viewport_height = ss_height
        temp_image_paths = []

        # Get total scrollable height
        page_height = dimensions['height']
        logger.debug(f"Taking large screenshot: {page_height}px height, {viewport_height}px viewport")

        screenshot_count = 0
        for y in range(0, page_height, viewport_height):
            try:
                # Scroll to position
                await page.evaluate(f"window.scrollTo(0, {y})")
                
                # Wait a brief moment for content to load (optimized for high-load)
                await asyncio.sleep(0.1)  # Reduced from potentially longer waits
                
                unique_id = uuid.uuid4().hex
                temp_image_path = f'{unique_id}_{x}_{y}.png'
                
                # Take screenshot with timeout
                screenshot_bytes = await page.screenshot(
                    timeout=settings.screenshot_timeout * 1000
                )
                
                x = x + ss_width
                image_file_obj = io.BytesIO(screenshot_bytes)

                # Upload to S3
                try:
                    logger.debug(f"Uploading large screenshot section {screenshot_count + 1}")
                    s3_client.upload_fileobj(
                        image_file_obj, 
                        settings.s3_bucket_name, 
                        f'Images/{os.path.basename(temp_image_path)}', 
                        ExtraArgs={
                            'ACL': 'public-read', 
                            'ContentType': 'image/png'
                        }
                    )
                    
                    remote_path = f'Images/{os.path.basename(temp_image_path)}'
                    temp_image_paths.append(remote_path)
                    screenshot_count += 1
                    logger.debug(f"Successfully uploaded section: {remote_path}")
                    
                except (NoCredentialsError, PartialCredentialsError) as e:
                    logger.error(f"S3 credentials error uploading section {screenshot_count}: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Error uploading section {screenshot_count} to S3: {e}")
                    continue
                    
            except Exception as e:
                logger.error(f"Error capturing screenshot section at y={y}: {e}")
                continue

        logger.info(f"Successfully captured and uploaded {len(temp_image_paths)} large screenshot sections")
        return temp_image_paths
        
    except Exception as e:
        logger.error(f"Critical error in take_large_screenshot: {e}")
        return []




@app.post("/screenshot/", response_model=Dict[str, Any])
async def create_screenshot(request: ScreenshotRequest) -> Dict[str, Any]:
    """
    Create a screenshot of a webpage using optimized browser pool management.
    ✅ OPTIMIZED: Implements intelligent queuing, parallel browser creation, and comprehensive metrics.
    
    Args:
        request: ScreenshotRequest with URL and configuration
        
    Returns:
        Dictionary with screenshot results and metadata
        
    Raises:
        HTTPException: If screenshot operation fails or rate limit exceeded
    """
    
    start_time = time.time()
    
    # ✅ STEP 1: Try to acquire slot immediately (non-blocking)
    if await screenshot_queue.acquire_screenshot_slot():
        # Got slot immediately - process the request
        try:
            return await _process_screenshot_request(request, start_time, False)
        finally:
            screenshot_queue.release_screenshot_slot()
    
    # ✅ STEP 2: No immediate slot available - check if queuing is enabled
    if not settings.screenshot_enable_queuing:
        # Queuing disabled - reject with 429
        with screenshot_queue.stats_lock:
            screenshot_queue.stats['queue_rejections'] += 1
        
        logger.warning(f"📷 Screenshot capacity reached, queuing disabled for URL: {request.url}")
        raise HTTPException(
            status_code=429,
            detail=f"Server at capacity. Max concurrent screenshots: {settings.screenshot_max_concurrent}. Queuing disabled. Please retry later."
        )
    
    # ✅ STEP 3: Try to queue the request (with timeout)
    if await screenshot_queue.acquire_screenshot_slot_with_wait(settings.screenshot_queue_timeout):
        # Got slot after waiting - process the request
        try:
            return await _process_screenshot_request(request, start_time, True)
        finally:
            screenshot_queue.release_screenshot_slot()
    else:
        # Timeout while waiting in queue
        with screenshot_queue.stats_lock:
            screenshot_queue.stats['timeout_errors'] += 1
        
        logger.warning(f"📷 Screenshot queue timeout after {settings.screenshot_queue_timeout}s for URL: {request.url}")
        raise HTTPException(
            status_code=408,
            detail=f"Request timeout. Queue wait time exceeded {settings.screenshot_queue_timeout} seconds. Please retry later."
        )

async def _process_screenshot_request(request: ScreenshotRequest, start_time: float, was_queued: bool) -> Dict[str, Any]:
    """
    Process the actual screenshot request with optimized browser management.
    ✅ OPTIMIZED: Includes caching, retry logic, URL-level locking, and comprehensive error handling.
    """
    
    page = None
    cache_hit = False
    browser_created = False
    browser_info = {'browser_id': 0}  # Initialize with default values
    
    try:
        # ✅ STEP 1: Check cache first
        respond = await check_existing_entry(str(request.url))
        if respond is not None:
            cache_hit = True
            processing_time = time.time() - start_time
            logger.info(f"📷 Screenshot cache HIT for URL: {request.url} (returned in {processing_time:.2f}s)")
            
            # Record cache hit metrics
            await screenshot_queue.record_request_metrics(processing_time, cache_hit, True, browser_created)
            
            return {
                "status_code": 200,
                "message": "Screenshot retrieved from cache",
                "base_path": settings.base_path,
                "slices": respond['slices'],  # Already parsed in check_existing_entry
                "processing_time": processing_time,
                "browser_id": 0,  # Cache hit
                "queue_position": screenshot_queue.active_requests,
                "cache_hit": True,
                "was_queued": was_queued
            }
        
        # ✅ STEP 2: Cache miss - create new screenshot
        cache_hit = False
        logger.info(f"📷 Creating new screenshot for URL: {request.url}")
        
        # ✅ STEP 3: Get a page from the optimized browser pool with retry logic
        max_browser_retries = 3
        for browser_attempt in range(max_browser_retries):
            try:
                page, browser_info = await browser_pool.get_page(
                    ss_width=request.ss_width, 
                    ss_height=request.ss_height
                )
                break  # Successfully got page
            except Exception as e:
                logger.warning(f"⚠️ Browser pool attempt {browser_attempt + 1}/{max_browser_retries} failed: {e}")
                if browser_attempt == max_browser_retries - 1:
                    logger.error(f"❌ Failed to get browser page after {max_browser_retries} attempts")
                    raise HTTPException(
                        status_code=503,
                        detail=f"Service temporarily unavailable. Browser pool exhausted. Please retry later."
                    )
                await asyncio.sleep(0.5)  # Brief pause before retry
        
        # Check if this was a newly created browser
        browser_created = browser_info.get('browser_id', 0) > len(browser_pool.browsers) - 2
        
        logger.debug(f"📄 Using browser #{browser_info['browser_id']} for screenshot (created: {browser_created})")
        
        output_path = request.output_base_path + "screenshot.png"
        
        # ✅ STEP 3: Take the screenshot with optimized timeout and retry logic
        links, slices = None, None
        last_error = None
        
        for attempt in range(settings.screenshot_retry_attempts + 1):
            try:
                # Use reduced timeout for faster processing
                page.set_default_timeout(settings.screenshot_timeout * 1000)
                
                # GLOBAL REQUEST TIMEOUT: Based on single request timing
                global_timeout = 90  # 90 seconds max for entire request (57s + buffer)
                slices = await asyncio.wait_for(
                    take_screenshot(
                        page, 
                        str(request.url), 
                        output_path, 
                        request.full_page, 
                        request.ss_width, 
                        request.ss_height
                    ),
                    timeout=global_timeout
                )
                break  # Success - exit retry loop
                
            except asyncio.TimeoutError:
                last_error = f"Global timeout after {global_timeout}s"
                logger.error(f"🚫 GLOBAL TIMEOUT: Request exceeded {global_timeout}s for URL: {request.url}")
                if attempt == settings.screenshot_retry_attempts:
                    raise HTTPException(
                        status_code=408,
                        detail=f"Request timeout. Processing exceeded {global_timeout} seconds. Site may be too slow."
                    )
                    
            except Exception as e:
                last_error = str(e)
                logger.warning(f"📷 Screenshot attempt {attempt + 1} failed: {e}")
                if attempt == settings.screenshot_retry_attempts:
                    # Final attempt failed
                    raise HTTPException(
                        status_code=500,
                        detail=f"Screenshot processing failed: {str(e)[:100]}"
                    )
                await asyncio.sleep(0.5)  # Brief pause before retry
        
        # ✅ STEP 4: Store results in database (background task for better performance)
        if slices:
            asyncio.create_task(store_slices_in_db(str(request.url), output_path, slices))
        
        processing_time = time.time() - start_time
        
        # ✅ STEP 5: Record metrics and return success
        await screenshot_queue.record_request_metrics(processing_time, cache_hit, True, browser_created)
        
        return {
            "status_code": 200,
            "message": "Screenshot created successfully",
            "base_path": settings.base_path,
            "slices": slices,
            "processing_time": processing_time,
            "browser_id": browser_info.get('browser_id', 0),
            "queue_position": screenshot_queue.active_requests,
            "cache_hit": cache_hit,
            "was_queued": was_queued
        }
        
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        # ✅ STEP 6: Comprehensive error handling and cleanup
        logger.error(f"❌ Unexpected error in screenshot processing for URL {request.url}: {e}")
        
        # Record failure metrics
        processing_time = time.time() - start_time
        await screenshot_queue.record_request_metrics(processing_time, cache_hit, False, browser_created)
        
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)[:100]}"
        )
    finally:
        # ✅ STEP 7: Always cleanup resources
        if page:
            try:
                await browser_pool.release_page(page)
                logger.debug(f"📄 Released page from browser #{browser_info.get('browser_id', 0)}")
            except Exception as e:
                logger.warning(f"⚠️ Error releasing page: {e}")

@app.get("/browser-pool-status", response_model=Dict[str, Any])
async def get_browser_pool_status() -> Dict[str, Any]:
    """
    Get current status of the browser pool for monitoring.
    
    Returns:
        Dictionary with browser pool status and statistics
    """
    try:
        status = await browser_pool.get_pool_status()
        logger.debug("Browser pool status requested")
        
        return {
            "status_code": 200,
            "message": "Browser pool status retrieved successfully",
            **status
        }
        
    except Exception as e:
        logger.error(f"Error getting browser pool status: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve browser pool status"
        )

@app.get("/metrics", response_model=Dict[str, Any])
async def get_performance_metrics() -> Dict[str, Any]:
    """
    Get detailed performance metrics for monitoring high-load scenarios.
    
    Returns:
        Dictionary with comprehensive performance statistics
    """
    try:
        performance_stats = metrics.get_stats()
        browser_pool_status = await browser_pool.get_pool_status()
        
        logger.debug("Performance metrics requested")
        
        return {
            "status_code": 200,
            "message": "Performance metrics retrieved successfully",
            "timestamp": datetime.utcnow().isoformat(),
            "system_performance": performance_stats,
            "browser_pool_status": browser_pool_status,
            "configuration": {
                "max_concurrent_screenshots": settings.screenshot_max_concurrent,
                "browser_pool_size": settings.browser_pool_size,
                "max_tabs_per_browser": settings.max_tabs_per_browser,
                "total_theoretical_capacity": settings.browser_pool_size * settings.max_tabs_per_browser,
                "screenshot_timeout": settings.screenshot_timeout
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting performance metrics: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve performance metrics"
        )






async def store_slices_in_db(url: str, output_path: str, slices: List[str]):
    """
    Store screenshot slices in database (links functionality removed).
    ⚡ OPTIMIZED: Non-blocking with short timeout for high concurrency.

    Args:
        url: The URL that was screenshotted
        output_path: Path where screenshot was saved
        slices: List of slice file paths

    Note:
        Uses short timeout to prevent blocking. If database is slow, storage is skipped.
    """
    # ⚡ PERFORMANCE: Skip database storage if caching disabled
    if not getattr(settings, 'db_enable_caching', True):
        return

    try:
        # ⏰ CONFIGURABLE TIMEOUT: Prevent blocking concurrent requests
        import asyncio
        storage_timeout = getattr(settings, 'db_storage_timeout', 1.0)
        async with asyncio.timeout(storage_timeout):
            async with get_db_connection() as conn:
            # Check if URL already exists
            check_sql = "SELECT id FROM screenshots WHERE url = %s LIMIT 1"
            existing = await execute_query(conn, check_sql, (url,))
            
            if existing:
                # Update existing record
                sql = "UPDATE screenshots SET slices = %s, output_path = %s, timestamp_column = CURRENT_TIMESTAMP WHERE url = %s"
                await execute_update(conn, sql, (json.dumps(slices), output_path, url))
                logger.info(f"✅ Updated screenshot data in database for URL: {url}")
            else:
                # Insert new record
                sql = "INSERT INTO screenshots (url, output_path, slices) VALUES (%s, %s, %s)"
                await execute_update(conn, sql, (url, output_path, json.dumps(slices)))
                logger.info(f"✅ Successfully stored screenshot data in database for URL: {url}")
            
            logger.debug(f"Generated {len(slices)} slices")

    except asyncio.TimeoutError:
        logger.debug(f"⚡ Database storage timeout (>{storage_timeout}s) for URL: {url} - storage skipped for performance")
        # Don't raise exception - storage is not critical to main functionality

    except Exception as e:
        logger.warning(f"⚠️ Could not store screenshot data in database: {e}")
        # Don't raise exception - this is not critical to the main functionality

async def check_existing_entry(url: str) -> Optional[Dict[str, Any]]:
    """
    Check if a screenshot already exists for the given URL.
    ⚡ OPTIMIZED: Non-blocking with short timeout for high concurrency.

    Args:
        url: URL to check for existing screenshot

    Returns:
        Dictionary with existing screenshot data or None if not found

    Note:
        Uses short timeout to prevent blocking concurrent requests.
        If database is slow/unavailable, returns None (cache miss).
    """
    # ⚡ PERFORMANCE: Skip database caching if disabled
    if not getattr(settings, 'db_enable_caching', True):
        return None

    try:
        # ⏰ CONFIGURABLE TIMEOUT: Prevent blocking concurrent requests
        import asyncio
        cache_timeout = getattr(settings, 'db_cache_timeout', 0.5)
        async with asyncio.timeout(cache_timeout):
            async with get_db_connection() as conn:
                logger.debug(f"🔍 Checking for existing screenshot entry for URL: {url}")

                sql = "SELECT slices FROM screenshots WHERE url = %s ORDER BY timestamp_column DESC LIMIT 1"
                result = await execute_query(conn, sql, (url,))

                if result:
                    # PostgreSQL returns asyncpg.Record objects, access by column name
                    slices_data = result[0]['slices']

                    # Handle JSONB - it might already be parsed or might be a string
                    if isinstance(slices_data, str):
                        slices_list = json.loads(slices_data)
                    else:
                        slices_list = slices_data  # Already parsed by asyncpg
                
                logger.info(f"✅ Found existing screenshot in cache for URL: {url}")
                return {
                    "status_code": 200,
                    "message": "Cached screenshot found",
                    "base_path": settings.base_path,
                    "slices": slices_list
                }
                
                logger.debug(f"No existing screenshot found for URL: {url}")
                return None

    except asyncio.TimeoutError:
        logger.debug(f"⚡ Cache check timeout (>{cache_timeout}s) for URL: {url} - proceeding with screenshot creation")
        return None  # Fast timeout - treat as cache miss

    except Exception as e:
        logger.debug(f"Cache check failed (DB not available or error): {e}")
        return None  # Return None instead of raising exception


browser = None

# =====================================================================
# SCREENSHOT API MONITORING ENDPOINTS  
# =====================================================================

@app.get("/screenshot-status", response_model=Dict[str, Any])
async def get_screenshot_status() -> Dict[str, Any]:
    """
    Get current status of the screenshot API queue and processing.
    ✅ NEW: Comprehensive screenshot API monitoring endpoint.
    
    Returns:
        Dictionary with screenshot queue status and statistics
    """
    try:
        screenshot_status = await screenshot_queue.get_status()
        browser_status = await browser_pool.get_pool_status()
        
        logger.debug("📷 Screenshot API status requested")
        
        return {
            "status_code": 200,
            "message": "Screenshot API status retrieved successfully",
            "timestamp": datetime.utcnow().isoformat(),
            "screenshot_queue": screenshot_status,
            "browser_pool": browser_status,
            "configuration": {
                "max_concurrent_screenshots": settings.screenshot_max_concurrent,
                "queue_enabled": settings.screenshot_enable_queuing,
                "queue_size_limit": settings.screenshot_queue_size,
                "queue_timeout_seconds": settings.screenshot_queue_timeout,
                "retry_attempts": settings.screenshot_retry_attempts,
                "screenshot_timeout_seconds": settings.screenshot_timeout,
                "browser_pool_size": settings.browser_pool_size,
                "max_tabs_per_browser": settings.max_tabs_per_browser,
                "concurrent_browser_creation": settings.browser_launch_concurrent
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Error getting screenshot API status: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve screenshot API status"
        )

@app.get("/screenshot-health", response_model=Dict[str, Any])
async def get_screenshot_health() -> Dict[str, Any]:
    """
    Health check endpoint for screenshot API with detailed diagnostics.
    ✅ NEW: Comprehensive health check for screenshot operations.
    
    Returns:
        Dictionary with health status and diagnostics
    """
    try:
        health_status = {
            "service": "healthy",
            "browser_pool": "unknown",
            "queue": "unknown",
            "issues": []
        }
        
        # Check browser pool health
        try:
            browser_status = await browser_pool.get_pool_status()
            if browser_status["total_browsers"] > 0:
                health_status["browser_pool"] = "healthy"
            else:
                health_status["browser_pool"] = "degraded"
                health_status["issues"].append("No active browsers in pool")
        except Exception as e:
            health_status["browser_pool"] = "unhealthy"
            health_status["issues"].append(f"Browser pool error: {str(e)}")
        
        # Check screenshot queue health
        try:
            queue_status = await screenshot_queue.get_status()
            utilization = queue_status.get("queue_utilization_percent", 0)
            if utilization < 90:
                health_status["queue"] = "healthy"
            elif utilization < 100:
                health_status["queue"] = "degraded"
                health_status["issues"].append(f"High queue utilization: {utilization}%")
            else:
                health_status["queue"] = "critical"
                health_status["issues"].append("Queue at full capacity")
        except Exception as e:
            health_status["queue"] = "unhealthy"
            health_status["issues"].append(f"Queue health check error: {str(e)}")
        
        # Determine overall health
        if health_status["browser_pool"] == "unhealthy" or health_status["queue"] == "unhealthy":
            health_status["service"] = "unhealthy"
        elif health_status["browser_pool"] == "degraded" or health_status["queue"] == "degraded":
            health_status["service"] = "degraded"
        elif health_status["browser_pool"] == "critical" or health_status["queue"] == "critical":
            health_status["service"] = "critical"
        
        status_code = 200
        if health_status["service"] == "degraded":
            status_code = 200  # Still functional
        elif health_status["service"] in ["critical", "unhealthy"]:
            status_code = 503  # Service unavailable
        
        return {
            "status_code": status_code,
            "message": f"Screenshot API health: {health_status['service']}",
            "timestamp": datetime.utcnow().isoformat(),
            "health": health_status
        }
        
    except Exception as e:
        logger.error(f"❌ Error during screenshot health check: {e}")
        return {
            "status_code": 503,
            "message": "Screenshot API health check failed",
            "timestamp": datetime.utcnow().isoformat(),
            "health": {
                "service": "unhealthy",
                "browser_pool": "unknown",
                "queue": "unknown",
                "issues": [f"Health check system error: {str(e)}"]
            }
        }

@app.get("/screenshot-metrics-detailed", response_model=Dict[str, Any])
async def get_screenshot_metrics_detailed() -> Dict[str, Any]:
    """
    Get detailed screenshot API performance metrics and analytics.
    ✅ NEW: Comprehensive metrics for screenshot API optimization monitoring.
    
    Returns:
        Dictionary with detailed performance analytics
    """
    try:
        screenshot_status = await screenshot_queue.get_status()
        browser_status = await browser_pool.get_pool_status()
        performance_stats = metrics.get_stats()
        
        # Calculate additional analytics
        total_requests = screenshot_status.get("total_requests", 0)
        successful_requests = screenshot_status.get("successful_requests", 0)
        failed_requests = screenshot_status.get("failed_requests", 0)
        
        # Browser efficiency metrics
        browser_creation_count = screenshot_status.get("browser_creation_count", 0)
        browser_reuse_count = screenshot_status.get("browser_reuse_count", 0)
        total_browser_operations = browser_creation_count + browser_reuse_count
        
        browser_reuse_rate = (browser_reuse_count / total_browser_operations * 100) if total_browser_operations > 0 else 0
        
        # Queue efficiency metrics
        queue_rejections = screenshot_status.get("queue_rejections", 0)
        timeout_errors = screenshot_status.get("timeout_errors", 0)
        queue_rejection_rate = (queue_rejections / max(total_requests, 1) * 100)
        timeout_rate = (timeout_errors / max(total_requests, 1) * 100)
        
        return {
            "status_code": 200,
            "message": "Detailed screenshot metrics retrieved successfully",
            "timestamp": datetime.utcnow().isoformat(),
            "performance_summary": {
                "total_requests": total_requests,
                "successful_requests": successful_requests,
                "failed_requests": failed_requests,
                "success_rate_percent": screenshot_status.get("success_rate_percent", 0),
                "avg_response_time_seconds": screenshot_status.get("avg_response_time_seconds", 0),
                "recent_avg_response_time_seconds": screenshot_status.get("recent_avg_response_time_seconds", 0)
            },
            "queue_analytics": {
                "active_requests": screenshot_status.get("active_requests", 0),
                "max_concurrent": screenshot_status.get("max_concurrent", 0),
                "utilization_percent": screenshot_status.get("queue_utilization_percent", 0),
                "queue_rejections": queue_rejections,
                "timeout_errors": timeout_errors,
                "queue_rejection_rate_percent": round(queue_rejection_rate, 2),
                "timeout_rate_percent": round(timeout_rate, 2)
            },
            "browser_analytics": {
                "total_browsers": browser_status.get("total_browsers", 0),
                "max_browsers": browser_status.get("max_browsers", 0),
                "browser_utilization_percent": browser_status.get("browser_utilization_percent", 0),
                "total_active_tabs": browser_status.get("total_active_tabs", 0),
                "tab_utilization_percent": browser_status.get("tab_utilization_percent", 0),
                "browser_creation_count": browser_creation_count,
                "browser_reuse_count": browser_reuse_count,
                "browser_reuse_rate_percent": round(browser_reuse_rate, 2)
            },
            "cache_analytics": {
                "cache_hits": screenshot_status.get("cache_hits", 0),
                "cache_misses": screenshot_status.get("cache_misses", 0),
                "cache_hit_rate_percent": screenshot_status.get("cache_hit_rate_percent", 0)
            },
            "configuration": {
                "optimized_settings": {
                    "max_concurrent": settings.screenshot_max_concurrent,
                    "queue_enabled": settings.screenshot_enable_queuing,
                    "queue_timeout": settings.screenshot_queue_timeout,
                    "screenshot_timeout": settings.screenshot_timeout,
                    "retry_attempts": settings.screenshot_retry_attempts,
                    "browser_pool_size": settings.browser_pool_size,
                    "max_tabs_per_browser": settings.max_tabs_per_browser,
                    "concurrent_browser_creation": settings.browser_launch_concurrent
                },
                "theoretical_capacity": {
                    "max_concurrent_screenshots": settings.screenshot_max_concurrent,
                    "max_browser_tabs": settings.browser_pool_size * settings.max_tabs_per_browser,
                    "estimated_throughput_per_minute": settings.screenshot_max_concurrent * (60 / max(screenshot_status.get("avg_response_time_seconds", 30), 1))
                }
            },
            "raw_data": {
                "screenshot_queue": screenshot_status,
                "browser_pool": browser_status,
                "system_performance": performance_stats
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Error getting detailed screenshot metrics: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve detailed screenshot metrics"
        )

# =====================================================================
# END SCREENSHOT API MONITORING ENDPOINTS
# =====================================================================

