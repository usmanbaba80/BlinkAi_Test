"""
Database connection management for PostgreSQL.

This module provides PostgreSQL database operations using asyncpg.
"""

import asyncio
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any
from app.core.config import settings
from app.core.logging import logger

# Global connection pool
connection_pool = None
pool_lock = asyncio.Lock()

# Import PostgreSQL driver
try:
    import asyncpg
    logger.info(f"🐘 Using PostgreSQL database driver (asyncpg)")
except ImportError:
    logger.error("❌ asyncpg not installed. Run: pip install asyncpg")
    raise


async def init_connection_pool():
    """Initialize PostgreSQL database connection pool."""
    global connection_pool
    
    try:
        async with pool_lock:
            # PostgreSQL connection pool
            connection_pool = await asyncpg.create_pool(
                host=settings.db_host,
                port=settings.db_port,
                user=settings.db_user,
                password=settings.db_password,
                database=settings.db_name,
                min_size=5,
                max_size=settings.db_pool_size,
                command_timeout=getattr(settings, 'db_command_timeout', 30),  # Query timeout
                # SSL configuration
                ssl='require' if settings.db_ssl_ca else None
            )
            logger.info(f"✅ PostgreSQL connection pool initialized successfully")
            logger.info(f"   Database: {settings.db_name}")
            logger.info(f"   Host: {settings.db_host}:{settings.db_port}")
            logger.info(f"   Max connections: {settings.db_pool_size}")
                
    except Exception as e:
        logger.error(f"❌ Failed to initialize PostgreSQL connection pool: {e}")
        connection_pool = None
        raise


@asynccontextmanager
async def get_db_connection():
    """
    Get PostgreSQL database connection from pool.
    
    Usage:
        async with get_db_connection() as conn:
            # Use connection
            result = await execute_query(conn, query, params)
    """
    global connection_pool
    
    if connection_pool is None:
        logger.error(f"❌ PostgreSQL connection pool not initialized")
        raise Exception(f"PostgreSQL connection pool not available. Check database configuration in .env file.")
    
    try:
        async with connection_pool.acquire() as conn:
            yield conn
    except Exception as e:
        logger.warning(f"⚠️ Pool error detected: {e}. Attempting to reset connection pool.")
        try:
            await init_connection_pool()
            if connection_pool is None:
                raise Exception("Failed to reinitialize connection pool")
            async with connection_pool.acquire() as conn:
                yield conn
        except Exception as reset_error:
            logger.error(f"❌ Failed to reset connection pool: {reset_error}")
            connection_pool = None
            raise Exception(f"PostgreSQL connection failed: {reset_error}")


async def execute_query(conn, query: str, params: tuple = None) -> List[asyncpg.Record]:
    """
    Execute SELECT query using PostgreSQL.
    
    Args:
        conn: PostgreSQL database connection
        query: SQL query (use $1, $2, $3 or %s syntax - auto-converted)
        params: Query parameters as tuple
        
    Returns:
        List of query results (asyncpg.Record objects)
        
    Example:
        result = await execute_query(conn, "SELECT * FROM table WHERE id = $1", (123,))
        # Or with %s (auto-converted):
        result = await execute_query(conn, "SELECT * FROM table WHERE id = %s", (123,))
    """
    try:
        # PostgreSQL uses $1, $2, $3 syntax
        # Convert %s to $1, $2, $3 if needed
        if '%s' in query:
            count = query.count('%s')
            for i in range(1, count + 1):  # FIXED: was range(count, 0, -1) which reversed order!
                query = query.replace('%s', f'${i}', 1)
        
        if params:
            result = await conn.fetch(query, *params)
        else:
            result = await conn.fetch(query)
        return result
                
    except Exception as e:
        logger.error(f"❌ PostgreSQL query execution error: {e}")
        logger.error(f"   Query: {query}")
        logger.error(f"   Params: {params}")
        raise


async def execute_update(conn, query: str, params: tuple = None) -> str:
    """
    Execute INSERT/UPDATE/DELETE using PostgreSQL.
    
    Args:
        conn: PostgreSQL database connection
        query: SQL query (use $1, $2, $3 or %s syntax - auto-converted)
        params: Query parameters as tuple
        
    Returns:
        Status string (e.g., "INSERT 0 1", "UPDATE 5", "DELETE 3")
        
    Example:
        status = await execute_update(conn, "INSERT INTO table (col) VALUES ($1)", ("value",))
        # Or with %s (auto-converted):
        status = await execute_update(conn, "INSERT INTO table (col) VALUES (%s)", ("value",))
    """
    try:
        # Convert %s to $1, $2, $3 if needed
        if '%s' in query:
            count = query.count('%s')
            for i in range(1, count + 1):  # FIXED: was range(count, 0, -1) which reversed order!
                query = query.replace('%s', f'${i}', 1)
        
        if params:
            result = await conn.execute(query, *params)
        else:
            result = await conn.execute(query)
        return result
                
    except Exception as e:
        logger.error(f"❌ PostgreSQL update execution error: {e}")
        logger.error(f"   Query: {query}")
        logger.error(f"   Params: {params}")
        raise


async def close_connection_pool():
    """Close the PostgreSQL database connection pool."""
    global connection_pool
    
    if connection_pool:
        try:
            await connection_pool.close()
            logger.info(f"✅ PostgreSQL connection pool closed")
            connection_pool = None
        except Exception as e:
            logger.error(f"❌ Error closing PostgreSQL connection pool: {e}")

