import functools
import time
import traceback
from CommonServices.Logger import Logger

logger = Logger()

def log_function_call(func):
    """Decorator to log function calls with timing and context."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        func_name = func.__name__
        
        try:
            logger.log_operation_start(func_name, args=str(args), kwargs=str(kwargs))
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            logger.log_operation_end(func_name, success=True, duration=duration)
            return result
        except Exception as e:
            duration = time.time() - start_time
            logger.log_exception(e, context={
                'function': func_name,
                'args': str(args),
                'kwargs': str(kwargs),
                'duration': duration
            })
            raise
    
    return wrapper

def log_async_function_call(func):
    """Decorator to log async function calls."""
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        func_name = func.__name__
        
        try:
            logger.log_operation_start(func_name, args=str(args), kwargs=str(kwargs))
            result = await func(*args, **kwargs)
            duration = time.time() - start_time
            logger.log_operation_end(func_name, success=True, duration=duration)
            return result
        except Exception as e:
            duration = time.time() - start_time
            logger.log_exception(e, context={
                'function': func_name,
                'args': str(args),
                'kwargs': str(kwargs),
                'duration': duration
            })
            raise
    
    return wrapper

def replace_print_with_log(print_message, log_level="INFO", **context):
    """Replace print statements with appropriate logging."""
    if log_level.upper() == "DEBUG":
        logger.debug(print_message, **context)
    elif log_level.upper() == "WARNING":
        logger.warning(print_message, **context)
    elif log_level.upper() == "ERROR":
        logger.error(print_message, **context)
    elif log_level.upper() == "CRITICAL":
        logger.critical(print_message, **context)
    else:
        logger.info(print_message, **context)

def log_performance_metric(metric_name, value, unit=None, **context):
    """Log performance metrics."""
    logger.info(
        f"Performance metric: {metric_name} = {value}{unit or ''}",
        metric_name=metric_name,
        metric_value=value,
        metric_unit=unit,
        **context
    )

def log_system_health(**context):
    """Log system health information."""
    try:
        import psutil
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        logger.info(
            "System health check",
            cpu_percent=cpu_percent,
            memory_percent=memory.percent,
            memory_available=memory.available,
            disk_percent=disk.percent,
            disk_free=disk.free,
            **context
        )
    except ImportError:
        logger.warning("psutil not available for system health monitoring")
    except Exception as e:
        logger.error(f"Failed to get system health: {e}")

def log_database_operation(operation, table=None, query=None, duration=None, **context):
    """Log database operations."""
    logger.info(
        f"Database operation: {operation}",
        db_operation=operation,
        db_table=table,
        db_query=query,
        duration_ms=duration * 1000 if duration else None,
        **context
    )

def log_network_operation(operation, url=None, method=None, status_code=None, duration=None, **context):
    """Log network operations."""
    logger.info(
        f"Network operation: {operation}",
        network_operation=operation,
        network_url=url,
        network_method=method,
        network_status_code=status_code,
        duration_ms=duration * 1000 if duration else None,
        **context
    )
