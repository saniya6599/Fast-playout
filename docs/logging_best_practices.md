# Logging Best Practices for Fast-playout

## Overview
This document outlines the logging standards and best practices for the Fast-playout application to ensure consistent, useful, and maintainable logging.

## Log Levels

### DEBUG
- Detailed information for debugging
- Function entry/exit points
- Variable values and state information
- Network packet details
- Performance metrics

**Example:**
```python
logger.debug("Processing video file", file_path=path, file_size=size)
```

### INFO
- General application flow
- Successful operations
- Configuration changes
- User actions
- System state changes

**Example:**
```python
logger.info("Playlist loaded successfully", playlist_name=name, item_count=count)
```

### WARNING
- Unexpected but recoverable situations
- Deprecated feature usage
- Resource usage approaching limits
- Retry attempts

**Example:**
```python
logger.warning("Connection timeout, retrying", attempt=retry_count, max_attempts=3)
```

### ERROR
- Failed operations that affect functionality
- Exceptions that are caught and handled
- External service failures
- Data validation failures

**Example:**
```python
logger.error("Failed to load playlist", playlist_name=name, error=str(e))
```

### CRITICAL
- System failures that require immediate attention
- Data corruption
- Security violations
- Unrecoverable errors

**Example:**
```python
logger.critical("Database connection lost", db_host=host, error=str(e))
```

## Structured Logging

### Context Information
Always include relevant context in log messages:

```python
# Good
logger.info("Video processing started", 
           video_id=video_id, 
           format=format, 
           duration=duration)

# Bad
logger.info(f"Processing video {video_id}")
```

### Performance Logging
Use the provided decorators for function timing:

```python
from CommonServices.logging_utils import log_function_call

@log_function_call
def process_video(video_path):
    # Function implementation
    pass
```

### Exception Logging
Use the dedicated exception logging method:

```python
try:
    # Some operation
    pass
except Exception as e:
    logger.log_exception(e, context={'operation': 'video_processing'})
```

## Command Logging

### Polling Commands
Polling commands (CHECK-STATUS, FETCH, POLL, STATE) should be logged at DEBUG level to reduce noise:

```python
logger.log_command_received(command, source="tcp")
```

### Action Commands
Action commands should be logged at INFO level with context:

```python
logger.info("Command executed", command=command, result=result, duration=duration)
```

## Connection Logging

### Connection Events
Use the dedicated connection logging methods:

```python
logger.log_connection_event("accepted", host, port, success=True)
logger.log_connection_event("failed", host, port, success=False, error=e)
```

### Message Logging
For network messages, log at DEBUG level with size information:

```python
logger.debug("Message received", 
            message_length=len(message), 
            message_preview=message[:100])
```

## Performance Monitoring

### System Health
Log system health metrics periodically:

```python
from CommonServices.logging_utils import log_system_health

# Log every 5 minutes
log_system_health(channel=channel_name)
```

### Database Operations
Log database operations with timing:

```python
from CommonServices.logging_utils import log_database_operation

start_time = time.time()
# Database operation
duration = time.time() - start_time
log_database_operation("SELECT", table="playlists", duration=duration)
```

## Log File Management

### Rotation
Log files are automatically rotated based on size and date:
- Daily rotation: `app_YYYY-MM-DD.log`
- Size rotation: 10MB per file
- Retention: 7 days by default

### Cleanup
Old log files are automatically cleaned up:
- Runs daily at 1:00 AM
- Configurable retention period
- Compresses old logs if enabled

## Configuration

### Environment Variables
- `DEBUG=true`: Enable console output and detailed logging
- `LOG_LEVEL=DEBUG`: Set minimum log level
- `LOG_DIR=logs`: Set log directory

### Configuration File
Use `logging_config.json` to configure:
- Log formats
- Handler settings
- Performance monitoring
- Debug mode settings

## Monitoring and Alerting

### Log Analysis
Use the provided log analyzer:

```bash
python scripts/log_analyzer.py --date 2024-01-15
python scripts/log_analyzer.py --hours 24
```

### Key Metrics to Monitor
- Error rate by type
- Response times for operations
- Connection success/failure rates
- System resource usage
- Command execution patterns

### Alerting Thresholds
- Error rate > 5% in 5 minutes
- Response time > 2 seconds
- Memory usage > 80%
- Disk usage > 90%

## Common Patterns

### Function Entry/Exit
```python
@log_function_call
def process_playlist(playlist_data):
    logger.log_operation_start("playlist_processing", playlist_id=playlist_data['id'])
    try:
        # Processing logic
        result = do_processing(playlist_data)
        logger.log_operation_end("playlist_processing", success=True, result=result)
        return result
    except Exception as e:
        logger.log_operation_end("playlist_processing", success=False, error=str(e))
        raise
```

### Database Operations
```python
def save_playlist(playlist):
    start_time = time.time()
    try:
        # Database operation
        result = db.save(playlist)
        duration = time.time() - start_time
        log_database_operation("INSERT", table="playlists", duration=duration)
        return result
    except Exception as e:
        duration = time.time() - start_time
        log_database_operation("INSERT", table="playlists", duration=duration, error=str(e))
        raise
```

### Network Operations
```python
def send_command(command):
    start_time = time.time()
    try:
        response = requests.post(url, json=command)
        duration = time.time() - start_time
        log_network_operation("POST", url=url, status_code=response.status_code, duration=duration)
        return response
    except Exception as e:
        duration = time.time() - start_time
        log_network_operation("POST", url=url, error=str(e), duration=duration)
        raise
```

## Troubleshooting

### High Log Volume
- Check for DEBUG level logging in production
- Review polling command frequency
- Verify log rotation settings

### Missing Logs
- Check log file permissions
- Verify log directory exists
- Review log level configuration

### Performance Issues
- Monitor log file I/O
- Check for synchronous logging
- Review log message size

## Security Considerations

### Sensitive Data
Never log sensitive information:
- Passwords
- API keys
- Personal data
- Internal IP addresses (in some cases)

### Log Access
- Restrict log file access
- Use log rotation to limit file size
- Consider log encryption for sensitive environments

## Integration with External Systems

### ELK Stack
The logging system can be configured to send logs to Elasticsearch:

```python
# In logging_config.json
{
    "handlers": {
        "elasticsearch_handler": {
            "class": "logging.handlers.HTTPHandler",
            "host": "elasticsearch.example.com",
            "url": "/logs",
            "method": "POST"
        }
    }
}
```

### Monitoring Tools
- Prometheus metrics
- Grafana dashboards
- Custom alerting rules

## Maintenance

### Regular Tasks
- Review log retention policies
- Monitor disk usage
- Update log analysis scripts
- Review and update alerting thresholds

### Performance Optimization
- Use asynchronous logging where appropriate
- Batch log messages when possible
- Monitor log processing overhead
