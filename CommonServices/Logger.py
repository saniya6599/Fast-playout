import datetime
import logging
import os
import re
import requests
import json
import traceback
import functools
import time
from collections import deque
from apscheduler.schedulers.background import BackgroundScheduler
from CommonServices.Global_context import GlobalContext


class Logger:
       
    def __init__(self, log_dir="logs", log_level=logging.INFO):
        self.global_context = GlobalContext()
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)  # Create log directory if it doesn't exist
        self.log_level = log_level
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(self.log_level)
        self._configure_logger()
        self._start_log_cleanup_scheduler()
        
        # Performance tracking
        self.performance_logs = deque(maxlen=1000)
        
    def _get_log_file_for_today(self):
        date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        return os.path.join(self.log_dir, f"app_{date_str}.log")

    def _configure_logger(self):
        """Ensure log file exists and configure logging handlers."""
        self.log_file = self._get_log_file_for_today()
        open(self.log_file, "a").close()  # Create file if missing
        
        # Remove existing handlers (needed when recreating log file)
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        # Add new file handler with structured format
        file_handler = logging.FileHandler(self.log_file, mode="a")
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
        )
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        # Add console handler for development
        if os.environ.get('DEBUG', 'false').lower() == 'true':
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    def _check_log_file(self):
        """Reconfigure logger if the date has changed."""
        expected_log_file = self._get_log_file_for_today()
        if self.log_file != expected_log_file:
            self._configure_logger()

    def log(self, level, message, **kwargs):
        """Enhanced logging with context and accurate caller location."""
        self._check_log_file()

        # Add context information
        channel_name = self.global_context.get_value('channel_name')
        if not channel_name:
            channel_name = 'unknown'

        context = {
            'channel': channel_name,
            'timestamp': datetime.datetime.now().isoformat(),
            **kwargs
        }

        # Format message with context
        formatted_message = f"{message} | Context: {json.dumps(context, default=str)}"

        # Use stacklevel to point to the original caller (skip: info/debug -> log)
        level_no = getattr(logging, level.upper(), logging.INFO)
        try:
            self.logger.log(level_no, formatted_message, stacklevel=3)
        except TypeError:
            # Fallback for environments without stacklevel support
            self.logger.log(level_no, formatted_message)

        # Track performance for critical operations
        if level.upper() in ['ERROR', 'CRITICAL']:
            self._track_performance(level, message, context)

    def debug(self, message, **kwargs):
        self.log("DEBUG", message, **kwargs)

    def info(self, message, **kwargs):
        self.log("INFO", message, **kwargs)

    def warning(self, message, **kwargs):
        self.log("WARNING", message, **kwargs)

    def error(self, message, **kwargs):
        self.log("ERROR", message, **kwargs)

    def critical(self, message, **kwargs):
        self.log("CRITICAL", message, **kwargs)
        
    def _track_performance(self, level, message, context):
        """Track performance metrics for critical operations."""
        performance_data = {
            'level': level,
            'message': message,
            'context': context,
            'timestamp': datetime.datetime.now().isoformat(),
            'memory_usage': self._get_memory_usage()
        }
        self.performance_logs.append(performance_data)
        
    def _get_memory_usage(self):
        """Get current memory usage (if psutil is available)."""
        try:
            import psutil
            process = psutil.Process()
            return {
                'rss': process.memory_info().rss,
                'vms': process.memory_info().vms,
                'percent': process.memory_percent()
            }
        except ImportError:
            return {'error': 'psutil not available'}
            
    def log_function_call(self, func_name, args=None, kwargs=None, duration=None):
        """Log function calls with performance metrics."""
        self.info(
            f"Function call: {func_name}",
            function_name=func_name,
            args=str(args) if args else None,
            kwargs=str(kwargs) if kwargs else None,
            duration_ms=duration * 1000 if duration else None
        )
        
    def log_exception(self, exception, context=None):
        """Log exceptions with full stack trace and context."""
        self.error(
            f"Exception occurred: {str(exception)}",
            exception_type=type(exception).__name__,
            exception_message=str(exception),
            stack_trace=traceback.format_exc(),
            context=context or {}
        )
        
    def log_operation_start(self, operation_name, **kwargs):
        """Log the start of an operation."""
        self.info(
            f"Operation started: {operation_name}",
            operation=operation_name,
            status="started",
            **kwargs
        )
        
    def log_operation_end(self, operation_name, success=True, duration=None, **kwargs):
        """Log the end of an operation."""
        status = "completed" if success else "failed"
        self.info(
            f"Operation {status}: {operation_name}",
            operation=operation_name,
            status=status,
            duration_ms=duration * 1000 if duration else None,
            **kwargs
        )
        
    def log_connection_event(self, event_type, host, port, success=True, error=None, connection_id=None):
        """Log connection-related events."""
        # Use DEBUG for frequent events to reduce noise
        log_fn = self.debug if event_type in ["accepted", "received", "sent"] else self.info
        log_fn(
            f"Connection {event_type}: {host}:{port}",
            connection_event=event_type,
            host=host,
            port=port,
            success=success,
            error=str(error) if error else None,
            connection_id=connection_id
        )
        
    def log_command_received(self, command, source=None):
        """Log received commands."""
        # Filter out frequent polling commands to reduce log noise
        if any(word in command for word in ["CHECK-STATUS", "FETCH", "POLL", "STATE"]):
            self.debug(f"Polling command received: {command}", command=command, source=source)
        else:
            self.info(f"Command received: {command}", command=command, source=source)

    def get_logs(self, params):
       
       
        date = params["date"] or datetime.datetime.now().strftime("%Y-%m-%d")
        log_type = params["log_type"] or "all"
        
        log_file_path = os.path.join(self.log_dir, f"app_{date}.log")
        if not os.path.exists(log_file_path):
            return {"error": f"Log file for {date} not found", "logs": []}

        
        try:
            with open(log_file_path, "r") as f:
                logs = f.readlines()
        except FileNotFoundError:
            return {"error": "Log file not found", "logs": []}
        
        
        # filtered_logs = [
        #     log for log in  logs
        #     if date in log and (log_type.lower() == "all" or f"- {log_type.upper()} -" in log)
        # ]
        
        filtered_logs = [
            log for log in logs
            if log_type == "all" or f"- {log_type.upper()} -" in log
        ]

        payload = {
            "type": log_type,
            "date": date,
            "logs": filtered_logs
        }

        return payload
     
    def _start_log_cleanup_scheduler(self):

        scheduler = BackgroundScheduler()

        def scheduled_cleanup():
            try:
                retention_days = int(self.global_context.get_value("retention days") or 3)
                self.logger.info(f"Logs retention days configured : {retention_days}")
                self.cleanup_old_logs(log_retention_days=retention_days)
            except Exception as e:
                self.logger.error(f"Log cleanup failed: {str(e)}")

        scheduler.add_job(scheduled_cleanup, 'cron', hour=1, minute=0)  # Daily at 1:00 AM
        scheduler.start()
       
    def cleanup_old_logs(self, log_retention_days=3):

        now = datetime.datetime.now()
        pattern = re.compile(r"app_(\d{4}-\d{2}-\d{2})\.log")

        for filename in os.listdir(self.log_dir):
            match = pattern.match(filename)
            if match:
                log_date_str = match.group(1)
                try:
                    log_date = datetime.datetime.strptime(log_date_str, "%Y-%m-%d")
                    age = (now - log_date).days
                    if age > log_retention_days:
                        file_path = os.path.join(self.log_dir, filename)
                        os.remove(file_path)
                        self.logger.info(f"Deleted old log file: {filename}")
                except ValueError:
                    continue