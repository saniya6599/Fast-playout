import datetime
import logging
import os
import re
import requests
from collections import deque
from apscheduler.schedulers.background import BackgroundScheduler
from CommonServices.Global_context import GlobalContext


class Logger:
       

    # def __init__(self, log_file="app.log", log_level=logging.INFO):
    #     self.log_file = log_file
    #     self.log_level = log_level
    #     self.logger = logging.getLogger(__name__)
    #     self.logger.setLevel(self.log_level)
    #     self._configure_logger()
    
    def __init__(self, log_dir="logs", log_level=logging.INFO):
        self.global_context = GlobalContext()
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)  # Create log directory if it doesn't exist
        self.log_level = log_level
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(self.log_level)
        self._configure_logger()
        self._start_log_cleanup_scheduler()
        
        
    def _get_log_file_for_today(self):
        date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        return os.path.join(self.log_dir, f"app_{date_str}.log")

    def _configure_logger(self):
        """Ensure log file exists and configure logging handlers."""
        self.log_file = self._get_log_file_for_today()
        open(self.log_file, "a").close()  # Create file   def cleanup_old_logs(self, log_retention_days=3):f missing
        
        # Remove existing handlers (needed when recreating log file)
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        # Add new file handler
        file_handler = logging.FileHandler(self.log_file, mode="a")
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def _check_log_file(self):
        """Reconfigure logger if the log file is deleted."""
        """Reconfigure logger if the date has changed."""
        expected_log_file = self._get_log_file_for_today()
        # if not os.path.exists(self.log_file):
        if self.log_file != expected_log_file:
            self._configure_logger()

    def log(self, level, message):
        self._check_log_file()
        getattr(self.logger, level.lower())(message)

    def debug(self, message):
        self.logger.debug(message)
        # self._store_log("DEBUG", message)

    def info(self, message):
        self.logger.info(message)
        # self._store_log("INFO", message)

    def warning(self, message):
        self.logger.warning(message)
        # self._store_log("WARNING", message)

    def error(self, message):
        self.logger.error(message)
        # self._store_log("ERROR", message)

    def critical(self, message):
        self.logger.critical(message)
    # self._store_log("CRITICAL", message) 
        
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