#!/usr/bin/env python3
"""
Log Analyzer for Fast-playout Application
Analyzes log files to identify patterns, errors, and performance issues.
"""

import argparse
import json
import re
import os
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import matplotlib.pyplot as plt
import pandas as pd

class LogAnalyzer:
    def __init__(self, log_dir="logs"):
        self.log_dir = log_dir
        self.log_pattern = re.compile(
            r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - (\w+) - (\w+) - \[([^:]+):(\d+)\] - (.+)'
        )
        
    def parse_log_line(self, line):
        """Parse a log line and extract components."""
        match = self.log_pattern.match(line.strip())
        if match:
            timestamp, logger_name, level, filename, line_num, message = match.groups()
            return {
                'timestamp': datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S,%f'),
                'logger': logger_name,
                'level': level,
                'filename': filename,
                'line': int(line_num),
                'message': message
            }
        return None
    
    def load_logs(self, date=None, hours=None):
        """Load logs for a specific date or time range."""
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
            
        log_file = os.path.join(self.log_dir, f"app_{date}.log")
        if not os.path.exists(log_file):
            print(f"Log file not found: {log_file}")
            return []
            
        logs = []
        with open(log_file, 'r', encoding='utf-8') as f:
            for line in f:
                parsed = self.parse_log_line(line)
                if parsed:
                    if hours:
                        cutoff_time = datetime.now() - timedelta(hours=hours)
                        if parsed['timestamp'] >= cutoff_time:
                            logs.append(parsed)
                    else:
                        logs.append(parsed)
        
        return logs
    
    def analyze_errors(self, logs):
        """Analyze error patterns in logs."""
        errors = [log for log in logs if log['level'] in ['ERROR', 'CRITICAL']]
        
        if not errors:
            print("No errors found in the specified time range.")
            return
            
        print(f"\n=== ERROR ANALYSIS ({len(errors)} errors) ===")
        
        # Error by type
        error_types = Counter([log['message'].split('|')[0].strip() for log in errors])
        print("\nTop Error Types:")
        for error_type, count in error_types.most_common(10):
            print(f"  {error_type}: {count}")
            
        # Error by file
        error_files = Counter([log['filename'] for log in errors])
        print("\nErrors by File:")
        for filename, count in error_files.most_common(5):
            print(f"  {filename}: {count}")
            
        # Error timeline
        error_timeline = defaultdict(int)
        for error in errors:
            hour = error['timestamp'].strftime('%Y-%m-%d %H:00')
            error_timeline[hour] += 1
            
        print("\nError Timeline (by hour):")
        for hour, count in sorted(error_timeline.items()):
            print(f"  {hour}: {count} errors")
    
    def analyze_performance(self, logs):
        """Analyze performance-related logs."""
        performance_logs = [log for log in logs if 'duration_ms' in log['message']]
        
        if not performance_logs:
            print("No performance logs found.")
            return
            
        print(f"\n=== PERFORMANCE ANALYSIS ({len(performance_logs)} operations) ===")
        
        # Extract duration information
        durations = []
        for log in performance_logs:
            match = re.search(r'duration_ms=([\d.]+)', log['message'])
            if match:
                durations.append(float(match.group(1)))
        
        if durations:
            print(f"Average operation duration: {sum(durations)/len(durations):.2f}ms")
            print(f"Max operation duration: {max(durations):.2f}ms")
            print(f"Min operation duration: {min(durations):.2f}ms")
            
            # Slow operations
            slow_threshold = 1000  # 1 second
            slow_ops = [d for d in durations if d > slow_threshold]
            if slow_ops:
                print(f"Slow operations (>1s): {len(slow_ops)}")
    
    def analyze_connections(self, logs):
        """Analyze connection-related logs."""
        connection_logs = [log for log in logs if 'Connection' in log['message']]
        
        if not connection_logs:
            print("No connection logs found.")
            return
            
        print(f"\n=== CONNECTION ANALYSIS ({len(connection_logs)} events) ===")
        
        # Connection events by type
        event_types = Counter()
        for log in connection_logs:
            if 'accepted' in log['message']:
                event_types['accepted'] += 1
            elif 'server_started' in log['message']:
                event_types['server_started'] += 1
            elif 'closed' in log['message']:
                event_types['closed'] += 1
                
        for event_type, count in event_types.items():
            print(f"  {event_type}: {count}")
    
    def analyze_commands(self, logs):
        """Analyze command patterns."""
        command_logs = [log for log in logs if 'Command received' in log['message']]
        
        if not command_logs:
            print("No command logs found.")
            return
            
        print(f"\n=== COMMAND ANALYSIS ({len(command_logs)} commands) ===")
        
        # Command types
        command_types = Counter()
        for log in command_logs:
            message = log['message']
            for cmd_type in ['CHECK-STATUS', 'FETCH-LIST', 'LOAD-PLAYLIST', 'PLAY', 'STOP', 'PAUSE']:
                if cmd_type in message:
                    command_types[cmd_type] += 1
                    break
                    
        print("Commands by type:")
        for cmd_type, count in command_types.most_common():
            print(f"  {cmd_type}: {count}")
    
    def generate_report(self, date=None, hours=None):
        """Generate a comprehensive log analysis report."""
        print(f"=== LOG ANALYSIS REPORT ===")
        print(f"Date: {date or 'today'}")
        if hours:
            print(f"Time range: Last {hours} hours")
        print(f"Log directory: {self.log_dir}")
        
        logs = self.load_logs(date, hours)
        if not logs:
            print("No logs found for the specified criteria.")
            return
            
        print(f"Total log entries: {len(logs)}")
        
        # Basic statistics
        levels = Counter([log['level'] for log in logs])
        print(f"\nLog levels: {dict(levels)}")
        
        # Run analyses
        self.analyze_errors(logs)
        self.analyze_performance(logs)
        self.analyze_connections(logs)
        self.analyze_commands(logs)
        
        # Generate visualizations
        self.generate_visualizations(logs)
    
    def generate_visualizations(self, logs):
        """Generate visualizations of log data."""
        try:
            # Create time series of log levels
            df = pd.DataFrame(logs)
            df['hour'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:00')
            
            # Log levels over time
            level_counts = df.groupby(['hour', 'level']).size().unstack(fill_value=0)
            
            plt.figure(figsize=(12, 6))
            level_counts.plot(kind='line', marker='o')
            plt.title('Log Levels Over Time')
            plt.xlabel('Time')
            plt.ylabel('Count')
            plt.legend()
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig('log_analysis.png')
            print("\nVisualization saved as 'log_analysis.png'")
            
        except ImportError:
            print("\nMatplotlib not available for visualizations")
        except Exception as e:
            print(f"\nError generating visualizations: {e}")

def main():
    parser = argparse.ArgumentParser(description='Analyze Fast-playout application logs')
    parser.add_argument('--date', help='Date to analyze (YYYY-MM-DD)')
    parser.add_argument('--hours', type=int, help='Analyze last N hours')
    parser.add_argument('--log-dir', default='logs', help='Log directory')
    parser.add_argument('--output', help='Output file for report')
    
    args = parser.parse_args()
    
    analyzer = LogAnalyzer(args.log_dir)
    analyzer.generate_report(args.date, args.hours)

if __name__ == '__main__':
    main()
