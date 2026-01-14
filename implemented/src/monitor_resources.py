#!/usr/bin/env python3
"""
Resource monitoring script for CS553 HW5
Monitors CPU, memory, and disk I/O usage
"""

import psutil
import time
import sys
import argparse
from datetime import datetime

def monitor_process(pid, output_file, interval=1):
    """
    Monitor a specific process and log resource usage
    
    Args:
        pid: Process ID to monitor
        output_file: Output CSV file
        interval: Sampling interval in seconds
    """
    try:
        process = psutil.Process(pid)
    except psutil.NoSuchProcess:
        print(f"Process {pid} not found")
        return
    
    # Open output file
    with open(output_file, 'w') as f:
        # Write header
        f.write("timestamp,elapsed_sec,cpu_percent,mem_gb,disk_read_mb_s,disk_write_mb_s\n")
        f.flush()
        
        start_time = time.time()
        prev_disk_read = 0
        prev_disk_write = 0
        
        print(f"Monitoring process {pid}... (Ctrl+C to stop)")
        print(f"Output: {output_file}")
        
        try:
            while process.is_running():
                current_time = time.time()
                elapsed = current_time - start_time
                
                # CPU usage
                cpu_percent = process.cpu_percent(interval=0.1)
                
                # Memory usage (in GB)
                mem_info = process.memory_info()
                mem_gb = mem_info.rss / (1024 ** 3)
                
                # Disk I/O (MB/s)
                disk_io = psutil.disk_io_counters()
                disk_read_mb = disk_io.read_bytes / (1024 ** 2)
                disk_write_mb = disk_io.write_bytes / (1024 ** 2)
                
                # Calculate rates
                if prev_disk_read > 0:
                    disk_read_rate = (disk_read_mb - prev_disk_read) / interval
                    disk_write_rate = (disk_write_mb - prev_disk_write) / interval
                else:
                    disk_read_rate = 0
                    disk_write_rate = 0
                
                prev_disk_read = disk_read_mb
                prev_disk_write = disk_write_mb
                
                # Write to file
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"{timestamp},{elapsed:.1f},{cpu_percent:.1f},{mem_gb:.2f},"
                       f"{disk_read_rate:.2f},{disk_write_rate:.2f}\n")
                f.flush()
                
                # Print to console
                print(f"\r[{elapsed:6.1f}s] CPU: {cpu_percent:5.1f}% | "
                      f"MEM: {mem_gb:5.2f}GB | "
                      f"DISK R: {disk_read_rate:6.2f}MB/s | "
                      f"DISK W: {disk_write_rate:6.2f}MB/s", end='')
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\nMonitoring stopped by user")
        except psutil.NoSuchProcess:
            print(f"\nProcess {pid} has terminated")

def monitor_system(output_file, duration=None, interval=1):
    """
    Monitor overall system resource usage
    
    Args:
        output_file: Output CSV file
        duration: Duration in seconds (None for infinite)
        interval: Sampling interval in seconds
    """
    with open(output_file, 'w') as f:
        # Write header
        f.write("timestamp,elapsed_sec,cpu_percent,mem_gb,mem_percent,"
               "disk_read_mb_s,disk_write_mb_s\n")
        f.flush()
        
        start_time = time.time()
        prev_disk_read = 0
        prev_disk_write = 0
        
        print(f"Monitoring system... (Ctrl+C to stop)")
        print(f"Output: {output_file}")
        
        try:
            while True:
                current_time = time.time()
                elapsed = current_time - start_time
                
                if duration and elapsed > duration:
                    break
                
                # CPU usage
                cpu_percent = psutil.cpu_percent(interval=0.1)
                
                # Memory usage
                mem = psutil.virtual_memory()
                mem_gb = mem.used / (1024 ** 3)
                mem_percent = mem.percent
                
                # Disk I/O
                disk_io = psutil.disk_io_counters()
                disk_read_mb = disk_io.read_bytes / (1024 ** 2)
                disk_write_mb = disk_io.write_bytes / (1024 ** 2)
                
                # Calculate rates
                if prev_disk_read > 0:
                    disk_read_rate = (disk_read_mb - prev_disk_read) / interval
                    disk_write_rate = (disk_write_mb - prev_disk_write) / interval
                else:
                    disk_read_rate = 0
                    disk_write_rate = 0
                
                prev_disk_read = disk_read_mb
                prev_disk_write = disk_write_mb
                
                # Write to file
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"{timestamp},{elapsed:.1f},{cpu_percent:.1f},"
                       f"{mem_gb:.2f},{mem_percent:.1f},"
                       f"{disk_read_rate:.2f},{disk_write_rate:.2f}\n")
                f.flush()
                
                # Print to console
                print(f"\r[{elapsed:6.1f}s] CPU: {cpu_percent:5.1f}% | "
                      f"MEM: {mem_gb:5.2f}GB ({mem_percent:4.1f}%) | "
                      f"DISK R: {disk_read_rate:6.2f}MB/s | "
                      f"DISK W: {disk_write_rate:6.2f}MB/s", end='')
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\nMonitoring stopped by user")

def plot_monitoring_data(input_file, output_file):
    """
    Create plots from monitoring data
    
    Args:
        input_file: Input CSV file with monitoring data
        output_file: Output image file
    """
    try:
        import pandas as pd
        import matplotlib.pyplot as plt
    except ImportError:
        print("Error: pandas and matplotlib required for plotting")
        print("Install with: pip3 install pandas matplotlib")
        return
    
    # Read data
    df = pd.read_csv(input_file)
    
    # Create figure with 3 subplots
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 10), sharex=True)
    
    # Plot CPU
    ax1.plot(df['elapsed_sec'], df['cpu_percent'], 'b-', linewidth=1.5)
    ax1.set_ylabel('CPU Usage (%)', fontsize=12)
    ax1.set_title('Resource Utilization Over Time', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim([0, 100])
    
    # Plot Memory
    if 'mem_gb' in df.columns:
        ax2.plot(df['elapsed_sec'], df['mem_gb'], 'g-', linewidth=1.5)
        ax2.set_ylabel('Memory (GB)', fontsize=12)
    else:
        ax2.plot(df['elapsed_sec'], df['mem_percent'], 'g-', linewidth=1.5)
        ax2.set_ylabel('Memory (%)', fontsize=12)
    ax2.grid(True, alpha=0.3)
    
    # Plot Disk I/O
    ax3.plot(df['elapsed_sec'], df['disk_read_mb_s'], 'r-', linewidth=1.5, label='Read')
    ax3.plot(df['elapsed_sec'], df['disk_write_mb_s'], 'orange', linewidth=1.5, label='Write')
    ax3.set_xlabel('Time (seconds)', fontsize=12)
    ax3.set_ylabel('Disk I/O (MB/s)', fontsize=12)
    ax3.legend(loc='upper right')
    ax3.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\nPlot saved to {output_file}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Monitor resource usage')
    parser.add_argument('-p', '--pid', type=int, help='Process ID to monitor')
    parser.add_argument('-o', '--output', required=True, help='Output CSV file')
    parser.add_argument('-d', '--duration', type=int, help='Duration in seconds')
    parser.add_argument('-i', '--interval', type=float, default=1.0, 
                       help='Sampling interval (default: 1.0s)')
    parser.add_argument('--plot', help='Create plot from CSV file')
    
    args = parser.parse_args()
    
    if args.plot:
        plot_monitoring_data(args.plot, args.output)
    elif args.pid:
        monitor_process(args.pid, args.output, args.interval)
    else:
        monitor_system(args.output, args.duration, args.interval)