#!/usr/bin/env python3
# Log Analysis and Chart Generation Tool
# This script processes log files and generates PNG charts for latency and throughput

import os
import re
import glob
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict

# Configuration
#LOG_TYPES = ['Ingress', 'StepA', 'StepB', 'StepD', 'StepE']
LOG_TYPES = ['Ingress', 'StepAudio', 'StepSearch', 'StepToxCheck', 'StepTTS']
#LOG_TYPES = ['Ingress_Mono', 'Monolith']
#LOG_TYPES = ['Monolith']
LOG_DIR = './logs'
OUTPUT_DIR = './charts'

# Define colors for each step type
COLORS = {
    'Monolith': '#82ca9d',
    'Ingress': '#8884d8',
    'Ingress_Mono': '#8884d8',
    'StepA': '#82ca9d',
    'StepAudio': '#82ca9d',
    'StepB': '#ffc658',
    'StepSearch': '#ffc658',
    'StepD': '#ff8042',
    'StepToxCheck': '#ff8042',
    'StepE': '#0088fe',
    'StepTTS': '#0088fe'
}

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Regex patterns for log lines
PATTERN_WITH_TIMESTAMP = re.compile(
    r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - INFO - (\w+)_(\w+) (\w+)$'
)
PATTERN_WITHOUT_TIMESTAMP = re.compile(r'^(\w+)_(\w+) (\w+)$')


def find_log_files(directory):
    """
    Recursively find all log files in the directory that contain any of the log types
    """
    matched_files = []
    
    # Walk through the directory
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            
            # Check if the file name contains any of the log types
            if any(log_type in file for log_type in LOG_TYPES):
                matched_files.append(file_path)
                continue
            
            # If not in filename, check file content (first few lines)
            try:
                with open(file_path, 'r') as f:
                    # Read first 10 lines to check for log types
                    first_lines = ''.join([f.readline() for _ in range(10)])
                    if any(log_type in first_lines for log_type in LOG_TYPES):
                        matched_files.append(file_path)
            except (UnicodeDecodeError, IOError):
                # Skip files that can't be read as text
                pass
    
    return matched_files


def parse_log_entries(file_path):
    """
    Parse log entries from a file and return structured data
    """
    entries = []
    last_timestamp = None
    
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            # Try pattern with timestamp
            match = PATTERN_WITH_TIMESTAMP.match(line)
            if match:
                timestamp, step, action, id_val = match.groups()
                last_timestamp = timestamp
                entries.append({
                    'timestamp': timestamp,
                    'step': step,
                    'action': action,
                    'id': id_val
                })
                continue
            
            # Try pattern without timestamp
            match = PATTERN_WITHOUT_TIMESTAMP.match(line)
            if match and last_timestamp:
                step, action, id_val = match.groups()
                entries.append({
                    'timestamp': last_timestamp,  # Use the last known timestamp
                    'step': step,
                    'action': action,
                    'id': id_val
                })
    
    return entries


def calculate_latencies(entries):
    """
    Calculate latencies by matching Enter/Exit events
    """
    enter_events = {}
    latencies = []
    
    for entry in entries:
        timestamp = entry['timestamp']
        step = entry['step']
        action = entry['action']
        id_val = entry['id']
        
        # Convert timestamp to datetime
        dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S,%f')
        time_ms = dt.timestamp() * 1000  # Convert to milliseconds
        
        if action == 'Enter':
            enter_events[id_val] = {'timestamp': time_ms, 'type': step}
        elif action == 'Exit' and id_val in enter_events:
            enter_time = enter_events[id_val]['timestamp']
            latency = time_ms - enter_time
            
            latencies.append({
                'id': id_val,
                'step': enter_events[id_val]['type'],
                'enter_time': enter_time,
                'exit_time': time_ms,
                'latency': latency,  # In milliseconds
                'enter_dt': datetime.fromtimestamp(enter_time / 1000)
            })
            
            del enter_events[id_val]
    
    # Sort by enter time
    return sorted(latencies, key=lambda x: x['enter_time'])


def calculate_throughput(latencies):
    """
    Calculate throughput (requests per second)
    """
    if not latencies:
        return 0
    
    start_time = min(l['enter_time'] for l in latencies)
    end_time = max(l['exit_time'] for l in latencies)
    total_time_seconds = (end_time - start_time) / 1000
    
    # Avoid division by zero
    if total_time_seconds <= 0:
        return 0
    
    return len(latencies) / total_time_seconds


def create_latency_chart(latencies, step_type):
    """
    Create a line chart showing latency over time for a specific step type
    """
    if not latencies:
        print(f"No latency data for {step_type}")
        return
    
    # Convert to DataFrame for easier plotting
    df = pd.DataFrame(latencies)
    
    plt.figure(figsize=(12, 7))
    plt.plot(df['enter_dt'], df['latency'], marker='o', linestyle='-', color=COLORS[step_type], alpha=0.7)
    
    plt.title(f'{step_type} Request Latency Over Time', fontsize=16)
    plt.xlabel('Time', fontsize=12)
    plt.ylabel('Latency (ms)', fontsize=12)
    plt.grid(True, alpha=0.3)
    
    # Format x-axis to show time
    plt.gcf().autofmt_xdate()
    
    # Add statistics to the chart
    avg_latency = np.mean(df['latency'])
    plt.axhline(y=avg_latency, color='r', linestyle='--', alpha=0.7)
    plt.text(
        df['enter_dt'].iloc[0], 
        avg_latency * 1.1, 
        f'Avg: {avg_latency:.2f} ms', 
        fontsize=10, 
        color='r'
    )
    
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, f'{step_type}_latency.png'), dpi=300)
    plt.close()
    
    print(f"Created {step_type} latency chart")


def create_throughput_chart(results):
    """
    Create a bar chart comparing throughput across step types
    """
    step_types = list(results.keys())
    throughputs = [results[t]['throughput'] for t in step_types]
    colors = [COLORS[t] for t in step_types]
    
    plt.figure(figsize=(12, 7))
    bars = plt.bar(step_types, throughputs, color=colors)
    
    plt.title('Throughput Comparison by Step Type', fontsize=16)
    plt.xlabel('Step Type', fontsize=12)
    plt.ylabel('Throughput (requests/second)', fontsize=12)
    plt.grid(True, axis='y', alpha=0.3)
    
    # Add value labels on top of bars
    for bar in bars:
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width()/2., 
            height * 1.01,
            f'{height:.2f}',
            ha='center', 
            va='bottom', 
            fontsize=10
        )
    
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, 'throughput_comparison.png'), dpi=300)
    plt.close()
    
    print("Created throughput comparison chart")


def create_avg_latency_chart(results):
    """
    Create a bar chart comparing average latency across step types
    """
    step_types = list(results.keys())
    avg_latencies = [results[t]['avg_latency'] for t in step_types]
    colors = [COLORS[t] for t in step_types]
    
    plt.figure(figsize=(12, 7))
    bars = plt.bar(step_types, avg_latencies, color=colors)
    
    plt.title('Average Latency Comparison by Step Type', fontsize=16)
    plt.xlabel('Step Type', fontsize=12)
    plt.ylabel('Average Latency (ms)', fontsize=12)
    plt.grid(True, axis='y', alpha=0.3)
    
    # Add value labels on top of bars
    for bar in bars:
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width()/2., 
            height * 1.01,
            f'{height:.2f}',
            ha='center', 
            va='bottom', 
            fontsize=10
        )
    
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, 'avg_latency_comparison.png'), dpi=300)
    plt.close()
    
    print("Created average latency comparison chart")


def create_latency_histogram(latencies_by_type):
    """
    Create histograms of latency distribution for each step type
    """
    plt.figure(figsize=(12, 10))
    
    # Determine the number of subplots needed
    types_with_data = [t for t in LOG_TYPES if latencies_by_type[t]]
    n_plots = len(types_with_data)
    
    if n_plots == 0:
        print("No data for latency histograms")
        return
    
    # Create subplot grid
    rows = (n_plots + 1) // 2  # Ceiling division
    cols = min(2, n_plots)
    
    for i, step_type in enumerate(types_with_data):
        latencies = [l['latency'] for l in latencies_by_type[step_type]]
        
        plt.subplot(rows, cols, i + 1)
        plt.hist(latencies, bins=20, color=COLORS[step_type], alpha=0.7)
        
        plt.title(f'{step_type} Latency Distribution')
        plt.xlabel('Latency (ms)')
        plt.ylabel('Frequency')
        plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, 'latency_histograms.png'), dpi=300)
    plt.close()
    
    print("Created latency histogram chart")


def main():
    """
    Main function to analyze logs and generate charts
    """
    print('Finding log files...')
    log_files = find_log_files(LOG_DIR)
    print(f'Found {len(log_files)} log files')
    
    # Extract all log entries
    all_entries = []
    for file in log_files:
        entries = parse_log_entries(file)
        all_entries.extend(entries)
    print(f'Extracted {len(all_entries)} log entries')
    
    # Group entries by step type
    entries_by_type = defaultdict(list)
    for entry in all_entries:
        entries_by_type[entry['step']].append(entry)
    
    # Calculate latencies for each type
    latencies_by_type = {}
    results = {}
    
    for type_name in LOG_TYPES:
        entries = entries_by_type[type_name]
        latencies = calculate_latencies(entries)
        latencies_by_type[type_name] = latencies
        
        if latencies:
            avg_latency = sum(l['latency'] for l in latencies) / len(latencies)
        else:
            avg_latency = 0
        
        results[type_name] = {
            'count': len(latencies),
            'avg_latency': avg_latency,
            'throughput': calculate_throughput(latencies)
        }
    
    # Print summary
    print('\nSummary:')
    for type_name in LOG_TYPES:
        print(f'{type_name}:')
        print(f'  Total requests: {results[type_name]["count"]}')
        print(f'  Average latency: {results[type_name]["avg_latency"]:.2f} ms')
        print(f'  Throughput: {results[type_name]["throughput"]:.2f} req/sec')
    
    # Generate charts
    print('\nGenerating charts...')
    
    # Create latency charts for each step type
    for type_name in LOG_TYPES:
        create_latency_chart(latencies_by_type[type_name], type_name)
    
    # Create comparison charts
    create_throughput_chart(results)
    create_avg_latency_chart(results)
    create_latency_histogram(latencies_by_type)
    
    # Save the analysis results as CSV
    print('\nSaving analysis results...')
    
    # Save latency data as CSV files
    for type_name in LOG_TYPES:
        if latencies_by_type[type_name]:
            df = pd.DataFrame(latencies_by_type[type_name])
            df['enter_time_str'] = df['enter_dt'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
            df[['id', 'enter_time_str', 'latency']].to_csv(
                os.path.join(OUTPUT_DIR, f'{type_name}_latencies.csv'),
                index=False
            )
    
    # Save summary data
    summary_df = pd.DataFrame([
        {
            'Step': step,
            'TotalRequests': data['count'],
            'AvgLatency_ms': data['avg_latency'],
            'Throughput_reqPerSec': data['throughput']
        }
        for step, data in results.items()
    ])
    
    summary_df.to_csv(os.path.join(OUTPUT_DIR, 'summary.csv'), index=False)
    
    print(f'\nAll outputs have been saved to the {OUTPUT_DIR} directory')


if __name__ == '__main__':
    main()