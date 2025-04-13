import sys
import pandas as pd
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

#function that takes in a dataframe, finds how many rows there are with identical timestaps, and returns a new dataframe with the timestamp and the number of rows
def find_identical_timestamps(dataframe):
    # Create a new DataFrame to store the results
    result_df = pd.DataFrame(columns=['enter_time_str', 'count'])

    # Group by timestamp and count occurrences
    grouped = dataframe.groupby('enter_time_str').size().reset_index(name='count')

    # Filter out groups with more than one occurrence
    #identical_timestamps = grouped[grouped['count'] > 1]

    # Add the identical timestamps to the result DataFrame
    result_df = pd.concat([result_df, grouped], ignore_index=True)

    return result_df

def plot_identical_timestamps(dataframe, stepName, dir):
    # Convert string timestamps to datetime for better x-axis formatting
    # Assuming the format is something like '2023-01-01 12:34:56'
    try:
        dataframe['datetime'] = pd.to_datetime(dataframe['enter_time_str'])
    except:
        # If conversion fails, use the strings directly
        dataframe['datetime'] = dataframe['enter_time_str']
    
    # Sort by datetime
    dataframe = dataframe.sort_values('datetime')
    
    # Create the plot
    plt.figure(figsize=(12, 6))
    plt.plot(dataframe['datetime'], dataframe['count'], marker='o', linestyle='none')
    
    # Formatting
    plt.title(f'Count of Identical Timestamps for {stepName}')
    plt.xlabel('Enter Time')
    plt.ylabel('Count')
    plt.grid(True, alpha=0.3)
    
    # Set y-axis to show only integers with step size of 1
    y_min = int(min(dataframe['count']))
    y_max = int(max(dataframe['count'])) + 1  # Add 1 to include the max value
    plt.yticks(range(y_min, y_max))

    # Format x-axis to show dates nicely
    if pd.api.types.is_datetime64_any_dtype(dataframe['datetime']):
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
        plt.gcf().autofmt_xdate()  # Rotate date labels
    
    # Save the plot
    plt.tight_layout()
    plt.savefig(os.path.join(dir,f'{stepName}_batch_plot.png'))
    plt.close()

def main():
    if len(sys.argv) < 2:
        print("Usage: python get_med_latency.py <csv_file>")
        sys.exit(1)

    dirname = os.path.dirname(sys.argv[1])
    ingress_file = os.path.join(dirname, 'Ingress_latencies.csv')
    stepa_file = os.path.join(dirname, 'StepA_latencies.csv')
    stepb_file = os.path.join(dirname, 'StepB_latencies.csv')
    stepd_file = os.path.join(dirname, 'StepD_latencies.csv')
    stepe_file = os.path.join(dirname, 'StepE_latencies.csv')

    try:
        data_ingress = pd.read_csv(ingress_file)
        data_stepa = pd.read_csv(stepa_file)
        data_stepb = pd.read_csv(stepb_file)
        data_stepd = pd.read_csv(stepd_file)
        data_stepe = pd.read_csv(stepe_file)
    except Exception as e:
        print(f"Error reading file: {e}")
        sys.exit(1)

    # if 'latency' not in data.columns:
    #     print("CSV does not contain a 'latency' column.")
    #     sys.exit(1)
    # Find identical timestamps in each DataFrame
    identical_ingress = find_identical_timestamps(data_ingress)
    identical_stepa = find_identical_timestamps(data_stepa)
    identical_stepb = find_identical_timestamps(data_stepb)
    identical_stepd = find_identical_timestamps(data_stepd)
    identical_stepe = find_identical_timestamps(data_stepe)
    # Save the results to CSV files
    identical_ingress.to_csv(os.path.join(dirname,'identical_ingress.csv'), index=False)
    identical_stepa.to_csv(os.path.join(dirname,'identical_stepa.csv'), index=False)
    identical_stepb.to_csv(os.path.join(dirname,'identical_stepb.csv'), index=False)
    identical_stepd.to_csv(os.path.join(dirname,'identical_stepd.csv'), index=False)
    identical_stepe.to_csv(os.path.join(dirname,'identical_stepe.csv'), index=False)

    # generate plots for the data
    plot_identical_timestamps(identical_ingress, 'Ingress', dirname)
    plot_identical_timestamps(identical_stepa, 'StepA', dirname)
    plot_identical_timestamps(identical_stepb, 'StepB', dirname)
    plot_identical_timestamps(identical_stepd, 'StepD', dirname)
    plot_identical_timestamps(identical_stepe, 'StepE', dirname)

if __name__ == "__main__":
    main()

