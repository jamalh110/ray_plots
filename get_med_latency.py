import sys
import pandas as pd

def main():
    if len(sys.argv) < 2:
        print("Usage: python get_med_latency.py <csv_file>")
        sys.exit(1)

    csv_file = sys.argv[1]
    try:
        data = pd.read_csv(csv_file)
    except Exception as e:
        print(f"Error reading {csv_file}: {e}")
        sys.exit(1)

    if 'latency' not in data.columns:
        print("CSV does not contain a 'latency' column.")
        sys.exit(1)

    median_latency = data['latency'].median()
    p95 = data['latency'].quantile(0.95)
    print("Median latency:", median_latency)
    print("95th percentile latency:", p95)
if __name__ == "__main__":
    main()

