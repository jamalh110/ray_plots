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
    length = len(data['latency'])
    print("Number of records:", length)
    warmup_num = 50
    median_latency_after_warmup = data['latency'][warmup_num:].median()
    p95_latency_after_warmup = data['latency'][warmup_num:].quantile(0.95)
    print("Median latency:", median_latency)
    print("95th percentile latency:", p95)
    #print("Median latency after warmup:", median_latency_after_warmup)
    #print("95th percentile latency after warmup:", p95_latency_after_warmup)
if __name__ == "__main__":
    main()

