import csv
from collections import Counter

response_counts = Counter()

with open('NASA_access_log_Jul95.csv', encoding='utf-8', errors='replace') as f:
    reader = csv.DictReader(f)
    for row in reader:
        response_counts[row.get('response', '').strip()] += 1

print("Response code breakdown:")
for code, count in sorted(response_counts.items(), key=lambda x: -x[1]):
    print(f"  {code}: {count}")