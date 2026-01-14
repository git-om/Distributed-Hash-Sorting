# scripts/make_hadoop_input.py
import sys

if len(sys.argv) != 4:
    print("Usage: make_hadoop_input.py <k> <num_mappers> <output_local_file>")
    sys.exit(1)

k = int(sys.argv[1])
num_mappers = int(sys.argv[2])
out = sys.argv[3]

total = 1 << k
base = total // num_mappers
extra = total % num_mappers

with open(out, "w") as f:
    for i in range(num_mappers):
        n = base + (1 if i < extra else 0)
        f.write(str(n) + "\n")
