import sys
with open(sys.argv[1], "r") as f:
    lines = f.readlines()[1::2]
with open(sys.argv[1], "w") as f:
    for line in lines:
        f.write(line)