import sys
with open(sys.argv[1], "r") as f:
    lines = f.readlines() if sys.argv[2] == "-1" else f.readlines()[int(sys.argv[2])-1::int(sys.argv[2])]
with open(sys.argv[1], "w") as f:
    for line in lines:
        f.write(line)