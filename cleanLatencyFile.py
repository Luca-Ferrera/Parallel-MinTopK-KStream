import sys
with open(sys.argv[1], "r") as f:
    lines = f.readlines() if sys.argv[2] == "-1" else f.readlines()[int(sys.argv[2])-1::int(sys.argv[2])]
if sys.argv[2] == "-1":
    printingLines = []
    for i in range(len(lines) - 1):
        if lines[i].split()[2] != lines[i+1].split()[2] :
            printingLines.append(lines[i])
    printingLines.append(lines[len(lines) - 1])
    with open(sys.argv[1], "w") as f:
        for line in printingLines:
            f.write(line)
else:
    with open(sys.argv[1], "w") as f:
        for line in lines:
            f.write(line)