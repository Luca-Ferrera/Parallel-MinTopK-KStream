import sys
with open(sys.argv[1], "r") as f:
    lines = f.readlines()
for i in range(len(lines)-1):
    if int(lines[i].split()[2])- int(lines[i+1].split()[2]) != -1 and int(lines[i].split()[2]) - int(lines[i+1].split()[2]) != 0 :
        print("Not Ordered " + lines[i].split()[2] + " " + lines[i+1].split()[2])
        break


