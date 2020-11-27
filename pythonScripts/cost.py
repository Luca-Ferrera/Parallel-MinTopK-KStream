import sys
from math import log2

def cost(instance, size, k):
    cen_cost = size * log2(size)
    dis_cost =  size/instance *log2(size/instance) - instance*k*log2(instance*k)-instance*k
    return (cen_cost-dis_cost, cen_cost, dis_cost)

def main():
    costs = cost(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]))
    print("Gain cost: {:f} ".format(costs[0]))
    print("Centralized cost: {:f} ".format(costs[1]))
    print("Distributed cost: {:f} ".format(costs[2]))


if __name__ == "__main__":
    main()
