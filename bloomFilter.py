"""
Author: Zhou You
Module: multiple bloom filters for hot/cold data idenfication

"""

import bloomFilter as bf
import sys
import gl
#import random

PRINT_ENABLE = 0

class multiBloomFilter:
    
    def __init__(self, bfNum=4, maxWeight=2, hotThres=4, dcWindowSize=512):
        """
        bfNum:     number of bloom filters.
        maxWeight: the weight of currently selected bloom filter.
                   Weights of other bloom filters are subtracted step by step,
                   i.e. maxWeight/bfNum, according to recency.
        hotThres:  the threshold weight of hot data (larger than T).
        decay:     After every N requests, the oldest bloom filter (previous 
                   to currently selected BF) is reset as 0s.
        """
        self.bfNum        = bfNum
        self.maxWeight    = maxWeight
        self.hotThres     = hotThres
        self.decayWindow  = dcWindowSize
        self.bloomfilters = []
        self.curBf        = 0
        self.decBf        = 0
        # weights are scaled up
        # when check hot, weight should be divided by 
        # (self.bfNum / self.maxWeight)
        self.weights      = [ (i + 1) for i in range(bfNum) ]
        self.reqNum       = 0
        self.keySet       = {}
        self.hotKeySet    = {}
        if PRINT_ENABLE:
            print("### Initial weights", end="")
            print(self.weights)
            
    def initBloomFilter(self, bitSize=2048, hashNum=2):
        for i in range(self.bfNum):
            self.bloomfilters.append(bf.BloomFilter(bitSize, hashNum))
            
    def decayBf(self):
        # reset weights            
        for i in range(self.bfNum):
            self.weights[i] -= 1
            if self.weights[i] == 0:
                if i != self.decBf:
                    print("Error in decay BF weight.")
                    sys.exit()
                self.weights[i] = self.bfNum
        if PRINT_ENABLE:
            print("### After decay, weights", end="")
            print(self.weights)
        # reset decayBF 
        self.bloomfilters[self.decBf].resetBf()
        # set next decayBF
        self.decBf = (self.bfNum + self.decBf + 1)  % self.bfNum
        if self.weights[self.decBf] != 1:
            print("Error in setting decay BF.")
            sys.exit()
                    
    def countWeight(self, key):
        w = 0.0
        for i in range(self.bfNum):
            if self.bloomfilters[i].query(key):
                w += self.weights[i]
        return (w * self.maxWeight / self.bfNum)
        
    def handleReq(self, key):
        self.reqNum += 1
        self.keySet[key] = gl.VALID
        # if performing decay
        if self.reqNum % self.decayWindow == 0:
            self.decayBf()
        # insert KEY to a BF: if hit, find next to insert
        if self.bloomfilters[self.curBf].query(key):
            nextBf = (self.curBf + 1) % self.bfNum
            while nextBf != self.curBf:
                if self.bloomfilters[nextBf].query(key):
                    nextBf = (nextBf + 1) % self.bfNum
                else:
                    self.bloomfilters[nextBf].add(key)
                    break
        else:
            self.bloomfilters[self.curBf].add(key)
        # set cur BF
        self.curBf = (self.curBf + 1)  % self.bfNum
		
    def checkHot(self, key):
        if self.countWeight(key) >= self.hotThres:
            self.hotKeySet[key] = gl.HOT
            if PRINT_ENABLE:
                print("-----hot-------")
                self.printHitBf(key)
                print("---------------")
            return gl.HOT
        else:
            return gl.COLD
            
    def printHitBf(self, key):
         for i in range(self.bfNum):
            if self.bloomfilters[i].query(key):
                print("    %d in bloomfilters[%d]" % (key, i))

    def displayKey(self):
        print("Number of unique keys:     %d" % len(self.keySet))
        print("Number of unique hot keys: %d" % len(self.hotKeySet))
        print("Hot percent: %.1f%%" % (100 * len(self.hotKeySet) /\
                                       len(self.keySet)))
            
    def displayBf(self, index):
        self.bloomfilters[index].displayFilter()
        
    def setBfTest(self, fIndex, bIndex):
        self.bloomfilters[fIndex].filter[bIndex] = 1

if __name__ == "__main__":
    mBFs = multiBloomFilter(4, 2, 4, 8)
    mBFs.initBloomFilter(32,2)
    testLpn = [6, 20, 5, 9, \
               22, 3, 3, 11,\
               1, 3, 0, 5,  \
               4, 2, 1, 0,  \
               3, 3, 3, 4,  \
               19, 1, 26, 6,\
               5, 6, 5, 7,  \
               7, 26, 6, 5]
    """
    for _ in range(32):
        if random.choice(range(100)) < 80:
            testLpn.append(random.choice(range(8)))
        else:
            testLpn.append(random.choice(range(8, 32)))
    """
    if PRINT_ENABLE:
        print("TEST LPNs:")
        print(testLpn)
    for key in testLpn:
        mBFs.handleReq(key)
        mBFs.checkHot(key)
        
    mBFs.displayKey()

            
            
            
            
            
            
            
            
            
            
            
