#!/usr/bin/env python
"""
This Bloom Filter implementation is based on
https://gist.github.com/josephkern/2897618.
"""

import GeneralHashFunctions as hf

class BloomFilter(object):
    """A simple bloom filter for lots of int()"""

    def __init__(self, bitSize=2048, hashNum=2):
        """Initializes a BloomFilter() object:
            Expects:
                bitSize:  filter size in bits
                hashNum (int): for the number of hashes to perform"""
        self.bitSize = bitSize
        byteSize = bitSize >> 3
        self.filter  = bytearray(byteSize)    # The filter itself
        self.hashNum = hashNum                # The number of hashes to use
        
    def getByteSize(self):
        return len((self.filter)) 

    def _hash(self, value):
        """Creates a hash of an int and yields a generator of hash functions
        Expects:
            value: int()
        Yields:
            generator of ints()"""

        # Build an int() around the sha256 digest of int() -> value
        value = value.__str__() # Comment out line if you're filtering strings()
        for i in range(self.hashNum):
            digest = hf.hashFunctions[i](value)
            # bitwise AND of the digest and all of the available bit positions 
            # in the filter
            yield (digest & (self.bitSize - 1))
            

    def add(self, value):
        """Bitwise OR to add value(s) into the self.filter
        Expects:
            value: generator of digest ints()
        """
        for digest in self._hash(value):
            self.filter[(digest >> 3)] |= (1 << (digest & 7))


    def query(self, value):
        """Bitwise AND to query values in self.filter
        Expects:
            value: value to check filter against (assumed int())"""
        # If all() hashes return True from a bitwise AND (the opposite 
        # described above in self.add()) for each digest returned from 
        # self._hash return True, else False
        return all(self.filter[(digest >> 3)] & (1 << (digest & 7)) 
            for digest in self._hash(value))
        
    def resetBf(self):
        for i in range(len((self.filter))):
            self.filter[i] &= 0x00
        
    def displayFilter(self):
        print("BF byte number: %d" % len((self.filter)))
        for i in range(len((self.filter))):
            print((self.filter)[i], end="")
        print()


if __name__ == "__main__":
    bf = BloomFilter(512, 2)
    bf.displayFilter()
    
    bf.add(30000)
    print("add 30000")
    bf.displayFilter()
    bf.add(1230213)
    print("add 1230213")
    bf.displayFilter()
    bf.add(28)
    print("add 28")
    bf.displayFilter()
    bf.add(1)
    print("add 1")
    bf.displayFilter()
    print("Filter size {0} bytes".format((bf.filter).__sizeof__()))
    print(bf.query(1)) # True
    print(bf.query(1230213)) # True
    print(bf.query(12)) # False
    bf.resetBf()
    bf.displayFilter()
