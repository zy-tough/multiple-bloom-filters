# multiple-bloom-filters
[Python] This is a simple implementation of multiple bloom filters, which can be used for hot/cold data identification. More details can be found in paper Hot Data Identification for Flash-based Storage Systems Using Multiple Bloom Filters.

Please find hash functions in GeneralHashFunctions.py on https://github.com/JamzyWang/HashCollector/blob/master/GeneralHashFunctions_Python/GeneralHashFunctions.py.

When using the multiple bloom filter, please add hashFunctions = [RSHash, DJBHash, FNVHash, SDBMHash] at the end of GeneralHashFunctions.py.

The bloom filter implementation bloom.py is based on https://gist.github.com/josephkern/2897618. I just made small changes, such as the hash functions and new functions.

