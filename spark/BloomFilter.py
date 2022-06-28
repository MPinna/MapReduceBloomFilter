import json

# https://pypi.org/project/bitarray/
from bitarray import bitarray

# https://pypi.org/project/mmh3/
import mmh3

class BloomFilter:
    def __init__(self, rating: int=0, m: int=0, K: int=0, p: float=0.0, json_string: str=None):
        if json_string is not None:
            obj = json.loads(json_string)
            self.rating = obj["rating"]
            self.m = obj["m"]
            self.K = obj["K"]
            self.P = obj["P"]
            self.bitArray = bitarray(obj["bitarray"])
        else:
            self.rating = rating
            self.m = m
            self.K = K
            self.P = p
            self.bitArray = m*bitarray('0')
        
    def add(self, movieId: str):
        hashIndexes = self.computeHash(self.K, movieId, self.m)
        for i in range(self.K):
            self.bitArray[hashIndexes[i]] = True


    def test(self, movieId: str):
        hashIndexes = self.computeHash(self.K, movieId, self.m)
        for i in range(self.K):
            if not self.bitArray[hashIndexes[i]]:
                return False
        return True

    @staticmethod
    def computeHash(k: int, movieId: str, m: int):
        hashIndexes = [0 for i in range(k)]
        # print(f"HashIndexes (Before): {hashIndexes}")
        for i in range(k):
            hashIndexes[i] = mmh3.hash(movieId, i)
            hashIndexes[i] %= m
            hashIndexes[i] = abs(hashIndexes[i])
        # print(f"HashIndexes (Then): {hashIndexes}")

        return hashIndexes

    def setAt(self, index: int):
        self.bitArray[index] = True             
        
    def setBitArray(self, bitArray: bitarray):
        self.bitArray = bitArray

    def orBloomFilter(self, that: 'BloomFilter'):
        self.bitArray = self.bitArray or that.bitArray
        return self

    def clear(self):
        self.bitArray.setall(False)
    
    def __str__(self):
        obj = {}
        obj["rating"] = self.rating
        obj["m"] = self.m
        obj["K"] = self.K
        obj["P"] = self.P
        obj["bitarray"] = self.bitArray.to01()
        return json.dumps(obj)
    