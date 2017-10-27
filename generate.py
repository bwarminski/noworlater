# Generates random numbers on the exponential curve to feed into TimestampLoadTest
from random import expovariate
while True:
    r = int(expovariate(1.0/60000))
    print(r)
