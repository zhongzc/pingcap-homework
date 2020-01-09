import random

a = list(range(0, 10000000))
random.shuffle(a)

with open('test-data.dat', 'wb') as f:
  for i in range(10000000):
    num = a[i]
    k = bytes(str(num), 'utf-8')
    l = len(k).to_bytes(4, 'big')
    f.write(l)
    f.write(k)
    f.write(l)
    f.write(k)
