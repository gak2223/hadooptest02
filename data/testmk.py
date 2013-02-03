# -*- coding: utf-8 -*-

import random

max = 1000000

f = open('testdata.txt', 'w')
[f.write( 'A' +  str(x).zfill(9) + ', B' + str(random.randint(0, 10)) + ', ' + str(random.randint(0, max)) + ', ' + str(random.randint(0, max)) + '\n') for x in range(10000000)]
f.close()