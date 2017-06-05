#!/usr/bin/env python

import numpy as np

path = 'numpy/src/test/resources/'

a1d = range(0, 10)
np.save(open(path + 'a1d-int.npy', 'w'), np.array(a1d, np.int32))
np.save(open(path + 'a1d-long.npy', 'w'), np.array(a1d, np.int64))
np.save(open(path + 'a1d-float.npy', 'w'), np.array(a1d, np.float32))
np.save(open(path + 'a1d-double.npy', 'w'), np.array(a1d, np.float64))

a2d = [[i * 10 + j for j in range(0, 5)] for i in range(0, 10)]
np.save(open(path + 'a2d-int.npy', 'w'), np.array(a2d, np.int32))
np.save(open(path + 'a2d-long.npy', 'w'), np.array(a2d, np.int64))
np.save(open(path + 'a2d-float.npy', 'w'), np.array(a2d, np.float32))
np.save(open(path + 'a2d-double.npy', 'w'), np.array(a2d, np.float64))
