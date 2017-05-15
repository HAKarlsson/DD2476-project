##
# Usage: python sample.py dataset fraction > sampled.txt
# 		 dataset - the name of the file to sample from, string
#		 fraction - rough fraction of the file to be sampled, float
# Example: python sample.py test.txt 0.4 > sampled.txt
# 		   Means that you want to sample roughly 40% of test.txt
##

import random
import sys
import logging
import time


logging.basicConfig(level=logging.DEBUG, format="%(asctime)-15s %(message)s")


def log_info(message=''):
    elapsed = time.time() - start_time
    # lines per second
    lps = lines_read / elapsed
    logging.debug("{}Processed {} lines, {:.2f} lps".format(message + ' ', lines_read, lps))

if __name__ == '__main__':
	dataset = sys.argv[1]
	sample_fraction = float(sys.argv[2])

	sampling = True

	start_time = time.time()
	with open(dataset) as f:
	    for lines_read, line in enumerate(f):
	        if line.split('\t')[1] == 'M':
	            sampling = random.random() > (1 - sample_fraction)

	        if sampling:
	            print(line, end='')
	        if lines_read % 50000 == 0 and lines_read > 0:
	        	log_info()
	    log_info()
