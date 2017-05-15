import random
import re

METADATA_REG = r'.*\tM\t.*'
sampling = True

with open('test') as f:
    for line in f:
        if re.search(METADATA_REG, line):
            sampling = random.random() > 0.5

        if sampling:
            print(line, end='')

        
