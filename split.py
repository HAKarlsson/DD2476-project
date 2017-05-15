#
#	Splits the training data into four parts.
#	dataset/train -> dataset/train[0-3]
#	Example:
#       python split.py path/to/train train
#       parts_prefix - the basename for parts file, will be parts_prefix%d
#

import os

import sys

file_to_split = sys.argv[1]
parts_prefix = sys.argv[2]

file_num = 0
path = 'dataset/train_parts/%s%d'
if not os.path.exists(os.path.dirname(path)):
	os.makedirs(os.path.dirname(path))
writeFile = open(path % (parts_prefix, file_num), 'w')

lines_read = 0
lines_in_file = 0
with open(file_to_split) as fp:
	for line in fp:
		if lines_in_file > 43000000:
			record = line.split()
			if record[1] == 'M':
				print("%d lines in %s%d " % (lines_in_file, parts_prefix, file_num))
				writeFile.close()
				file_num += 1
				writeFile = open(path%(parts_prefix, file_num), 'w')
				lines_in_file = 0
		writeFile.write(line)
		lines_read += 1
		lines_in_file += 1

		if lines_read % 1000000 == 0:
			print("%d lines copied " % lines_read)
print("%d lines copied " % lines_read)
writeFile.close()
