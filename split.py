#
#	Splits the training data into four parts.
#	dataset/train -> dataset/train[0-3]

file_num = 0
writeFile = open('dataset/train%d'%file_num, 'w')
lines_read = 0
lines_in_file = 0
with open('dataset/train') as fp:
	for line in fp:
		if lines_in_file > 43000000:
			record = line.split()
			if record[1] == 'M':
				print("%d lines in file%d " % (lines_in_file, file_num))
				writeFile.close()
				file_num += 1
				writeFile = open('dataset/train%d'%file_num, 'w')
				lines_in_file = 0
		writeFile.write(line)
		lines_read += 1
		lines_in_file += 1

		if lines_read % 1000000 == 0:
			print("%d lines copied " % lines_read)
print("%d lines copied " % lines_read)
writeFile.close()