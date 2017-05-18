import sys

feature_file = sys.argv[1]
score_file = sys.argv[2]
kaggle_file = sys.argv[3]

sessions = dict()
with open(feature_file,'r') as f:
	for line in f:
		tokens = line.strip().split()
		url = int(tokens[-1])
		session = int(tokens[1].split(":")[1])
		if session not in sessions.keys():
			sessions[session] = []
		sessions[session].append([url,0])

with open(score_file,'r') as f:
	for line in f:
		tokens = line.strip().split("\t")
		session = int(tokens[0])
		pos = int(tokens[1])
		score = float(tokens[2])
		sessions[session][pos][1] = score


with open(kaggle_file, 'w') as f:
	f.write("SessionID,URLID\n")
	for session, docs in sessions.items():
		docs.sort(key=lambda x: -x[1])
		for doc in docs:
			f.write("%d,%d\n"%(session, doc[0]))