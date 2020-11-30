#Convert from http://snap.stanford.edu/mappr/data.html format
#to JsonLongLongDoubleTextInputFormat

import sys

filename = sys.argv[1]
outputname = sys.argv[2] if len(sys.argv) > 2 else filename+"_conv.txt"
nodes = {}

with open(filename, 'r') as file:
	lines = file.read().split('\n')
	for l in lines:
		pair = l.split(' ')
		if len(pair) == 2:
			pair = [int(i) for i in pair]
			left, right = pair[0], pair[1]
			if left in nodes:
				nodes[left].append(right)
			else:
				nodes[left] = [right]
with open(outputname,'w') as file:
	text=""
	for n,edges in nodes.items():
		line = "["+str(n)+", 0, ["
		for i,e in enumerate(edges):
			line+="["+str(e)+", "+"1.0]"
			if i != len(edges)-1:
				line+=", "
		line+="]]\n"
		text+=line
	file.write(text)

