#sort the file by value(component,community...) first then vertexid
import json
import sys

filename = sys.argv[1]
outputname = sys.argv[2] if len(sys.argv) > 2 else filename

with open(filename,'r') as file:
	lines=file.read().split('\n')
	lists = [json.loads(l) for l in lines if l != '']
	lists.sort(key = lambda l : (l[1],l[0]))
with open(outputname,'w') as file:
	text=""
	for i,l in enumerate(lists):
		text+=str(l)
		if i+1  != len(lists):
			text+='\n'
	file.write(text)