from itertools import combinations
import pandas as pd
import sys
from operator import itemgetter

rows=[]
share_friends_sep={}

fr_gr = None

for row in sys.stdin:
    #removing whitespace at the beginning and end of row
    row = row.strip()
    #splitting based on the tab at each row
    fr_gr, count = row.split('\t', 1)
    count = int(count)
    #splitting two keys
    fr_gr=fr_gr.strip().split(",")
    #converting to int
    fr_gr=tuple(map(int,fr_gr))
    #appending to list
    rows.append([fr_gr, count])
#converting list to dataframe
df1=pd.DataFrame(rows, columns=["fr_gr","count"])
#grouping similar keys to sum the counts
df2=df1.groupby(df1["fr_gr"]).agg({"count":[sum]})
df2.columns=["count"]
df2=df2.reset_index()
#creating a dictionary for each single keys that stores the number of share friends with another key
for fr in df2["fr_gr"]:
    if fr[0] not in share_friends_sep.keys():
        share_friends_sep[fr[0]]=[]
    if fr[1] not in share_friends_sep.keys():
        share_friends_sep[fr[1]]=[]
#repeating saving for each member of pair fr[0] and fr[1]
    share_friends_sep[fr[0]].append((fr[1],int(df2[df2["fr_gr"]==fr]["count"])))
    share_friends_sep[fr[1]].append((fr[0],int(df2[df2["fr_gr"]==fr]["count"])))

#sorting the list of each key based on the number of shared friend
for p in share_friends_sep.keys():
    share_friends_sep[p].sort(key=lambda x:x[1], reverse=True)
#recommending 10 top friend 
for p in [0,1, 2,3]:
    if p in share_friends_sep.keys():
        print(p, '\t',[fr_suggest[0] for fr_suggest in share_friends_sep[p][:10]])
    else:
      print(p,'\t', "No recommendation")
