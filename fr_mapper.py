
from itertools import combinations
import pandas as pd
import sys
from operator import itemgetter
# for row in df[1]:

#in the map part we look at friends of each person and create a tuple of members\
# who are not friends of each other and and assign value of 1 to each key if they\
# are friends to each other we woill not consider them


df=pd.read_csv(sys.stdin,header = None, sep='\t') #converting data to a df
df=df.dropna() #dropping people wh have no friends
#df has two columns of 0 (person) and 1 (friends)
for row in df[1]:
    friends = row.split(',')
    friends = sorted(map(int, friends)) #converting splitted list to list of numbers
    if len(friends)>1:
        friends_comb=combinations(list(friends),2) #creating combination of friends in the list
        for fc in list(friends_comb):
            fc=tuple(sorted(fc,key = int)) #sorting tuples so that smaller number be at the beginning
            if fc[0] not in map(int,list(df[df[0] == int(fc[1])][1])[0].split(',')): #checkin the members of the tuples are not friends already
                fc = ','.join(map(str,fc)) #creating a string of two numbers which are joined by ','
                print(fc,'\t', 1) #exporting to reducer

