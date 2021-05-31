from itertools import permutations
import pandas as pd
import numpy as np

#reading the csv of neighbourhood names and making a dataframe
df = pd.read_csv(r'C:\Users\humza\geopy2020\ThesisWork\1-creating origin destination pairs\neighbourhoods_3301.csv', header = 0 ,sep =',',encoding = 'Windows-1252', converters={'kood' : str})

#using permutation function to find all 2*2 possible relations
comb_rows = list(permutations(df.nimi, 2))

odlist = pd.DataFrame(comb_rows) 

#mapping the column header names for the the newly made dataframe from permutation list
mapping = {odlist.columns[0]:'origin', odlist.columns[1]:'destination'}

odlist = odlist.rename(columns=mapping)

#adding new empty columns for adding centriod x and y values
odxy = odlist.reindex(columns = odlist.columns.tolist() + ['org_id','origin_centX','origin_centY','dest_id','destination_centX','destination_centY','od_code']) 

#now fill up the lat long values for the origin neighbourhoods
new= pd.merge(odxy,df,left_on='origin',right_on='nimi')

new['org_id']=new['kood']
new['origin_centX']=new['centX']
new['origin_centY']=new['centY']

#now repeat the same for destination neighbourhoods, here we changed the columns because of recursive join
new2= pd.merge(new,df,left_on='destination',right_on='nimi')

new2['dest_id']=new2['kood_y']
new2['destination_centX']=new2['centX_y']
new2['destination_centY']=new2['centY_y']

# adding od codes for each record
new2['od_code'] = 'O'+ new2['org_id'].astype(str) +'D'+ new2['dest_id'].astype(str)

#final trimming the columns before exporting to csv
cols_to_keep = ['origin','org_id', 'destination','dest_id','od_code', 'origin_centX','origin_centY','destination_centX','destination_centY']

#exporting the csv
odxyfinal= new2.loc[: , cols_to_keep].to_csv('od_tartu.csv',encoding='Windows-1252', index=False, header=True)