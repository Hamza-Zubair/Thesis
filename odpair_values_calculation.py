import pandas as pd
import geopandas as gpd
from fiona.crs import from_epsg
from shapely.geometry import Point, LineString,shape
from shapely import wkt
#from sqlalchemy import create_engine

import networkx

##LOADING CSV TO MAKE DATAFRAME
way1=pd.read_csv(r'C:\Users\hamza_subair\Desktop\Data\PythonScripts\first_way.csv', header=0, sep= ',', encoding = 'utf-8-sig')
way2=pd.read_csv(r'C:\Users\hamza_subair\Desktop\Data\PythonScripts\second_way.csv', header=0, sep= ',', encoding = 'utf-8-sig')
route_file=pd.read_csv(r'C:\Users\hamza_subair\Desktop\Data\Routes_2019_full.csv', header=0, sep= ',', encoding = 'utf-8-sig')


#merging route_file with way1 and way2 file
combined1 = pd.merge(way1,route_file,left_on = 'route_code', right_on = 'route_code', how = 'left')
combined2 = pd.merge(way2, route_file, left_on = 'route_code', right_on='route_code', how = 'left')

#keeping selected columns
combined1 = combined1[['route_code', 'source_id', 'source', 'target_id', 'target', 'od_code', 'userID_new']]
combined2 = combined2[['route_code', 'source_id', 'source', 'target_id', 'target', 'od_code', 'userID_new']]

#creating pivot tables
table1 = pd.pivot_table(combined1, values = ['route_code', 'userID_new'], index = 'od_code',
aggfunc={'route_code': pd.Series.count,'userID_new': pd.Series.nunique})

table2 = pd.pivot_table(combined2, values = ['route_code', 'userID_new'], index = 'od_code',
aggfunc={'route_code': pd.Series.count,'userID_new': pd.Series.nunique})

#final od file updation, will be done on my machine
table1.to_csv(r'C:\Users\hamza_subair\Desktop\Data\PythonScripts\way1_ods.csv',encoding='utf-8-sig', index = True)
table2.to_csv(r'C:\Users\hamza_subair\Desktop\Data\PythonScripts\way2_ods.csv',encoding='utf-8-sig', index = True)

