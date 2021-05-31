import pandas as pd
import geopandas as gpd
from fiona.crs import from_epsg
from shapely.geometry import Point, LineString,shape
from shapely import wkt
#from sqlalchemy import create_engine

##LOADING SHAPEFILE
asumid = gpd.GeoDataFrame.from_file(r"C:\Users\hamza_subair\Desktop\Data\forscript\asumid_3301.shp")
asumid = asumid.to_crs(epsg=4326)
asumid['kood'] = asumid['kood'].astype('int')
asumid['kood'] = asumid['kood'].astype('str')
asumid['kood'] = asumid['kood'].apply(lambda x: '{0:0>2}'.format(x))

##LOADING CSV TO MAKE DATAFRAME
data=pd.read_csv(r'C:\Users\hamza_subair\Desktop\Data\forscript\GPS_2019_full.csv', header=0, sep= ',', encoding = 'latin1')

##REMOVING THOSE POINTS WHERE VALUES WERE NA
df = data.dropna(axis=0)

##ZIPPING COORIDNATES TO MAKE GEOMETRY FROM DF
geometry = [Point(xy) for xy in zip(df.longitude, df.latitude)]
point_gdf = gpd.GeoDataFrame(df, geometry = geometry)
point_gdf.crs = 'epsg:4326'

##SPATIAL JOIN OF DF AND SHP
point_sj = gpd.sjoin(point_gdf, asumid, how = "left", op = "within")

##KEEPING ONLY REQUIRED COLUMNS 
points = point_sj[['route_code', 'cyclenumber', 'latitude', 'longitude', 'coord_date', 'coord_time', 'userID_new', 'geometry','kood', 'nimi']]

##DELETEING THOSE POINTS WHERE coord_time is NA or which are lying outside the neighbourhood
points.dropna(subset=['kood'], inplace=True)

###WAY 2 FOR KEEPING ONLY START AND END NEIGHBOURHOODS
#deleting duplicates on basis of route code and neighbourhood code
filterpoints = points.drop_duplicates(subset=['route_code', 'kood'])

#grouping and making ordered list of all neighbourhoods
filterpoints['nh_list'] = filterpoints.groupby(['route_code'])['kood'].transform(lambda x : ','.join(x))

#removing trips which are in single neighbourhood
final  = filterpoints[filterpoints['nh_list'].isin(['1','2','3','4','5','6','7','8','9','10',
'11','12','13','14','15','16','17','18','19','20',
'21','22','23','24','25','26','27','28','29','30','31','32','33','34']) == False]

#making pairwise list of neighbourhoods
final['nh_list'] = final['nh_list'].apply(lambda row: row.split(','))
final['pairs'] = final['nh_list'].apply(lambda row: [[row[i], row[i+1]] for i in range(len(row)-1)])

#exploding the dataframe to get route od codes for all intersecting neighbourhoods
export = final.copy()
export = pd.DataFrame(export)

results = export.explode('pairs')

results.dropna(inplace = True)

#converting pairs list to strings
results["pairs"] = results.pairs.apply(str)

#making new column to add the split string
results[['source_id', 'target_id']] = results.pairs.str.split(",", expand = True)

#trimming the newly made columns to keep only the codes
results['source_id'] = results['source_id'].str[2:4]
results['target_id'] = results['target_id'].str[2:4]

#generate od code
results['od_code'] = 'O'+ results['source_id'].astype(str) + 'D' + results['target_id'].astype(str)

#making the source and target name column and filling it
results['source'] = results['nimi']

results1 = pd.merge(results,asumid,left_on = 'target_id', right_on = 'kood', how = 'left')

results1['target'] = results1['nimi_y']

finalresult = results1[['route_code', 'source_id', 'source', 'target_id', 'target', 'od_code']]


#exporting the csv
finalresult.to_csv(r'C:\Users\hamza_subair\Desktop\Data\PythonScripts\second_way.csv',encoding='utf-8-sig', index = False)