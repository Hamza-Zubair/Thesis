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

###WAY 1 FOR KEEPING ONLY START AND END NEIGHBOURHOODS
#group by on basis of route codes to make line strings
lines = points.groupby(['route_code'])['geometry'].apply(lambda x: LineString(x.tolist()) if x.size > 1 else None)
line_gdf = gpd.GeoDataFrame(lines, geometry = 'geometry')

line_gdf.crs = 'epsg:4326'


#Removing any null geometries
line_gdf = line_gdf[line_gdf.geometry.is_valid]

#line_gdf.to_file(r'C:\Users\hamza_subair\Desktop\Data\PythonScripts\abc.shp')

#slicing the start end end coordinates of lines
#create empty columns
line_gdf['first'] = None
line_gdf['last'] = None

#iterate through gdf to get coordinates
for index, row in line_gdf.iterrows():
    coords = [(coords) for coords in list(row['geometry'].coords)]
    first_coord, last_coord = [ coords[i] for i in (0, -1)]
    line_gdf.at[index, 'first'] = Point(first_coord)
    line_gdf.at[index, 'last'] = Point(last_coord)
line_gdf.reset_index()

# #creating seperate dataframes for start and end 
start_wgs = gpd.GeoDataFrame(line_gdf, geometry = 'first')
end_wgs = gpd.GeoDataFrame(line_gdf, geometry = 'last')

start_wgs.crs = 'epsg:4326'
end_wgs.crs = 'epsg:4326'

#reset the indices
start = start_wgs.reset_index()
start =start[['route_code','first']]

end = end_wgs.reset_index()
end =end[['route_code','last']]

#spatial join with neighbourhoods
startsj = gpd.sjoin(start, asumid, how = "left", op = "within")
endsj = gpd.sjoin(end, asumid, how = "left", op = "within")

# #keeping only selected columns
startsj.rename(columns = {"kood": "source_id", "nimi":"source"},inplace = True)
endsj.rename(columns = {"kood": "target_id", "nimi":"target"},inplace = True)

#merging both df as attribute join
combined = pd.merge(startsj,endsj, left_on = 'route_code', right_on = 'route_code')

#removing all those trips which were started and ended in same neighbourhood
combined = combined[combined['source_id']!= combined['target_id']]

#generate od code
combined['od_code'] = 'O'+ combined['source_id'].astype(str) +'D' + combined['target_id'].astype(str)

finaloutput = combined[['route_code', 'source_id', 'source', 'target_id', 'target', 'od_code' ]]

#to attach user ids

# final = pd.merge(finaloutput,df, left_on = 'route_code', right_on = 'route_code', how ='left')

# final = final[['route_code', 'source_id', 'source', 'target_id', 'target', 'od_code', 'userID_new']]

finaloutput.to_csv(r'C:\Users\hamza_subair\Desktop\Data\PythonScripts\first_way.csv',encoding='utf-8-sig', index = False)