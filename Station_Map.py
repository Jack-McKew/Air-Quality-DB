#%%
import folium
import pandas as pd
from sqlalchemy import create_engine

config = {
    'user':'root',
    'password':'AirQualityDB_2018',
    'host':'localhost',
    'database':'bom_data_test'
}

db_connection = create_engine('mysql+mysqldb://root:AirQualityDB_2018@localhost:3306/bom_data_test')

print("imported")

#%%

df = pd.read_sql("SELECT * FROM bom_data_test.directory LIMIT 1000",con=db_connection)



m = folium.Map(location=[-25.2744,133.7751],
                zoom_start=4,
                tiles='Stamen Terrain')

for index,row in df.iterrows():
    folium.Marker([float(row['Latitude to 4 decimal places - in decimal degrees.']),
                    float(row['Longitude to 4 decimal places - in decimal degrees.'])],
                            popup=str(str(row['Station Name.']) + '<br> <center>' + str(row['Station Number']) + '</center>')
                            ).add_to(m)

m.add_child(folium.LatLngPopup())

m.save('map.html')

m

