from os import walk
import csv
import datetime as dt
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

config = {
    'user':'root',
    'password':'AirQualityDB_2018',
    'host':'localhost',
    'database':'bom_data_test'
}

prev = 0

db_connection = create_engine('mysql+mysqldb://root:AirQualityDB_2018@localhost:3306/bom_data_test')

bom_data = []

for (dirpath,dirnames,filenames) in walk('C:\\Users\\McKewJ\\Documents\\Programming Jobs\\AirQualityDB'):
    for name in filenames:
        if name.endswith('.txt'):
            name = dirpath + "\\" + name
            bom_data.append(name)

columns =[
        'Record identifier - st',
        'Station Number',
        'Rainfall district code',
        'Station Name.',
        'Month/Year site opened. (MM/YYYY)',
        'Month/Year site closed. (MM/YYYY)',
        'Latitude to 4 decimal places - in decimal degrees.',
        'Longitude to 4 decimal places - in decimal degrees.',
        'Method by which latitude/longitude was derived.',
        'State.',
        'Height of station above mean sea level in metres.',
        'Height of barometer above mean sea level in metres.',
        'WMO (World Meteorological Organisation) Index Number.',
        'First year of data supplied in data file.',
        'Last year of data supplied in data file.',
        'Percentage complete between first and last records.',
        'Percentage of values with quality flag Y.',
        'Percentage of values with quality flag N.',
        'Percentage of values with quality flag W.',
        'Percentage of values with quality flag S.',
        'Percentage of values with quality flag I.',
        '# symbol, end of record indicator.'
        ]
rows_list = []
for txt_data in bom_data:
    in_txt = csv.reader(open(txt_data,"r"),delimiter=',')
    if "StnDet" in txt_data:
        for i,row in enumerate(in_txt):
            for j,element in enumerate(row):
                row[j] = element.strip()
            rows_list.append(row)

df = pd.DataFrame(rows_list,columns=columns)
df['Station Number'] = df['Station Number'].astype(int)
df.to_sql(con=db_connection,name="directory",if_exists='replace',chunksize=2000,index=False)
db_connection.execute('ALTER TABLE bom_data_test.directory ADD PRIMARY KEY (`Station Number`)')
df['Station Number'] = df['Station Number'].astype(str)
print(df.loc[:,'Station Number'])

print(bom_data)

def date_formatter(hour):
    if len(str(hour)) == 1:
        return "0" + str(hour)
    else:
        return str(hour)


def missing_number_fill(x):
    global prev
    try:
        float(x)
        prev = x
    except:
        x = prev
        return x


for station_number in df.loc[:,'Station Number']:
    for txt_data in bom_data:
        if station_number in txt_data:
            # FORMAT DATA READY FOR INPUT
            print(station_number +" : "+txt_data)
            input_data = pd.read_csv(txt_data,low_memory=False)
            input_data.replace(r'^\s+$',np.nan,regex=True)
            colnames = input_data.columns.tolist()
            for i,colname in enumerate(colnames):
                colnames[i] = colname.strip()[:63]
            input_data.columns = colnames
            input_data.drop(input_data.columns[[2,3,4,5,6]],axis=1,inplace=True)
            input_data.iloc[:,5].update(input_data.iloc[:,5].apply(date_formatter))
            input_data.iloc[:,6].update(input_data.iloc[:,6].apply(date_formatter))
            input_data['date_time'] = input_data.iloc[:,2].map(str) + "-" + input_data.iloc[:,3].map(str) + "-" + input_data.iloc[:,4].map(str) + " " + input_data.iloc[:,5].map(str) + ":" +  input_data.iloc[:,6].map(str) + ":00"
            input_data.drop(input_data.columns[[2,3,4,5,6]],axis=1,inplace=True)
            input_data.index = pd.to_datetime(input_data['date_time'])
            input_data.drop('date_time',axis=1,inplace=True)
            # input_data.to_csv(txt_data.split('.')[0]+".csv",index=False)


            sampling_settings = {
                                    'Precipitation since last (AWS) observation in mm' : np.sum,
                                    'QNH pressure in hPa' : np.mean
                                }
            resampled_data = pd.DataFrame()
            for colname,function in sampling_settings.items():
                input_data[colname].update(input_data[colname].apply(pd.to_numeric,errors='coerce'))
                input_data[colname].update(input_data[colname].apply(missing_number_fill))
                sample = input_data[colname].astype(float).resample('H').apply(function)
                resampled_data = pd.concat([resampled_data,sample],axis=1)
            resampled_data.insert(0,'Station Number',input_data['Station Number'].value_counts().idxmax())
            resampled_data.to_csv(txt_data.split('.')[0]+"_Resampled.csv")
            resampled_data['Timestamp'] = resampled_data.index.to_series().apply(lambda x: dt.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
            resampled_data.to_sql(con=db_connection,name=station_number,if_exists='replace',chunksize=2000,index=False)
            db_connection.execute('ALTER TABLE bom_data_test.' + station_number + " MODIFY COLUMN `Station Number` int(11);")
            db_connection.execute('ALTER TABLE bom_data_test.' + station_number + " ADD FOREIGN KEY (`Station Number`) REFERENCES bom_data_test.directory(`Station Number`);")