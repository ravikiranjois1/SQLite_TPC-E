import glob
import sqlite3


conn=sqlite3.connect('tpce')
cur=conn.cursor()

for file in glob.glob("./flat_out/*.txt"):
    file_name=file.strip('./flat_out/').strip('txt')
    table_name=file_name.strip('.')
    with open('./flat_out/'+file_name+'txt') as data:
        rows=data.readlines(10000000)
        csv_rows=[]
        for row in rows:
            csv_row=row.replace('|',',')
            csv_rows.append(csv_row)
        with open('./raw_data/'+table_name+'.csv','w') as output:
            output.writelines(csv_rows)
