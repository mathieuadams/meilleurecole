import csv
with open('french_school_enriched.csv','r',encoding='utf-8-sig',newline='') as f:
    r=csv.reader(f)
    h=next(r)
    for i,col in enumerate(h):
        print(i, col)
