import csv
target = "Nombre d'élèves présents au Bac"
with open('french_school_enriched.csv','r',encoding='utf-8-sig',newline='') as f:
    r=csv.reader(f)
    h=next(r)
    print('Equal?', h[50] == target)
    print('Header col50:', h[50])
