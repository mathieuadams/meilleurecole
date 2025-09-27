import csv
with open('Lycee general and pro.csv','r',encoding='latin-1',errors='strict',newline='') as f:
    r=csv.reader(f)
    h=next(r)
    for i,col in enumerate(h):
        print(i, col)
