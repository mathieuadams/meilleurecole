import csv
for enc in ('utf-8-sig','utf-8','latin-1'):
    try:
        with open('Lycee general and pro.csv','r',encoding=enc,errors='strict',newline='') as f:
            r=csv.reader(f)
            h=next(r)
            print(enc,'OK -> first headers:', h[:6])
    except Exception as e:
        print(enc,'FAIL',type(e).__name__,str(e)[:120])
