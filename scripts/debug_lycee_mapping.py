import csv, unicodedata
from pathlib import Path

def norm(s: str) -> str:
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace('\u2019',"'").replace('\u2013','-').replace('\u2014','-').replace('.', ' ')
    return ' '.join(s.lower().strip().split())

p = Path('Lycee general and pro.csv')
with p.open('r', encoding='latin-1', errors='strict', newline='') as f:
    reader = csv.DictReader(f)
    fn = reader.fieldnames or []
    print('Fieldnames:')
    for h in fn:
        print('-', h, '| norm:', norm(h))
    def find_col(tokens):
        for h in fn:
            if all(t in norm(h) for t in tokens):
                return h
        return None
    cols = {
        'uai': find_col(['uai']),
        'nb_bac': find_col(['nombre','eleves','presents','bac']),
        'taux_reussite': find_col(['taux','reussite']),
        'taux_mentions': find_col(['taux','mentions']),
        'eff_2nde': find_col(['effectifs','rentree','2nd']) or find_col(['effectifs','rentree','2nde']),
        'eff_1ere': find_col(['effectifs','rentree','1']),
        'eff_term': find_col(['effectifs','rentree','term']),
    }
    print('Chosen columns:', cols)
    for row in reader:
        if row.get(cols['uai']) == '0040003G':
            print('Row 0040003G values:')
            for k,c in cols.items():
                print(k, c, '=>', row.get(c))
            break
