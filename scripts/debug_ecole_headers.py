import csv, unicodedata
from pathlib import Path

p = Path('french_school_etudiant_par_class2024.csv')
with p.open('r', encoding='utf-8-sig', errors='replace', newline='') as f:
    reader = csv.DictReader(f)
    fn = reader.fieldnames or []
    def norm(s: str) -> str:
        s = unicodedata.normalize('NFKD', s)
        s = ''.join(ch for ch in s if not unicodedata.combining(ch))
        s = s.replace('\u2019',"'").replace('\u2013','-').replace('\u2014','-').replace('.', ' ')
        return ' '.join(s.lower().strip().split())
    print('Fieldnames count:', len(fn))
    for i, h in enumerate(fn):
        print(i, '|', h, '| norm:', norm(h))
