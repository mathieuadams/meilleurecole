import os
import psycopg2

DB = os.environ.get(
    "DATABASE_URL",
    "postgresql://school_platform_db_fr_user:cDW1EB5Ah6x9KCguituTxOs63EZlXgnV@dpg-d37b0r6r433s73ejel20-a.oregon-postgres.render.com/school_platform_db_fr",
)

UAI = os.environ.get("UAI", "0040490L")

conn = psycopg2.connect(DB, sslmode="require")
cur = conn.cursor()
cur.execute(
    'select "identifiant_de_l_etablissement", lycee_bac_candidates, lycee_bac_success_rate, lycee_mentions_rate, lycee_effectifs_seconde, lycee_effectifs_premiere, lycee_effectifs_terminale, lycee_students_total, girls_total, boys_total from fr_ecoles where "identifiant_de_l_etablissement" = %s',
    (UAI,),
)
print(cur.fetchone())
conn.close()

