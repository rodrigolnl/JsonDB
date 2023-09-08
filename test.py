from JsonDB import JsonDB, Identity


db = JsonDB(repository='cadastro')
db.collection('cad_pessoas2')
for x in db:
    print(db[x])
db.commit()
