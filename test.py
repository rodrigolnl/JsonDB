from JsonDB import JsonDB, Identity


db = JsonDB(repository='cadastro')
db.collection('cad_pessoas')
db[Identity] = {'None': 'Rodrigo2'}
db.commit()
print(db[1])

