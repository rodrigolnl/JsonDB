from JsonDB import JsonDB, Identity


db = JsonDB(repository='cadastro')
db.collection('cad_pessoas')
print(db.all_repositories)
