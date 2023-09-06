import json

from JsonDB import JsonDB


db = JsonDB(repository='cadastro')
db.collection('cad_pessoas')
for x in range(500):
    db['_IDENTITY'] = {'Nome': 'Rodrigo', 'Sobrenome': 'Lopes'}
db.commit()
print(db[1])
