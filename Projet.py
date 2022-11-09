from py2neo import *
import pandas as pd
from pandas import DataFrame
import os
import matplotlib.pyplot as plt
import numpy as np

# Connexion au graphe
graph = Graph("bolt://localhost:7687", auth=("neo4j", "kenya"))

# Nettoyage avant de commencer
graph.run("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r")

# Setting working directory
os.chdir("/Users/elise/OneDrive/Documents/ACO_M2Stats/Computer_Science_for_Big_Data_C_Largouet/projet")

##############################################################################
############################ CREATION DES NOEUDS #############################
##############################################################################

# Lecture du fichier csv des individus

df = pd.read_csv("individus.csv", delimiter=";")
individus = {}
for index, row in df.iterrows():
    individus[row["id"]] = Node("individu",
                                id=str(row["id"]),
                                foyer=str(row["foyer"]),
                                age=str(row["age"]),
                                sexe=str(row["sexe"]))

foyers = {}
for index, row in df.iterrows():
    foyers[row["foyer"]] = Node("foyer",
                                foyer=str(row["foyer"]))

    
    
##############################################################################
########################### CREATION DES RELATIONS ###########################
##############################################################################    

habite = []
for index, row in df.iterrows():
    individu = individus[row["id"]]
    foyer = foyers[row["foyer"]]    
    habite.append(Relationship(individu,"Habite", foyer))

df = pd.read_csv("relations.csv", delimiter=";")

contact_moins_200s = []
contact_entre_200_et_400s = []
contact_entre_400_et_600s = []
contact_entre_600_et_800s = []

for index, row in df.iterrows():
    individu1 = individus[row['id1']]
    individu2 = individus[row['id2']]
    if row["duree"] < 200 :
        contact_moins_200s.append(Relationship(individu1, "Contact_200s", individu2))
    elif row["duree"] < 400 :
        contact_entre_200_et_400s.append(Relationship(individu1, "Contact_200_400s", individu2))
    elif row["duree"] < 600 :
        contact_entre_400_et_600s.append(Relationship(individu1, "Contact_400_600s", individu2))
    else :
        contact_entre_600_et_800s.append(Relationship(individu1, "Contact_600_800s", individu2))
    
    
    
# Création du graphe dans la base

for r in contact_moins_200s:
    graph.create(r)    

for r in contact_entre_200_et_400s:
    graph.create(r)    
    
for r in contact_entre_400_et_600s:
    graph.create(r)    

for r in contact_entre_600_et_800s:
    graph.create(r)    

for r in habite:
    graph.create(r)


##############################################################################
########################### REQUETES #########################################
##############################################################################  

list_age = range(5)
list_sexe = ["M","F"]
list_foyer = ["E","L","F","B","H"]

# Combien de personnes par classe d'âge ?

nb_id_age = []
for i in list_age:
    rq = "MATCH (n:individu {age:'"+str(i)+"'}) RETURN count(*)"
    df = graph.run(rq).to_data_frame()
    nb_id_age.append(df['count(*)'][0])
print(nb_id_age)

plt.bar(list_age,nb_id_age, color='black')
plt.xlabel("Nombre d'individus par classe d'âge", fontweight='bold')
plt.show()

# Combien de personnes par foyer ?

nb_id_foyer = []
for i in list_foyer:
    rq = "MATCH (n:individu {foyer:'"+str(i)+"'}) RETURN count(*)"
    df = graph.run(rq).to_data_frame()
    nb_id_foyer.append(df['count(*)'][0])
print(nb_id_foyer)

plt.bar(list_foyer,nb_id_foyer, color="black")
plt.xlabel("Nombre d'individus par foyer", fontweight='bold')
plt.show()

# Combien de personnes par sexe ?

nb_id_sexe = []
for i in list_sexe:
    rq = "MATCH (n:individu {sexe:'"+str(i)+"'}) RETURN count(*)"
    df = graph.run(rq).to_data_frame()
    nb_id_sexe.append(df['count(*)'][0])
print(nb_id_sexe)
plt.bar(list_sexe,nb_id_sexe,color="black")
plt.xlabel("Nombre d'individus par sexe", fontweight='bold')
plt.show()

## Comptage des interactions en tout (inter et intra confondues)

# Combien d'interactions de chaque durée ?

rq200 = "MATCH (n:individu) -[:Contact_200s]-> (m) RETURN n"
rq400 = "MATCH (n:individu) -[:Contact_200_400s]-> (m) RETURN n"
rq600 = "MATCH (n:individu)-[:Contact_400_600s]-> (m) RETURN n"
rq800 = "MATCH (n:individu)-[:Contact_600_800s]-> (m) RETURN n"
df = graph.run(rq200).to_data_frame()
nb_inter200 = df.shape[0]
df = graph.run(rq400).to_data_frame()
nb_inter400 = df.shape[0]
df = graph.run(rq600).to_data_frame()
nb_inter600 = df.shape[0]
df = graph.run(rq800).to_data_frame()
nb_inter800 = df.shape[0]

barWidth = 0.20

plt.bar(['- 200s','200-400s','400-600s','600-800s'],[nb_inter200,nb_inter400,nb_inter600,nb_inter800], color = "grey")
plt.xlabel("Nombre d'interactions selon la durée du contact entre deux individus", fontweight='bold')

plt.legend()
plt.show()


# Combien d'interactions de chaque durée par classe d'âge ?

nb_inter200_age, nb_inter400_age, nb_inter600_age, nb_inter800_age = [], [], [], []

for i in list_age:
    rq200 = "MATCH (n:individu {age:'"+str(i)+"'})-[:Contact_200s]-> (m) RETURN n"
    rq400 = "MATCH (n:individu {age:'"+str(i)+"'})-[:Contact_200_400s]-> (m) RETURN n"
    rq600 = "MATCH (n:individu {age:'"+str(i)+"'})-[:Contact_400_600s]-> (m) RETURN n"
    rq800 = "MATCH (n:individu {age:'"+str(i)+"'})-[:Contact_600_800s]-> (m) RETURN n"
    df = graph.run(rq200).to_data_frame()
    nb_inter200_age.append(df.shape[0])
    df = graph.run(rq400).to_data_frame()
    nb_inter400_age.append(df.shape[0])
    df = graph.run(rq600).to_data_frame()
    nb_inter600_age.append(df.shape[0])
    df = graph.run(rq800).to_data_frame()
    nb_inter800_age.append(df.shape[0])

barWidth = 0.20

plt.bar(['Classe 0', 'Classe 1', 'Classe 2', 'Classe 3', 'Classe 4'],nb_inter200_age, color='#d9c8ae', label='- 200s')
plt.bar(['Classe 0', 'Classe 1', 'Classe 2', 'Classe 3', 'Classe 4'],nb_inter400_age, color='#57c7e3', label='200-400s',bottom=np.array(nb_inter200_age))
plt.bar(['Classe 0', 'Classe 1', 'Classe 2', 'Classe 3', 'Classe 4'],nb_inter600_age, color='#8ecc93', label='400-600s',bottom=np.array(nb_inter400_age)+np.array(nb_inter200_age))
plt.bar(['Classe 0', 'Classe 1', 'Classe 2', 'Classe 3', 'Classe 4'],nb_inter800_age, color='#c990c0', label='600-800s',bottom=np.array(nb_inter600_age)+ np.array(nb_inter400_age)+np.array(nb_inter200_age))

plt.xlabel("Nombre d'interactions en fonction de la classe d'âge", fontweight='bold')
#plt.xticks([r + barWidth for r in range(len(nb_inter200_age))], ['Classe 0', 'Classe 1', 'Classe 2', 'Classe 3', 'Classe 4'])

plt.legend()
plt.show()


# Combien d'interactions de chaque durée par foyer ?

nb_inter200_foyer = []
nb_inter400_foyer = []
nb_inter600_foyer = []
nb_inter800_foyer = []

for i in list_foyer:
    rq200 = "MATCH (n:individu {foyer:'"+str(i)+"'})-[:Contact_200s]-> (m) RETURN n"
    rq400 = "MATCH (n:individu {foyer:'"+str(i)+"'})-[:Contact_200_400s]-> (m) RETURN n"
    rq600 = "MATCH (n:individu {foyer:'"+str(i)+"'})-[:Contact_400_600s]-> (m) RETURN n"
    rq800 = "MATCH (n:individu {foyer:'"+str(i)+"'})-[:Contact_600_800s]-> (m) RETURN n"
    df = graph.run(rq200).to_data_frame()
    nb_inter200_foyer.append(df.shape[0])
    df = graph.run(rq400).to_data_frame()
    nb_inter400_foyer.append(df.shape[0])
    df = graph.run(rq600).to_data_frame()
    nb_inter600_foyer.append(df.shape[0])
    df = graph.run(rq800).to_data_frame()
    nb_inter800_foyer.append(df.shape[0])

# Création du graphique
barWidth = 0.20

r1 = np.arange(len(nb_inter200_foyer))
r2 = [x + barWidth for x in r1]
r3 = [x + barWidth for x in r2]
r4 = [x + barWidth for x in r3]

plt.bar(r1, nb_inter200_foyer, color='#d9c8ae', width=barWidth, edgecolor='white', label='-200s')
plt.bar(r2, nb_inter400_foyer, color='#57c7e3', width=barWidth, edgecolor='white', label='200-400s')
plt.bar(r3, nb_inter600_foyer, color='#8ecc93', width=barWidth, edgecolor='white', label='400-600s')
plt.bar(r4, nb_inter800_foyer, color='#c990c0', width=barWidth, edgecolor='white', label='600-800s')

plt.xlabel("Nombre d'interactions en fonction du foyer", fontweight='bold')
plt.xticks([r + barWidth for r in range(len(nb_inter200_foyer))], ["Foyer E","Foyer L","Foyer F","Foyer B","Foyer H"])

plt.legend()
plt.show()

# Quel individu a le plus d'interactions inter ? le plus d'interactions intra ?

rq = "MATCH (n:individu) -[:Contact_200s]-> (m) RETURN n.id, n.sexe, n.age, n.foyer, count(*) ORDER BY count(*) DESC"
df = graph.run(rq).to_data_frame()
print("La personne ayant eu le plus d'interactions, quel que soit le type de l'interaction, est l'individu n°"+str(df['n.id'][0])+", de sexe "+str(df['n.sexe'][0])+", appartenant à la classe d'âge "+str(df['n.age'][0])+" et au foyer " +str(df['n.foyer'][0]))

rq = "MATCH (n:individu) -[:Contact_200s]-> (m) WHERE n.foyer <> m.foyer RETURN n.id, n.sexe, n.age, n.foyer, count(*) ORDER BY count(*) DESC"
df = graph.run(rq).to_data_frame()
print("La personne ayant eu le plus d'interactions inter-foyer est l'individu n°"+str(df['n.id'][0])+", de sexe "+str(df['n.sexe'][0])+", appartenant à la classe d'âge "+str(df['n.age'][0])+" et au foyer " +str(df['n.foyer'][0]))

rq = "MATCH (n:individu) -[:Contact_200s]-> (m) WHERE n.foyer = m.foyer RETURN  n.id, n.sexe, n.age, n.foyer, count(*) ORDER BY count(*) DESC"
df = graph.run(rq).to_data_frame()
print("La personne ayant eu le plus d'interactions intra-foyer est l'individu n°"+str(df['n.id'][0])+", de sexe "+str(df['n.sexe'][0])+", appartenant à la classe d'âge "+str(df['n.age'][0])+" et au foyer " +str(df['n.foyer'][0]))



# Combien d'interactions inter-foyer par classe d'âge ?

rq0_inter = "MATCH (n:individu {age:'0'})-[:Contact_200s | Contact_200_400s | Contact_400_600s | Contact_600_800s] -> (m) WHERE n.foyer <> m.foyer RETURN count(*)"
df = graph.run(rq0_inter).to_data_frame()
nb_inter_age0 = df['count(*)'][0]
    
rq1_inter = "MATCH (n:individu {age:'1'})-[:Contact_200s | Contact_200_400s | Contact_400_600s | Contact_600_800s] -> (m) WHERE n.foyer <> m.foyer RETURN count(*)"
df = graph.run(rq1_inter).to_data_frame()
nb_inter_age1 = df['count(*)'][0]

rq2_inter = "MATCH (n:individu {age:'2'})-[:Contact_200s | Contact_200_400s | Contact_400_600s | Contact_600_800s] -> (m) WHERE n.foyer <> m.foyer RETURN count(*)"
df = graph.run(rq2_inter).to_data_frame()
nb_inter_age2 = df['count(*)'][0]

rq3_inter = "MATCH (n:individu {age:'3'})-[:Contact_200s | Contact_200_400s | Contact_400_600s | Contact_600_800s] -> (m) WHERE n.foyer <> m.foyer RETURN count(*)"
df = graph.run(rq3_inter).to_data_frame()
nb_inter_age3 = df['count(*)'][0]

rq4_inter = "MATCH (n:individu {age:'4'})-[:Contact_200s | Contact_200_400s | Contact_400_600s | Contact_600_800s] -> (m) WHERE n.foyer <> m.foyer RETURN count(*)"
df = graph.run(rq4_inter).to_data_frame()
nb_inter_age4 = df['count(*)'][0]


plt.figure(figsize = (8, 8))
plt.pie([nb_inter_age0, nb_inter_age1, nb_inter_age2, nb_inter_age3, nb_inter_age4], 
        labels = ['Classe 0', 'Classe 1', 'Classe 2', 'Classe 3','Classe 4'], 
        labeldistance = 0.6,
        normalize = True)
plt.xlabel("Répartition des interactions inter-foyer par classe d'âge",fontweight="bold")
plt.legend() 

# Combien d'interactions intra-foyer par classe d'âge ?

rq0_intra = "MATCH (n:individu {age:'0'})-[:Contact_200s | Contact_200_400s | Contact_400_600s | Contact_600_800s] -> (m) WHERE n.foyer = m.foyer RETURN count(*)"
df = graph.run(rq0_intra).to_data_frame()
nb_intra_age0 = df['count(*)'][0]
    
rq1_intra = "MATCH (n:individu {age:'1'})-[:Contact_200s | Contact_200_400s | Contact_400_600s | Contact_600_800s] -> (m) WHERE n.foyer = m.foyer RETURN count(*)"
df = graph.run(rq1_intra).to_data_frame()
nb_intra_age1 = df['count(*)'][0]

rq2_intra = "MATCH (n:individu {age:'2'})-[:Contact_200s | Contact_200_400s | Contact_400_600s | Contact_600_800s] -> (m) WHERE n.foyer = m.foyer RETURN count(*)"
df = graph.run(rq2_intra).to_data_frame()
nb_intra_age2 = df['count(*)'][0]

rq3_intra = "MATCH (n:individu {age:'3'})-[:Contact_200s | Contact_200_400s | Contact_400_600s | Contact_600_800s] -> (m) WHERE n.foyer = m.foyer RETURN count(*)"
df = graph.run(rq3_intra).to_data_frame()
nb_intra_age3 = df['count(*)'][0]

rq4_intra = "MATCH (n:individu {age:'4'})-[:Contact_200s | Contact_200_400s | Contact_400_600s | Contact_600_800s] -> (m) WHERE n.foyer = m.foyer RETURN count(*)"
df = graph.run(rq4_intra).to_data_frame()
nb_intra_age4 = df['count(*)'][0]



plt.figure(figsize = (8, 8))
plt.pie([nb_intra_age0, nb_intra_age1, nb_intra_age2, nb_intra_age3, nb_intra_age4], 
        labels = ['Classe 0', 'Classe 1', 'Classe 2', 'Classe 3','Classe 4'], 
        labeldistance = 0.6,
        normalize = True)
plt.xlabel("Répartition des interactions intra-foyer par classe d'âge",fontweight="bold")
plt.legend() 

# Camembert de la répartition des interactions inter et intra par durée d'interaction

rq200_intra = "MATCH (n:individu {age:'0'})-[:Contact_200_400s] -> (m) WHERE n.foyer = m.foyer RETURN count(*)"
df = graph.run(rq200_intra).to_data_frame()
nb_intra_200 = df['count(*)'][0]

rq200_inter = "MATCH (n:individu {age:'0'})-[:Contact_200_400s] -> (m) WHERE n.foyer <> m.foyer RETURN count(*)"
df = graph.run(rq200_inter).to_data_frame()
nb_inter_200 = df['count(*)'][0]

plt.figure(figsize = (8, 8))
plt.pie([nb_intra_200, nb_inter_200], 
        labels = ['Intra-foyer', 'Inter-foyer'], 
        labeldistance = 0.6,
        normalize = True,
        colors=["mediumpurple","pink"])
plt.xlabel("Proportions d'interactions intra-foyer et inter-foyer pour la durée 200-400s",fontweight="bold")
plt.legend() 
