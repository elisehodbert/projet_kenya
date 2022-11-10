from py2neo import *
import pandas as pd
from pandas import DataFrame
import os
import matplotlib.pyplot as plt
import numpy as np

##############################################################################
################## Bien changer le FILEPATH et le password ###################
##############################################################################
FILEPATH = "/Users/niels/Downloads"
password = "4691"

# Connexion au graphe
graph = Graph("bolt://localhost:7687", auth=("neo4j", password))

# Nettoyage avant de commencer
graph.run("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r")

# Setting working directory
os.chdir(FILEPATH)

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

def rq_age():
    nb_id_age = []
    for i in list_age:
        rq_age = "MATCH (n:individu {age:'"+str(i)+"'}) RETURN count(*)"
        df = graph.run(rq_age).to_data_frame()
        nb_id_age.append(df['count(*)'][0])
    
    plt.bar(list_age,nb_id_age, color='black')
    plt.xlabel("Nombre d'individus par classe d'âge", fontweight='bold')
    plt.show()
    return nb_id_age

# Combien de personnes par classe d'âge (sans affichage graphique) ?

def rq_age_bis():
    nb_id_age = []
    for i in list_age:
        rq_age = "MATCH (n:individu {age:'"+str(i)+"'}) RETURN count(*)"
        df = graph.run(rq_age).to_data_frame()
        nb_id_age.append(df['count(*)'][0])
    return nb_id_age

# Combien de personnes par foyer ?

def rq_foyer():
    nb_id_foyer = []
    for i in list_foyer:
        rq_foyer = "MATCH (n:individu {foyer:'"+str(i)+"'}) RETURN count(*)"
        df = graph.run(rq_foyer).to_data_frame()
        nb_id_foyer.append(df['count(*)'][0])
    print(nb_id_foyer)
    
    plt.bar(list_foyer,nb_id_foyer, color="black")
    plt.xlabel("Nombre d'individus par foyer", fontweight='bold')
    plt.show()

# Combien de personnes par sexe ?

def rq_sexe():
    nb_id_sexe = []
    for i in list_sexe:
        rq_sexe = "MATCH (n:individu {sexe:'"+str(i)+"'}) RETURN count(*)"
        df = graph.run(rq_sexe).to_data_frame()
        nb_id_sexe.append(df['count(*)'][0])
    print(nb_id_sexe)
    plt.bar(list_sexe,nb_id_sexe,color="black")
    plt.xlabel("Nombre d'individus par sexe", fontweight='bold')
    plt.show()

## Comptage des interactions en tout (inter et intra confondues)

# Combien d'interactions de chaque durée ?

def rq_duree():
    rq200s = "MATCH (n:individu) -[:Contact_200s]-> (m) RETURN count(*)"
    rq400s = "MATCH (n:individu) -[:Contact_200_400s]-> (m) RETURN count(*)"
    rq600s = "MATCH (n:individu)-[:Contact_400_600s]-> (m) RETURN count(*)"
    rq800s = "MATCH (n:individu)-[:Contact_600_800s]-> (m) RETURN count(*)"
    df = graph.run(rq200s).to_data_frame()
    nb_inter200 = df['count(*)'][0]
    df = graph.run(rq400s).to_data_frame()
    nb_inter400 = df['count(*)'][0]
    df = graph.run(rq600s).to_data_frame()
    nb_inter600 = df['count(*)'][0]
    df = graph.run(rq800s).to_data_frame()
    nb_inter800 = df['count(*)'][0]
    
    barWidth = 0.20
    
    plt.bar(['- 200s','200-400s','400-600s','600-800s'],[nb_inter200,nb_inter400,nb_inter600,nb_inter800], color = "grey")
    plt.xlabel("Nombre d'interactions selon la durée du contact entre deux individus", fontweight='bold')
    
    plt.legend()
    plt.show()


# Combien d'interactions de chaque durée par classe d'âge ?

nb_id_age = rq_age_bis()

def rq_duree_age():
    nb_inter200_age, nb_inter400_age, nb_inter600_age, nb_inter800_age = [], [], [], []
    for i in list_age:
        rq200_age = "MATCH (n:individu {age:'"+str(i)+"'})-[:Contact_200s]-> (m) RETURN count(*)"
        rq400_age = "MATCH (n:individu {age:'"+str(i)+"'})-[:Contact_200_400s]-> (m) RETURN count(*)"
        rq600_age = "MATCH (n:individu {age:'"+str(i)+"'})-[:Contact_400_600s]-> (m) RETURN count(*)"
        rq800_age = "MATCH (n:individu {age:'"+str(i)+"'})-[:Contact_600_800s]-> (m) RETURN count(*)"
        df = graph.run(rq200_age).to_data_frame()
        nb_inter200_age.append(df['count(*)'][0]/nb_id_age[i])
        df = graph.run(rq400_age).to_data_frame()
        nb_inter400_age.append(df['count(*)'][0]/nb_id_age[i])
        df = graph.run(rq600_age).to_data_frame()
        nb_inter600_age.append(df['count(*)'][0]/nb_id_age[i])
        df = graph.run(rq800_age).to_data_frame()
        nb_inter800_age.append(df['count(*)'][0]/nb_id_age[i])
    
    plt.bar(['Classe 0', 'Classe 1', 'Classe 2', 'Classe 3', 'Classe 4'],nb_inter200_age, color='#d9c8ae', label='- 200s')
    plt.bar(['Classe 0', 'Classe 1', 'Classe 2', 'Classe 3', 'Classe 4'],nb_inter400_age, color='#f79667', label='200-400s',bottom=np.array(nb_inter200_age))
    plt.bar(['Classe 0', 'Classe 1', 'Classe 2', 'Classe 3', 'Classe 4'],nb_inter600_age, color='#d97193', label='400-600s',bottom=np.array(nb_inter400_age)+np.array(nb_inter200_age))
    plt.bar(['Classe 0', 'Classe 1', 'Classe 2', 'Classe 3', 'Classe 4'],nb_inter800_age, color='#cb92c2', label='600-800s',bottom=np.array(nb_inter600_age)+ np.array(nb_inter400_age)+np.array(nb_inter200_age))
    
    plt.xlabel("Nombre d'interactions par personne selon la classe d'âge", fontweight='bold')
    #plt.xticks([r + barWidth for r in range(len(nb_inter200_age))], ['Classe 0', 'Classe 1', 'Classe 2', 'Classe 3', 'Classe 4'])
    
    plt.legend()
    plt.show()


# Combien d'interactions inter-foyer par classe d'âge ?

def rq_inter_age():
    rq0_inter = "MATCH (n:individu {age:'0'})-[:Contact_200s | Contact_200_400s | Contact_400_600s | Contact_600_800s] -> (m) WHERE n.foyer <> m.foyer RETURN count(*)"
    df = graph.run(rq0_inter).to_data_frame()
    nb_inter_age0 = df['count(*)'][0]/nb_id_age[0]
        
    rq1_inter = "MATCH (n:individu {age:'1'})-[:Contact_200s | Contact_200_400s | Contact_400_600s | Contact_600_800s] -> (m) WHERE n.foyer <> m.foyer RETURN count(*)"
    df = graph.run(rq1_inter).to_data_frame()
    nb_inter_age1 = df['count(*)'][0]/nb_id_age[1]
    
    rq2_inter = "MATCH (n:individu {age:'2'})-[:Contact_200s | Contact_200_400s | Contact_400_600s | Contact_600_800s] -> (m) WHERE n.foyer <> m.foyer RETURN count(*)"
    df = graph.run(rq2_inter).to_data_frame()
    nb_inter_age2 = df['count(*)'][0]/nb_id_age[2]
    
    rq3_inter = "MATCH (n:individu {age:'3'})-[:Contact_200s | Contact_200_400s | Contact_400_600s | Contact_600_800s] -> (m) WHERE n.foyer <> m.foyer RETURN count(*)"
    df = graph.run(rq3_inter).to_data_frame()
    nb_inter_age3 = df['count(*)'][0]/nb_id_age[3]
    
    rq4_inter = "MATCH (n:individu {age:'4'})-[:Contact_200s | Contact_200_400s | Contact_400_600s | Contact_600_800s] -> (m) WHERE n.foyer <> m.foyer RETURN count(*)"
    df = graph.run(rq4_inter).to_data_frame()
    nb_inter_age4 = df['count(*)'][0]/nb_id_age[4]
    
    
    plt.figure(figsize = (8, 8))
    plt.pie([nb_inter_age0, nb_inter_age1, nb_inter_age2, nb_inter_age3, nb_inter_age4], 
            labels = ['Classe 0', 'Classe 1', 'Classe 2', 'Classe 3','Classe 4'], 
            labeldistance = 0.6,
            normalize = True)
    plt.xlabel("Nombre d'interactions inter-foyer par personne selon la classe d'âge",fontweight="bold")
    plt.legend() 

# Combien d'interactions intra-foyer par classe d'âge ?

def rq_intra_age():
    rq0_intra = "MATCH (n:individu {age:'0'})-[:Contact_200s | Contact_200_400s | Contact_400_600s | Contact_600_800s] -> (m) WHERE n.foyer = m.foyer RETURN count(*)"
    df = graph.run(rq0_intra).to_data_frame()
    nb_intra_age0 = df['count(*)'][0]/nb_id_age[0]
        
    rq1_intra = "MATCH (n:individu {age:'1'})-[:Contact_200s | Contact_200_400s | Contact_400_600s | Contact_600_800s] -> (m) WHERE n.foyer = m.foyer RETURN count(*)"
    df = graph.run(rq1_intra).to_data_frame()
    nb_intra_age1 = df['count(*)'][0]/nb_id_age[1]
    
    rq2_intra = "MATCH (n:individu {age:'2'})-[:Contact_200s | Contact_200_400s | Contact_400_600s | Contact_600_800s] -> (m) WHERE n.foyer = m.foyer RETURN count(*)"
    df = graph.run(rq2_intra).to_data_frame()
    nb_intra_age2 = df['count(*)'][0]/nb_id_age[2]
    
    rq3_intra = "MATCH (n:individu {age:'3'})-[:Contact_200s | Contact_200_400s | Contact_400_600s | Contact_600_800s] -> (m) WHERE n.foyer = m.foyer RETURN count(*)"
    df = graph.run(rq3_intra).to_data_frame()
    nb_intra_age3 = df['count(*)'][0]/nb_id_age[3]
    
    rq4_intra = "MATCH (n:individu {age:'4'})-[:Contact_200s | Contact_200_400s | Contact_400_600s | Contact_600_800s] -> (m) WHERE n.foyer = m.foyer RETURN count(*)"
    df = graph.run(rq4_intra).to_data_frame()
    nb_intra_age4 = df['count(*)'][0]/nb_id_age[4]
    
    
    
    plt.figure(figsize = (8, 8))
    plt.pie([nb_intra_age0, nb_intra_age1, nb_intra_age2, nb_intra_age3, nb_intra_age4], 
            labels = ['Classe 0', 'Classe 1', 'Classe 2', 'Classe 3','Classe 4'], 
            labeldistance = 0.6,
            normalize = True)
    plt.xlabel("Nombre d'interactions intra-foyer par personne selon la classe d'âge",fontweight="bold")
    plt.legend() 

# Camembert de la répartition des interactions inter et intra par durée d'interaction

# rq0_intra = "MATCH (n:individu)-[:Contact_200s] -> (m) WHERE n.foyer = m.foyer RETURN count(*)"
# df = graph.run(rq0_intra).to_data_frame()
# nb_intra_0 = df['count(*)'][0]

# rq0_inter = "MATCH (n:individu)-[:Contact_200s] -> (m) WHERE n.foyer <> m.foyer RETURN count(*)"
# df = graph.run(rq0_inter).to_data_frame()
# nb_inter_0 = df['count(*)'][0]

# plt.figure(figsize = (8, 8))
# plt.pie([nb_intra_0, nb_inter_0], 
#         labels = ['Contact intra-foyer', 'Contact inter-foyer'], 
#         labeldistance = 0.6,
#         normalize = True,
#         colors=["mediumpurple","pink"])
# plt.xlabel("Proportions d'interactions intra-foyer et inter-foyer pour des contacts de -200s",fontweight="bold")
# plt.legend() 


def  rq_intra_inter_400():
    rq200_intra = "MATCH (n:individu)-[:Contact_200_400s] -> (m) WHERE n.foyer = m.foyer RETURN count(*)"
    df = graph.run(rq200_intra).to_data_frame()
    nb_intra_400 = df['count(*)'][0]
    
    rq200_inter = "MATCH (n:individu)-[:Contact_200_400s] -> (m) WHERE n.foyer <> m.foyer RETURN count(*)"
    df = graph.run(rq200_inter).to_data_frame()
    nb_inter_400 = df['count(*)'][0]
    
    plt.figure(figsize = (8, 8))
    plt.pie([nb_intra_400, nb_inter_400], 
            labels = ['Contact intra-foyer', 'Contact inter-foyer'], 
            labeldistance = 0.6,
            normalize = True,
            colors=["mediumpurple","pink"])
    plt.xlabel("Proportions d'interactions intra-foyer et inter-foyer pour la durée 200-400s",fontweight="bold")
    plt.legend() 



# rq400_intra = "MATCH (n:individu)-[:Contact_400_600s] -> (m) WHERE n.foyer = m.foyer RETURN count(*)"
# df = graph.run(rq400_intra).to_data_frame()
# nb_intra_400 = df['count(*)'][0]

# rq400_inter = "MATCH (n:individu)-[:Contact_400_600s] -> (m) WHERE n.foyer <> m.foyer RETURN count(*)"
# df = graph.run(rq400_inter).to_data_frame()
# nb_inter_400 = df['count(*)'][0]

# plt.figure(figsize = (8, 8))
# plt.pie([nb_intra_400, nb_inter_400], 
#         labels = ['Contact intra-foyer', 'Contact inter-foyer'], 
#         labeldistance = 0.6,
#         normalize = True,
#         colors=["mediumpurple","pink"])X
# plt.xlabel("Proportions d'interactions intra-foyer et inter-foyer pour la durée 400-600s",fontweight="bold")
# plt.legend() 



# rq600_intra = "MATCH (n:individu)-[:Contact_600_800s] -> (m) WHERE n.foyer = m.foyer RETURN count(*)"
# df = graph.run(rq600_intra).to_data_frame()
# nb_intra_600 = df['count(*)'][0]

# rq600_inter = "MATCH (n:individu)-[:Contact_600_800s] -> (m) WHERE n.foyer <> m.foyer RETURN count(*)"
# df = graph.run(rq600_inter).to_data_frame()
# nb_inter_600 = df['count(*)'][0]

# plt.figure(figsize = (8, 8))
# plt.pie([nb_intra_600, nb_inter_600], 
#         labels = ['Contact intra-foyer', 'Contact inter-foyer'], 
#         labeldistance = 0.6,
#         normalize = True,
#         colors=["mediumpurple","pink"])
# plt.xlabel("Proportions d'interactions intra-foyer et inter-foyer pour la durée 600-800s",fontweight="bold")
# plt.legend() 

# Quel individu a le plus d'interactions inter ? le plus d'interactions intra ?

def rq_plus_interactions():
    rq_all = "MATCH (n:individu) -[:Contact_200s | Contact_200_400s | Contact_400_600s | Contact_600_800s]-> (m) RETURN n.id, n.sexe, n.age, n.foyer, count(*) ORDER BY count(*) DESC LIMIT 1"
    df = graph.run(rq_all).to_data_frame()
    print("La personne ayant eu le plus d'interactions, quel que soit le type de l'interaction, est l'individu n°"+str(df['n.id'][0])+", de sexe "+str(df['n.sexe'][0])+", appartenant à la classe d'âge "+str(df['n.age'][0])+" et au foyer " +str(df['n.foyer'][0]))

def rq_plus_inter():
    rq_inter = "MATCH (n:individu) -[:Contact_200s | Contact_200_400s | Contact_400_600s | Contact_600_800s]-> (m) WHERE n.foyer <> m.foyer RETURN n.id, n.sexe, n.age, n.foyer, count(*) ORDER BY count(*) DESC LIMIT 1"
    df = graph.run(rq_inter).to_data_frame()
    print("La personne ayant eu le plus d'interactions inter-foyer est l'individu n°"+str(df['n.id'][0])+", de sexe "+str(df['n.sexe'][0])+", appartenant à la classe d'âge "+str(df['n.age'][0])+" et au foyer " +str(df['n.foyer'][0]))

def rq_plus_intra():
    rq_intra = "MATCH (n:individu) -[:Contact_200s | Contact_200_400s | Contact_400_600s | Contact_600_800s]-> (m) WHERE n.foyer = m.foyer RETURN  n.id, n.sexe, n.age, n.foyer, count(*) ORDER BY count(*) DESC LIMIT 1"
    df = graph.run(rq_intra).to_data_frame()
    print("La personne ayant eu le plus d'interactions intra-foyer est l'individu n°"+str(df['n.id'][0])+", de sexe "+str(df['n.sexe'][0])+", appartenant à la classe d'âge "+str(df['n.age'][0])+" et au foyer " +str(df['n.foyer'][0]))


##############################################################################
########################## LANCEMENT DES REQUÊTES ############################
##############################################################################

rq_age() # La requête qui affiche la répartition des individus dans les différentes classes d'âge
rq_foyer() # La requête qui affiche la répartition des individus dans les différents foyers
rq_sexe() # La requête qui affiche la répartition des individus hommes ou femmes
rq_duree() # La requête qui affiche la répartition des durées en fonction de leur durée
rq_duree_age() # La requête qui affiche la répartition des durées des interactions par personnes selon la classe d'âge
rq_inter_age() # La requête qui affiche le nombre d'interactions inter par personnes selon la classe d'âge
rq_intra_age() # La requête qui affiche le nombre d'interactions intra par personnes selon la classe d'âge
rq_intra_inter_400() # La requête qui affiche les proportions d'interactions moyennes (200-400s) inter et intra 
rq_plus_interactions() # La requête qui renvoie la personne qui a le plus d'interactions totales
rq_plus_inter() # La requête qui renvoie la personne qui a le plus d'interactions inter
rq_plus_intra() # La requête qui renvoie la personne qui a le plus d'interactions intra
