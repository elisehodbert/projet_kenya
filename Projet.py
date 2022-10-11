from py2neo import *
import pandas as pd
from pandas import DataFrame
import os

# Connexion au graphe
graph = Graph("bolt://localhost:7687", auth=("neo4j", "4691"))

# petit nettoyage de la base avant de commencer
graph.run("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r")

# Setting working directory
os.chdir("/Users/niels/Desktop/Cours/M2/BigData/Projet avec l'autre qui fait comme ma daronne")

# CREATION DES NOEUDS
#=======================

# lecture du fichier csv des utilisateurs
#=======================
df = pd.read_csv("projet malawi.csv", delimiter=",")

#ok