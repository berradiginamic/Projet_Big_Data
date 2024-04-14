# Projet-Big-Data
Groupe : Christ / Fatima / Christopher / Dorian

Langage requis : Python 3.5 ou une version stable recommandée

Modules Python à installer :

- sys
- pandas
- datetime
- decimal
- matplotlib.pyplot
- sample
- csv
- happybase 

API utilisée: Apache Hbase Rest

Ce dossier contient des scripts Python commentés avec des docstrings expliquant leurs objectifs et leur utilisation.
L'utilisateur a le choix de lancer les scripts directement depuis un IDE compatible Python ou depuis Hadoop (nécessaire pour HBase - Lot3).

-Attention ! Les répertoires des dossiers doivent être commentés et décommentés selon l'environnement (IDE/Hadoop).

Pour Hadoop:
- Exécution de l'environnement Hadoop et modification ou configuration des dossiers appropriés.
- Lancement d'un job MapReduce avec la commande (exemple pour le lot 1):
    - hadoop jar hadoop-streaming-2.7.2.jar -file mapper_lot1.py -mapper "python3 mapper_lot1.py" -file reducer_lot1.py -reducer "python3 reducer_lot1.py" -input input/dataw_fro03.csv -output output/output_lot1_exo1
- Vérification des résultats dans le répertoire de sortie approprié.

Veuillez noter que le processus d'analyse et d'exécution des scripts doit être effectué avec soin pour garantir des résultats précis et fiables.



Enoncé :

 LOT 1
•
Contexte :
•
Une Fromagerie (le client) a un datawarehouse depuis 2004 qui est représenté par le fichier csv fournit dans ce document.
•
Créer des jobs pour limiter le flux d’information (Mapper-Reducer) pour obtenir uniquement les informations voulues pour répondre au besoin du client décrit ci-dessous :
•
Le client désire les statistiques suivantes :

1) Filtrer les données selon les critères suivants :
Entre 2006 et 2010,
Avec uniquement les départements 53, 61 et 28

2) A partir du point 1 : Ressortir dans un tableau des 100 meilleures commandes avec la ville, la somme des quantités des articles et la valeur de « timbrecde » (la notion de meilleures commandes : la somme des quantités la plus grande ainsi que le plus grand nombre de « timbrecde » )

3) Exporter le résultat dans un fichier Excel.


LOT 2
•
Contexte :
•
Une Fromagerie (le client) a un datawarehouse depuis 2004 qui est représenté par le fichier csv fournit dans ce document.
•
Le client désire les statistiques suivantes :

1) Filtrer les données selon les critères suivants :
Entre 2011 et 2016,
Avec uniquement les départements 22, 49 et 53

2) A partir du point 1 : Ressortir de façon aléatoire de 5% des 100 meilleures commandes avec la ville, la somme des quantités des articles sans « timbrecli » (le timbrecli non renseigné ou à 0) avec la moyenne des quantités de chaque commande)
Avoir un PDF avec un graphe (PIE) (par Ville)

3) Exporter le résultat dans un fichier Excel.

LOT 3

(De votre poste local : interroger votre VM LINUX sur le port 9090 (port privé de votre VM =>
prendre votre endpoint public correspondant)
1. Mettre en place une base NoSQL HBASE pour stocker le contenu du fichier CSV afin
d’interroger ce Data Warehouse avec des scripts python.
· La meilleur commande de Nantes de l’année 2020.
· Le nombre total de commandes effectuées entre 2010 et 2015, réparties par année
· Le nom, le prénom, le nombre de commande et la somme des quantités d’objets du
client qui a eu le plus de frais de timbrecde.

Créer un programme python (avec Panda) pour créer des graphes en pdf et des tableaux
Excel et csv de votre importation dans HBase : § Question 1 partie 1 du lot 3 en csv § Question 2 partie 1 du lot 3 en barplot matplotib exporté en pdf § Question 3 partie 1 du lot 3 en excel

Lot 4

(De votre poste local, importer dans le HBase de votre VM Linux)
(De votre VM Windows, utiliser Power BI)
• Mettre en oeuvre un moteur de recherche avec Power BI pour interroger le Data
Warehouse HBase.
· Pour répondre au Lot 1 et Lot 2 au niveau des résultats avec les graphes,
· Vous avez carte blanche pour créer d’autres graphes , d’autres types de requêtes
avec par exemple de la géolocalisation etc.
· Mise en place d’un Dashboard interactif
