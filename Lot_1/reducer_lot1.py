#!/usr/bin/env python3
"""reducer_lot1.py

Ce script lit les données du mapper_lot1.py et les agrège en fonction du code CDE.
Il calcule la quantité totale commandée et le timbre CDE maximum pour chaque code CDE.
Le résultat est ensuite trié par quantité et timbre CDE et enregistré dans un fichier Excel.

Fonctionnement:

1. Lit les données ligne par ligne depuis l'entrée standard.
2. Pour chaque ligne:
   - Découpe la ligne en colonnes en utilisant la tabulation comme séparateur.
   - Extrait les informations spécifiques des colonnes:
       - code CDE
       - ville
       - timbre CDE
       - quantité
       - date
       - département
   - Met à jour un dictionnaire `commandes` avec les informations extraites.
3. Trie le dictionnaire `commandes` par quantité et timbre CDE.
4. Crée un DataFrame Pandas à partir des données du dictionnaire.
5. Enregistre le DataFrame dans un fichier Excel nommé `lot1_exo1.xlsx`.

Gestion d'erreurs:

- Si la quantité n'est pas un nombre, la valeur par défaut 0 est utilisée.
"""
import sys
import decimal
import pandas as pd

# Fonction pour traiter une ligne de données et mettre à jour le dictionnaire commandes
def process_line(line, commandes):
    # Séparer les colonnes de la ligne
    columns = line.strip().split("\t")

    # Assurez-vous que la ligne a le bon format
    if len(columns) == 6:
        # Extraire les valeurs des colonnes
        key, ville, timbrecde, qte, date, dept = columns

        # Assurez-vous que la quantité est un nombre
        try:
            qte = int(qte)
        except ValueError:
            qte = 0

        # Mettre à jour le dictionnaire commandes
        if key not in commandes:
            commandes[key] = {"ville": ville, "timbrecde": decimal.Decimal(timbrecde), "qte": qte}
        else:
            commandes[key]["qte"] += qte
            commandes[key]["timbrecde"] = max(commandes[key]["timbrecde"], decimal.Decimal(timbrecde))

# Dictionnaire pour stocker les commandes
commandes = {}

# Lecture de l'entrée standard
for line in sys.stdin:
    process_line(line, commandes)

# Triez le dictionnaire en fonction de la clé "qte" et "timbrecde" de manière décroissante
sorted_commandes = sorted(
    commandes.items(), key=lambda x: (x[1]["qte"], x[1]["timbrecde"]), reverse=True
)

# Créez un DataFrame à partir des données
mydata = []
for codcde, values in sorted_commandes[:100]:
    mydata.append([codcde, values["ville"], values["qte"], values["timbrecde"]])

# Données à mettre dans le DataFrame
df = pd.DataFrame(mydata, columns=["codcde", "ville", "qte", "timbrecde"])
print(df)
# Enregistrez le DataFrame dans un fichier Excel
#df.to_excel("/datavolume1/lot1_exo1.xlsx", index=False) #Répertoire Hbase
df.to_excel("./Output/Lot_1/lot1_exo1.xlsx", index=False)

