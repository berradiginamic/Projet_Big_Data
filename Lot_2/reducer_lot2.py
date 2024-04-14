"""
Script pour analyser les commandes et générer un graphique à secteurs.

Fonctionnement:

1. Lit les données ligne par ligne depuis l'entrée standard.
2. Pour chaque ligne:
   - Extrait les informations clés: code commande, ville, quantité, timbre client.
   - Agrège les données par code commande, en calculant:
       - La quantité totale commandée.
       - Le nombre d'articles sans timbre client.
   - Tri les commandes par quantité totale décroissante.
   - Sélectionne 5% des commandes avec les quantités totales les plus élevées.
3. Crée un DataFrame Pandas avec les informations suivantes:
   - Code commande
   - Ville
   - Nombre d'articles sans timbre
   - Moyenne des quantités de la commande
4. Enregistre le DataFrame dans un fichier Excel nommé "lot2_exo1.xlsx".
5. Génère un graphique à secteurs représentant la répartition des villes par rapport à la moyenne des quantités de commande.
6. Enregistre le graphique dans un fichier PDF nommé "PieChart01.pdf".

Bibliothèques utilisées:

- sys: Pour la gestion des entrées/sorties standard.
- decimal: Pour la gestion des nombres à virgule fixe.
- random: Pour la sélection aléatoire des commandes.
- pandas: Pour la manipulation des données tabulaires.
- matplotlib.pyplot: Pour la création de graphiques.
"""
import sys
import decimal
from random import sample
import pandas as pd
import matplotlib.pyplot as plt

# Fonction pour traiter une ligne de données et mettre à jour le dictionnaire commandes
def process_line(line, commandes):
    columns = line.strip().split("\t")

    # Vérifie si la ligne a le bon format
    if len(columns) == 7:
        key, ville, timbrecde, qte, date, dept, timbrecli = columns

        # Conversion des valeurs en entiers, si possible
        try:
            qte = int(qte)
            timbrecli = int(timbrecli)
        except ValueError:
            qte = 0
            timbrecli = 0

        # Mettre à jour le dictionnaire de commandes
        if key not in commandes:
            commandes[key] = {"ville": ville, "qte_total": qte, "article_count": 1}
        else:
            commandes[key]["qte_total"] += qte
            commandes[key]["article_count"] += 1

# Dictionnaire pour stocker les commandes
commandes = {}

# Lecture de l'entrée standard
for line in sys.stdin:
    process_line(line, commandes)

# Triez le dictionnaire en fonction de la quantité totale des commandes
sorted_commandes = sorted(
    commandes.items(), key=lambda x: x[1]["qte_total"], reverse=True
)

# Sélection aléatoire de 5% des meilleures commandes
command_selected = int(0.05 * 100)
meilleurs_commandes = sorted_commandes[:100]
selection_aleatoire = sample(meilleurs_commandes, command_selected)

# Préparation des données pour le DataFrame
mydata = []
for codcde, values in selection_aleatoire:
    ville = values["ville"]
    qte_total = values["qte_total"]
    article_count = values["article_count"]
    moy_commandes = qte_total / article_count if article_count > 0 else 0
    mydata.append([codcde, ville, qte_total, moy_commandes])

# Création du DataFrame
df = pd.DataFrame(
    mydata, columns=["Code Commande", "Ville", "Nb. Articles sans Timbre", "Moyennes Quantites de la Commande"]
)

# Enregistrement du DataFrame dans un fichier Excel
#df.to_excel("/datavolume1/resultats_lot2_3.xlsx", index=False) # Repertoire Hbase
df.to_excel("./Output/Lot_2/resultats_lot2_3.xlsx", index=False)

# Tracer un diagramme circulaire (pie chart)
plt.pie(df["Moyennes Quantites de la Commande"], labels=df["Ville"], autopct='%1.1f%%', startangle=90)
plt.title('Repartition des villes par rapport a la quantite Avec Hadoop')

# Enregistrer le diagramme circulaire au format PDF
# plt.savefig('/datavolume1/PieChart01.pdf') # Repertoire Hbase
plt.savefig('./Output/Lot_2/resultats_lot2_2.pdf')

# Afficher le diagramme circulaire (dans l'environnement adapté)
#plt.show()
