"""
Ce script analyse un ensemble de données HBase pour identifier le client ayant payé les frais de timbrecde les plus élevés.

## Fonctionnalités :

- Se connecte à une instance HBase spécifiée.
- Scanne une table HBase spécifiée.
- Extrait les données des colonnes 'nomcli', 'prenomcli', 'timbrecde'.
- Localise le client ayant payé les frais de timbrecde les plus élevés.
- Calcule le nombre total de commandes et la somme des quantités pour ce client.
- Affiche les informations du client avec les frais de timbrecde les plus élevés.
- Exporte les informations du client vers un fichier Excel.
- Ferme la connexion à HBase.

## Prérequis :

- Avoir installé les modules `happybase` et `pandas`.
- Avoir une instance HBase accessible.

## Exemple d'utilisation :

```python
python hbase_lot3_Q3.py


## Remarques :

- Le script peut être facilement modifié pour analyser d'autres colonnes de la table HBase.
- Il est possible d'ajouter des fonctionnalités pour enregistrer les résultats dans un fichier ou une base de données.
"""

import happybase
import pandas as pd

# Configurer la connexion à HBase
connection = happybase.Connection('node175910-env-1839015-etudiant18.sh1.hidora.com', 11560)  # Mettre à jour avec votre hôte et port HBase
connection.open()

# Sélectionner la table HBase
table_name = 'dataFromagerie'  # Mettre à jour avec le nom de votre table HBase
table = connection.table(table_name)

# Initialiser un dictionnaire pour stocker les informations du client avec les frais de timbrecde les plus élevés
max_timbre_client = {
    'nomcli': None,
    'prenomcli': None,
    'nombre_commandes': 0,
    'somme_quantites': 0,
    'max_timbrecde': 0
}

# Scanner les lignes de la table HBase
for key, data in table.scan():
    # Extraire les données de chaque ligne
    nomcli = data[b'cf:nomcli'].decode()
    prenomcli = data[b'cf:prenomcli'].decode()
    timbrecde = data[b'cf:timbrecde'].decode()

    try:
        # Vérifier si les frais de timbrecde sont supérieurs au maximum actuel
        if float(timbrecde) > max_timbre_client['max_timbrecde']:
            max_timbre_client['nomcli'] = nomcli
            max_timbre_client['prenomcli'] = prenomcli
            max_timbre_client['max_timbrecde'] = float(timbrecde)

            # Mettre à jour le nombre total de commandes et la somme des quantités pour le client actuel
            max_timbre_client['nombre_commandes'] += 1
            max_timbre_client['somme_quantites'] += 1

    except ValueError:
        pass  # Passer à l'itération suivante en cas d'erreur de valeur lors de la conversion des frais de timbrecde

# Imprimer les informations du client avec les frais de timbrecde les plus élevés
print(f"Client avec les frais de timbrecde les plus élevés : {max_timbre_client['nomcli']} {max_timbre_client['prenomcli']}")
print(f"Nombre de commandes : {max_timbre_client['nombre_commandes']}")
print(f"Somme des quantités d'objets : {max_timbre_client['somme_quantites']}")
print(f"Frais de timbrecde les plus élevés : {max_timbre_client['max_timbrecde']}")

# Fermer la connexion à HBase
connection.close()

# Créer un DataFrame pandas à partir des informations du client
df = pd.DataFrame([max_timbre_client])

# Exporter le DataFrame vers un fichier Excel
#excel_filename = '/datavolume1/resultats_lot3_3.xlsx' # Répertoire Hbase
excel_filename = '../Output/Lot_3/resultats_lot3_3.xlsx'

df.to_excel(excel_filename, index=False)

print(f"Les informations du client ont ete exportees dans le fichier Excel : {excel_filename}")
