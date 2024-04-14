#!/usr/bin/env python
"""mapper_lot1.py

Lit les données depuis l'entrée standard, filtre les lignes selon des critères spécifiques,
et imprime les lignes filtrées dans un format tabulaire.

Fonctionnement:

1. Lit les données ligne par ligne depuis l'entrée standard.
2. Pour chaque ligne:
  - Supprime les espaces blancs de début et de fin.
  - S'il y a du contenu sur la ligne:
    - Découpe la ligne en colonnes en utilisant la virgule comme séparateur.
    - Extrait les informations spécifiques des colonnes:
      - code CDE
      - département
      - ville
      - timbre CDE
      - quantité
    - Vérifie si le département est valide (numérique).
    - Convertit la date en objet datetime.
    - Applique les filtres suivants:
      - La date est comprise entre 2006-01-01 et 2010-12-31.
      - Le département est 53, 61 ou 28.
    - Si tous les filtres sont satisfaits, imprime les informations extraites dans un format tabulaire.

Gestion d'erreurs:

- Si la conversion de la date échoue, ignore la ligne et passe à la suivante.
"""

import sys
from datetime import datetime

# Parcours des lignes en entrée depuis stdin
for line in sys.stdin:
    line = line.strip()  # Supprime les espaces vides en début et fin de ligne
    if line:
        # Sépare la ligne en colonnes en utilisant la virgule comme séparateur
        columns = line.split(",")

        # Extraction des données de chaque colonne en supprimant les guillemets
        codecde = columns[6].strip('""')
        dept_str = columns[4].strip('""')
        ville = columns[5].strip('""')
        timbrecde = columns[9].strip('""')
        qte = columns[15].strip('""')

        # Vérification si la colonne de département est un nombre
        if not dept_str.isdigit():
            continue  # Passe à la prochaine itération si le département n'est pas un nombre

        # Conversion du département en entier
        dept = int(dept_str)

        # Extraction de la date de la colonne et définition des dates de début et de fin
        date_str = columns[7].strip('""')
        start_date = datetime.strptime('2006-01-01', "%Y-%m-%d")
        end_date = datetime.strptime('2010-12-31', "%Y-%m-%d")

        try:
            # Vérifie si la date est valide et dans la plage spécifiée
            if date_str.lower() in ('null', 'laval', 'vannes'):
                continue  # Passe à la prochaine itération si la date est invalide
            date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

            # Vérifie si la date est dans la plage spécifiée et que le département correspond à certains critères
            if (start_date <= date <= end_date) and (dept // 1000 in (53, 61, 28)):
                # Affichage des données sélectionnées séparées par des tabulations
                print('%s\t%s\t%s\t%s\t%s\t%s' % (codecde,  ville, timbrecde, qte, date, dept // 1000))

        except ValueError:
            pass  # Passe à la prochaine itération en cas d'erreur de valeur lors de la conversion de la date
