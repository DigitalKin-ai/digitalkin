# Méthode : du besoin au produit technique

Objectif : que le business et la tech parlent le même langage, et que plus rien n'entre dans le produit sans scope.

## Les règles du jeu

1. **Pas de critère, pas de milestone.** Un besoin entre dans le [registre](besoins.md) avec un ID, un porteur, une priorité et un **critère d'acceptation mesurable**. Tant que le critère manque, le besoin reste `à qualifier` et aucun développement ne démarre.
2. **Pas de besoin, pas de chantier.** Tout chantier technique (ticket, PR, epic) cite au moins un ID de besoin. Un chantier technique pur se rattache à un besoin `T-xx`, lui-même dérivé d'un besoin U, E ou DK.
3. **Les décisions structurantes passent par un ADR** (gabarit ci-dessous). Chaque ADR a un décideur nommé et indique le milestone qu'il bloque. Le décideur tranche avant que ce milestone démarre, même sans consensus.
4. **Pilote d'abord.** Toute capacité est démontrée sur le cas impôts avant d'être généralisée. Si elle ne sert pas le pilote, elle attend.
5. **Démo, pas slides.** Un milestone est validé par une démonstration sur le pilote face aux critères des besoins concernés.
6. **Scope fermé par milestone.** Un nouveau besoin en cours de milestone va dans le registre pour le milestone suivant. Il n'entre dans le milestone en cours que si on en retire autre chose, de taille équivalente.

## Rôles

| Rôle | Responsabilité |
|---|---|
| Porteur de besoin (business/expert) | Formule le besoin et **valide le critère mesurable** |
| Responsable technique | Traduit le besoin en capacités et en ADR, estime, refuse ce qui n'a pas de critère |
| Décideur ADR | Tranche avant le démarrage du milestone bloqué |
| Expert pilote (impôts) | Fournit le format de brief, le format de livrable et le jeu d'évaluation |

## Cadence

- **À chaque fin de milestone : revue produit + tech.** Démo du milestone, revue du registre (nouveaux besoins, qualification des critères), décisions ADR qui bloquent le milestone suivant.
- **Hebdomadaire : point technique interne.** Avancement, risques.

## Chaîne de traduction

```
Besoin (U/E/DK) ──► Critère mesurable ──► Capacité produit ──► Composant technique ──► Milestone
     U-01               « 100 % sourcé »      Livrable justifié     Contrat de livrable    A, C
                                                                   + jeu d'évaluation
```

Chaque maillon est écrit noir sur blanc. Si l'un d'eux manque, la discussion revient au maillon précédent.

## Gabarit de besoin

```markdown
| ID | Besoin | Critère d'acceptation mesurable | Porteur | Priorité (P0-P2) | Statut |
```

- **P0** : bloque le pilote.
- **P1** : nécessaire avant d'ouvrir un 2e service.
- **P2** : industrialisation.

## Gabarit d'ADR

Fichier : `docs/product/adr/NNNN-titre.md`

```markdown
# ADR NNNN — Titre

- Statut : proposé | accepté | rejeté | remplacé par NNNN
- Décideur : <nom>
- Bloque le milestone : <lettre>
- Besoins concernés : U-xx, E-xx, ...

## Contexte
## Options considérées
## Décision
## Conséquences (ce qui devient possible / impossible / à faire)
```
