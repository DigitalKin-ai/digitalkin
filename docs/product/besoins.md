# Registre des besoins

Source initiale : la note « Pourquoi le CoS ? ». Règles d'entrée et gabarit : voir [Méthode](methode-besoins.md). Les milestones qui couvrent chaque besoin sont dans la [Définition du produit](definition-produit.md#9-milestones-par-feature).

**Statuts.**

- `à qualifier` : le critère mesurable manque. Ce besoin **n'entre dans aucun milestone**.
- `qualifié` : le critère est validé par son porteur.
- `livré` : le critère est démontré sur le pilote.

Les critères proposés ci-dessous sont des **propositions tech**. Le porteur métier les valide ou les corrige, puis le statut passe à `qualifié`.

## Utilisateur final (U)

Besoin racine : résoudre un problème grâce à des compétences précises et fiables.

| ID | Besoin | Critère proposé | Porteur | Priorité | Statut |
|---|---|---|---|---|---|
| U-01 | Confiance dans la réponse | 100 % des affirmations du livrable sont rattachées à une source ou à une règle de l'expert. Le taux de réussite sur le jeu d'évaluation expert est ≥ seuil fixé par l'expert. | | P0 | à qualifier |
| U-02 | Disponibilité | Délai de premier livrable ≤ X min. Taux de missions terminées sans erreur ≥ 99 %. | | P1 | à qualifier |
| U-03 | Coût maîtrisé | Le coût estimé est affiché avant le lancement. Le coût réel reste ≤ estimation + 20 %. | | P0 | à qualifier |
| U-04 | Compréhension du but (précision) | Le brief produit par la maïeutique est validé par l'utilisateur avant exécution, en ≤ N échanges. | | P0 | à qualifier |
| U-05 | Simplicité d'utilisation | Un nouvel utilisateur obtient son premier livrable sans aide. Le taux d'abandon avant brief validé est mesuré. | | P1 | à qualifier |

## Expert (E)

Besoin racine : industrialiser la vente de son expertise.

| ID | Besoin | Critère proposé | Porteur | Priorité | Statut |
|---|---|---|---|---|---|
| E-01 | Respect de son image (brief, traitement, justification, livrable) | L'expert valide le format de brief, le format de livrable et le jeu d'évaluation. Aucune version de service n'est publiée sans passer son jeu d'évaluation. | | P0 | à qualifier |
| E-02 | Fiabilité : reproductible et répétable | Même brief + même version de setup = livrable conforme au même jeu d'évaluation. Chaque livrable porte la version de setup qui l'a produit. | | P0 | à qualifier |
| E-03 | Simplicité de création du service | Un expert crée un service (setup + contrat + évaluations) en ≤ X jours, sans écrire de code. | | P2 | à qualifier |
| E-04 | Découvrabilité du service | Le service est trouvable par le site web, la bibliothèque et une proposition du CoS. Le taux de proposition pertinente est mesuré. | | P1 | à qualifier |

## Plateforme DigitalKin (DK)

Besoin racine : maximiser le revenu en satisfaisant experts et utilisateurs.

| ID | Besoin | Critère proposé | Porteur | Priorité | Statut |
|---|---|---|---|---|---|
| DK-01 | Rétention : l'utilisateur reste pour des usages non prévus | Pourcentage d'utilisateurs venus pour un service qui en lancent un second dans les 30 jours. | | P1 | à qualifier |
| DK-02 | Sentiment d'écoute | Les buts exprimés sont capitalisés et réutilisés dans les missions suivantes. L'utilisateur peut les voir et les corriger. | | P1 | à qualifier |
| DK-03 | Marge par mission | Le coût réel (LLM + tools + services) et le provider sont tracés par mission. La marge par service est calculable. | | P0 | à qualifier |
| DK-04 | Promesses publiques tenues | Pas de partage de contexte entre services ni entre utilisateurs. Chaque décision est traçable (cf. digitalkin.com/tech). | | P0 | à qualifier |

## Besoins techniques internes (T)

Ces besoins sont tous dérivés de besoins U/E/DK et n'existent pas pour eux-mêmes.

| ID | Besoin | Dérive de | Critère proposé | Statut |
|---|---|---|---|---|
| T-01 | Isolation du contexte par mission | DK-04, E-02 | Une mission ne reçoit que son brief et ses fichiers scopés `mission_id`, jamais l'historique de l'orchestrateur. | à qualifier |
| T-02 | Comptabilité complète des coûts | DK-03, U-03 | Chaque appel LLM (y compris résumés et sous-agents) et chaque appel de tool est débité via `CostStrategy`. | à qualifier |
| T-03 | Gestion du thread documentée par archétype | U-01, E-02 | Pour chaque archétype : ce qui est gardé, tronqué ou résumé, et quand l'historique cesse d'être intégral. | à qualifier |
| T-04 | Références sans réécriture | U-03, DK-03 | Un agent cite `<<REF:id>>` et le système substitue le contenu. Gain de tokens mesuré sur le pilote. | à qualifier |
