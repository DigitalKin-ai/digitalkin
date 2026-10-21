# Définition du produit

Documents liés : [Registre des besoins](besoins.md) · [Méthode](methode-besoins.md) · [ADR 0001 — Définition d'un service](adr/0001-definition-service.md)

## 1. En une phrase

> DigitalKin met en relation **un utilisateur qui a un but** et **des services d'experts fiables**. Un **Chief of Staff** comprend le but, le découpe et lance des **missions isolées**. Chaque mission est exécutée par un service dont l'expert garantit la qualité, avec un coût connu.

C'est aussi ce que promet digitalkin.com :

- « operates in accordance with your expertise and standards » ;
- « every decision is traceable and justifiable » ;
- « Nothing is shared across services or users » ;
- facturation en crédits par mission.

## 2. Ce que le produit n'est pas

- **Pas un chat généraliste** dans lequel on importe un nombre illimité de services. Ce modèle mélange les contextes, rend les coûts intraçables et empêche tout engagement envers l'expert.
- **Pas un générateur de prompts.** Le comportement d'un service est écrit ou validé par son expert. Le CoS ne le réécrit jamais.
- **Pas un historique partagé.** Aucun composant ne voit la conversation interne d'un autre.

## 3. Acteurs

| Acteur | Ce qu'il veut | Ce qu'il manipule |
|---|---|---|
| **Utilisateur** | Résoudre son problème avec une réponse fiable, au coût connu | Buts, briefs, livrables |
| **Expert** | Vendre son expertise à grande échelle sans abîmer son image | Services : setup, contrat, jeu d'évaluation |
| **Plateforme DK** | Marge par mission, rétention, promesses tenues | Catalogue, relevés de coûts, métriques |

## 4. Modèle du domaine

```
Utilisateur ─┬─ Mémoire (buts permanents)
             └─ But ──(arbre)── But
                        │
                        └─ Mission ──exécute──► Service ──= Archétype + Setup versionné + Contrat + Jeu d'évaluation
                             │   ▲                                         (propriété : Expert)
                         Brief  Livrable
                             └── Relevé de coût
```

### Objets

| Objet | Définition | Invariants | Dans le SDK aujourd'hui |
|---|---|---|---|
| **Archétype** | Le moteur d'exécution générique : boucle d'agent, framework, outils | Il n'a aucune connaissance métier, qui vient du setup | `ArchetypeModule`, `RegistryModuleType.ARCHETYPE`. Ada et Isaac dans le sandbox |
| **Setup** | La configuration versionnée d'un archétype : prompts, règles, process, modèle, outils autorisés | Une version publiée est immuable. Seul l'expert propriétaire publie | `SetupModel`, `services/setup/` (versions, version courante) |
| **Contrat** | Ce qu'un service attend et ce qu'il garantit : schéma du brief, schéma du livrable, garanties (sources, justification), fourchette de coût | Un service sans contrat n'est pas publiable. Le livrable est validé contre le schéma | **Absent.** `input_format` et `output_format` existent mais au niveau de la **classe** de l'archétype, pas du setup |
| **Jeu d'évaluation** | Des cas « brief → critères attendus », écrits ou validés par l'expert | Il est rejoué à chaque nouvelle version de setup. Un échec bloque la publication | **Absent** |
| **Service** | Archétype + setup versionné + contrat + jeu d'évaluation. C'est l'unité vendue | Il a un propriétaire, l'expert | `RegistryModuleType.SERVICE` existe, sans exécution |
| **But** | Ce que l'utilisateur cherche à obtenir, formulé et validé avec lui | Les buts forment un arbre. Un but feuille donne lieu à une ou plusieurs missions | **Absent** |
| **Brief** | L'entrée d'une mission, conforme au contrat du service | Il est validé par l'utilisateur avant exécution | **Absent** (l'entrée est un chat libre) |
| **Mission** | Une exécution d'un service sur un brief | Contexte isolé : elle ne voit que son brief, ses fichiers et ses références. Elle porte la version de setup utilisée | `mission_id` scope déjà les tâches, le stockage et les fichiers |
| **Livrable** | La sortie d'une mission, conforme au contrat | Il est sourcé, justifié et horodaté, et indique la version du service | **Absent** (la sortie est un flux de chat) |
| **Relevé de coût** | Tous les coûts d'une mission : LLM, tools, sous-services, avec le provider | Complet : aucun appel non débité | `CostStrategy` existe mais il est incomplet (voir §8) |
| **Mémoire utilisateur** | Les buts permanents et les préférences, capitalisés entre sessions | Visible et corrigible par l'utilisateur. Jamais transmise brute à une mission | `user_profile` ne fait que la lecture et l'accès |
| **Référence** | Un pointeur `<<REF:id>>` vers un contenu existant (document, livrable, instruction) | Résolu par le système, pas réécrit en tokens par l'agent | `ToolReference` existe pour les setups ; pas de références de contenu |

## 5. Composants

| Composant | Responsabilité | Ne fait **pas** |
|---|---|---|
| **Chief of Staff** (archétype orchestrateur) | Maïeutique du but, arbre de buts, choix du service, rédaction du brief, présentation du livrable, mise à jour de la mémoire | Exécuter le métier lui-même. Voir l'intérieur d'une mission |
| **Moteur de mission** | Créer la mission, l'isoler, appeler le service, valider le livrable contre le contrat, clôturer le relevé de coût | Choisir le service |
| **Catalogue de services** | Rechercher et proposer des services publiés, avec leur contrat et leur coût estimé | Proposer un service sans contrat |
| **Atelier expert** | Créer et éditer un setup, un contrat et un jeu d'évaluation ; rejouer les évaluations ; publier une version | Publier une version qui échoue à ses évaluations |
| **Comptabilité** | Débiter chaque appel LLM et chaque appel de tool sur sa mission, avec le provider | Estimer. Elle enregistre le réel |
| **Résolveur de références** | Remplacer `<<REF:id>>` par le contenu, dans les droits de la mission | Donner accès à un contenu hors du scope de la mission |

## 6. Qui voit quoi : règles de contexte

C'est la règle qui corrige le problème d'historique énorme et partagé.

| Composant | Voit | Ne voit jamais |
|---|---|---|
| CoS | La conversation avec l'utilisateur, l'arbre de buts, la mémoire, et les **briefs et livrables** des missions | L'historique interne d'une mission, ses appels de tools |
| Mission | Son brief, ses fichiers et références scopés `mission_id`, son setup | La conversation du CoS, la mémoire brute, les autres missions |
| Tool appelé par une mission | Ses arguments | Tout le reste |
| Expert | Ses services, leurs évaluations, des métriques agrégées | Les données d'un utilisateur, sauf consentement explicite |

**Conséquence.** Le contexte du CoS grandit avec le **nombre de livrables**, pas avec le volume des conversations internes. Pour chaque archétype, il faut documenter ce qui est gardé, tronqué ou résumé (besoin T-03).

## 7. Parcours

### Utilisateur : le cas impôts

1. L'utilisateur arrive depuis le site de l'expert : « je veux payer moins d'impôts ».
2. **Maïeutique.** Le CoS pose les questions nécessaires. Le but est reformulé et validé : « optimiser la déclaration 2026 d'un foyer avec revenus fonciers ».
3. **Choix du service.** Le CoS propose le service impôts de l'expert, avec son contrat et son coût estimé.
4. **Brief.** Le CoS remplit le brief selon le contrat. L'utilisateur le valide, ce qui lance la mission.
5. **Mission.** Le service s'exécute en isolation. Le livrable est validé contre le contrat, puis rendu sourcé et justifié, avec le coût réel.
6. **Capitalisation.** Le but permanent (« optimiser la fiscalité du foyer ») est enregistré dans la mémoire.
7. **Rebond.** Le CoS propose un service aligné sur ce but, par exemple la structuration des revenus fonciers. L'utilisateur reste pour une raison qu'il n'avait pas prévue (DK-01).

### Expert : créer un service

1. Il choisit un archétype.
2. Il rédige le setup : identité, règles, process, style, outils.
3. Il définit le contrat : ce qu'il lui faut en entrée, ce qu'il garantit en sortie.
4. Il écrit le jeu d'évaluation.
5. Il rejoue les évaluations. Si elles passent, il publie la version, et le service devient visible dans le catalogue.

### Cycle de vie d'une mission

```
créée ─► brief validé ─► en cours ─► livrable validé ─► clôturée (relevé de coût figé)
                            │              │
                            └─► échec ◄────┘  (livrable non conforme au contrat)
```

## 8. Point de départ technique

Ce qui existe et qu'on garde :

- Setup versionné (`models/module/setup_types.py`, `services/setup/`).
- `mission_id`, qui scope les tâches, le stockage et les fichiers.
- Appel de module à module (`services/communication/` : `call_module`).
- Recherche dans le registry (`services/registry/`).
- `CostStrategy` (`services/cost/`).
- Lecture d'historique à la demande (`community/agno/toolkits/chat_history.py`).

Ce qui doit changer :

- **Contrat au niveau du setup.** Les schémas d'entrée et de sortie sont aujourd'hui portés par la classe de l'archétype, donc identiques pour tous les services d'un même archétype.
- **Coûts.** `ModuleToolkit` ne débite pas `CostStrategy` (note de classe dans `community/agno/module_toolkit.py`). Le hook de coût d'Ada rate les tokens de résumé et de cache-creation. Isaac trace correctement via un callback LiteLLM par mission (`digitalkin-sandbox/modules/archetype-isaac/.../utils/cost_tracker_litellm.py`) : c'est le modèle à remonter dans le SDK.
- **Services exécutables.** Le loader actuel (`community/agno/toolkits/loader/kit.py`) ne charge que des tools dans le chat. Il faut remplacer ce mécanisme par le lancement de missions.
- **Historique.** Pas de budget de contexte ni de compression documentée.

## 9. Milestones par feature

Chaque milestone est **démontrable** : il est terminé quand sa démo passe. L'ordre suit les dépendances, pas un calendrier.

```
A Service contractualisé ─┬─► C Service évalué ─► D Service impôts ─► E CoS orchestrateur ─► F Mémoire & rebond
B Mission isolée & tracée ─┘                                              │
                                                                          └─► G Références
                                                     C ─► H Atelier expert
```

### A — Service contractualisé

- Contrat (schéma du brief, schéma du livrable, garanties, fourchette de coût) attaché à la version de setup.
- Validation du livrable contre le schéma.
- Le livrable porte l'identifiant du service et sa version.
- *Besoins :* E-01, E-02, U-01.
- *Démo :* un livrable non conforme est rejeté ; un livrable conforme indique la version du service qui l'a produit.

### B — Mission isolée et tracée

- Une mission est lancée par appel de module (`call_module`) avec un `mission_id` propre. Elle ne reçoit que son brief.
- Comptabilité complète dans le SDK : callback LLM par mission, débit des tools dans `ModuleToolkit`, provider enregistré.
- Cycle de vie de la mission (§7).
- *Besoins :* T-01, T-02, DK-03, DK-04, U-03.
- *Démo :* deux missions en parallèle pour un même utilisateur, sans fuite de contexte, avec un relevé de coût complet pour chacune.

### C — Service évalué

- Jeu d'évaluation attaché au service.
- Rejeu automatique à chaque nouvelle version de setup.
- Une version qui échoue n'est pas publiable.
- *Besoins :* E-01, E-02, U-01.
- *Démo :* on modifie un prompt du setup, une évaluation casse, la publication est refusée.

### D — Service impôts

- Premier service réel, construit sur A + B + C avec l'expert : contrat, setup, jeu d'évaluation.
- Documentation du comportement du thread de l'archétype utilisé (T-03).
- *Besoins :* U-01, U-02, E-01, E-02, T-03.
- *Démo :* un vrai utilisateur reçoit un livrable impôts qui passe l'évaluation, avec son coût réel.

### E — CoS orchestrateur

- Maïeutique jusqu'à un but validé.
- Arbre de buts.
- Choix du service dans le catalogue, en ne retenant que les services avec contrat.
- Brief rédigé selon le contrat puis validé par l'utilisateur.
- Présentation du livrable.
- Aucune conversation de mission dans le contexte du CoS.
- *Besoins :* U-04, U-05, E-04, DK-04.
- *Démo :* parcours utilisateur §7, étapes 1 à 5. Le contexte du CoS ne contient que des briefs et des livrables.

### F — Mémoire et rebond

- Buts permanents capitalisés, visibles et corrigibles.
- Proposition de services alignés sur ces buts.
- *Besoins :* DK-01, DK-02, E-04.
- *Démo :* parcours §7, étapes 6 et 7. Dans une seconde session, le CoS repart des buts connus.

### G — Références

- `<<REF:id>>` résolu par le système dans le scope de la mission : documents, livrables précédents, instructions d'expert.
- *Besoins :* T-04, U-03.
- *Démo :* le même livrable est produit avec et sans références, et l'écart de tokens est mesuré.

### H — Atelier expert

- Création d'un service (setup, contrat, évaluations) sans code.
- Rejeu des évaluations et publication.
- *Besoins :* E-03, E-04.
- *Démo :* un expert publie un nouveau service seul, et le service apparaît dans le catalogue.

## 10. Décisions ouvertes

| Question | Options | Bloque |
|---|---|---|
| Où vit le contrat ? | Champ dédié du `SetupModel` / objet séparé lié à la version de setup | A |
| Un service peut-il appeler un autre service ? | Non (seul le CoS orchestre) / oui, via une mission enfant avec son propre contrat | B, E |
| Le CoS est-il un archétype comme les autres ? | Oui (setup dédié) / composant plateforme | E |
| Forme de l'évaluation | Critères vérifiables par code / LLM-juge avec grille de l'expert / revue humaine | C |
| Résolution des références | Côté SDK (avant l'appel LLM) / côté tool | G |
| Sort du CoS hybride actuel | Gel / migration vers E | E |
