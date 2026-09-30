# ADR 0001 — Définition d'un service : setup versionné + contrat

- Statut : **proposé**
- Décideur : _à nommer_
- Bloque le milestone : A (Service contractualisé)
- Besoins concernés : U-01, U-03, U-04, E-01, E-02, DK-03, DK-04, T-01, T-02

## Contexte

Deux visions s'opposent :

- **Prompt.** Un service est un prompt que le CoS génère ou injecte dans une conversation commune. On importe autant de services que voulu dans un chat unique.
- **Setup.** Un service est une configuration versionnée d'un archétype, exécutée comme un module à part.

Le CoS hybride actuel suit la première vision. Il s'appuie sur Ada et sur les toolkits registry/loader du SDK (`community/agno/toolkits/defaults.py`, `registry/`, `loader/kit.py`). Conséquences observées :

- **Coûts non tracés.** `ModuleToolkit` ne débite pas `CostStrategy` (`community/agno/module_toolkit.py`, note de classe). Le hook de coût d'Ada rate les tokens de résumé de session et de cache-creation.
- **Contexte partagé.** Un historique unique et volumineux est partagé entre services. C'est contraire à la promesse publique « Nothing is shared across services or users » (digitalkin.com/tech).
- **Aucun engagement possible envers l'expert.** Le comportement d'un service dépend du reste de la conversation. Il n'est donc ni reproductible ni évaluable, ce qui bloque E-01 et E-02.
- **Chargement limité.** Le loader ne charge que des tools : les services sont cherchables mais pas exécutables.

## Options considérées

1. **Prompt dans un chat commun** (statu quo). Rejeté pour les raisons ci-dessus.
2. **Setup seul.** C'est déjà le modèle SDK (`SetupModel` versionné, `services/setup/`). Il manque un contrat : rien ne dit ce qu'un service attend en entrée ni ce qu'il garantit en sortie.
3. **Setup versionné + contrat** (proposé).

## Décision proposée

**Un service, c'est :**

| Élément | Contenu | Existant SDK |
|---|---|---|
| Archétype | Le moteur d'exécution (Ada, Isaac, …) | `ArchetypeModule` |
| Setup versionné | Prompts, règles, process, outils autorisés, modèle. **Écrit ou validé par l'expert**, jamais généré à la volée | `SetupModel`, `SetupVersionData` |
| Contrat de brief | Schéma de l'entrée attendue | à ajouter |
| Contrat de livrable | Schéma de la sortie, justification et sources obligatoires | à ajouter |
| Jeu d'évaluation | Cas brief → critères attendus, validés par l'expert, rejoués à chaque nouvelle version | à ajouter |
| Coût estimé | Fourchette par mission | `services/cost/` |

**Une mission** est l'exécution d'un service sous un `mission_id` propre. Elle ne reçoit que son brief et ses fichiers scopés, jamais l'historique de l'appelant (T-01). Tous ses appels LLM et tools sont débités sur ce `mission_id` (T-02). Le callback LiteLLM d'Isaac (`digitalkin-sandbox/modules/archetype-isaac/src/archetype_isaac/utils/cost_tracker_litellm.py`) sert de modèle.

**Le CoS** est un orchestrateur. Son rôle :

- conduire la maïeutique ;
- tenir l'arbre de buts ;
- choisir un service ;
- rédiger un brief conforme au contrat ;
- recevoir le livrable.

Il ne voit que des briefs et des livrables, jamais l'historique interne d'une mission.

## Conséquences

- **Devient possible :**
  - engagement de qualité envers l'expert (évaluation par version) ;
  - coût et marge par mission ;
  - promesses publiques tenues ;
  - contexte du CoS borné (des livrables, pas des conversations).
- **Devient impossible :** importer « n'importe quel service » dans un chat sans contrat. Un service sans contrat n'est pas publiable.
- **À faire :**
  - ajouter le contrat à la version de setup (milestone A) ;
  - remonter la comptabilité de coûts dans le SDK (milestone B) ;
  - écrire le premier contrat et le premier jeu d'évaluation avec l'expert impôts (milestones C et D).
