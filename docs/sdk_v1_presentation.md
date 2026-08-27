---
marp: true
theme: gaia
paginate: true
size: 16:9
style: |
  section { font-size: 25px; line-height: 1.5; }
  h1 { font-size: 46px; }
  h2 { font-size: 34px; }
  h3 { font-size: 24px; }
  li { font-size: 23px; }
  table { font-size: 20px; }
  blockquote { font-size: 23px; }
  pre { font-size: 18px; line-height: 1.3; }
  .term { color: #5b8def; font-weight: bold; }
  .avant { color: #c0392b; font-weight: bold; }
  .apres { color: #27ae60; font-weight: bold; }
---

<!--
SDK v1 — présentation équipe non technique. ~15 min, 17 diapos.
Sources : docs/sdk_redis_platform_talk.md (technique, vérifié) +
~/Downloads/ajout-securite.slides.md (sécurité node-services-provider) +
src/digitalkin/grpc_servers/interceptors/request_ids.py (permissions SDK).
Ton : sobre, affirmatif, sans familiarité. Aucune invention.
-->

# DigitalKin SDK v1

## Une plateforme construite par soustraction

<br>

Architecture · Comparaison · Mesures · Sécurité · Validation

<!-- ~30s -->

---

## La thèse

La v0.4.5 était une librairie : chaque agent était appelé directement.
La v1 est une plateforme : un point d'entrée unique — la **Gateway** — et une base
de données live — **Redis** — entre les clients et les agents.

Ce résultat n'a pas été obtenu en ajoutant des systèmes, mais en en supprimant.

> La simplicité n'est pas un point de départ. C'est un résultat.

**La v1 n'est pas compatible avec la v0.4.5.** L'API a entièrement changé :
tout client existant doit migrer. Il n'existe pas de mode de transition.

<!-- ~1 min. Thèse + rupture, posées d'entrée. -->

---

## Le vocabulaire

| Composant | Rôle |
|---|---|
| **Python / SDK** | Le langage et le kit avec lesquels chaque agent est construit. |
| **gRPC** | Le protocole d'appel entre applications : rapide, typé, avec **streaming** — les résultats sont transmis au fil de leur production. |
| **Protobuf** | Le format des messages : binaire, compact, régi par un contrat partagé entre les deux parties. |
| **Redis** | Base de données en mémoire : le journal des résultats, l'état des tâches, les signaux. |
| **Gateway** | Notre composant : le point d'entrée unique. Elle oriente, elle ne stocke rien. |
| **Docker** | L'unité de déploiement standard, identique en local et sur serveur. |

<!-- ~1.5 min -->

---

## L'architecture

```
 Client ──────► Gateway ──────► Redis ──────► Agent (module)
    ▲                             │
    └──── résultats, en direct ◄──┘
```

- Le client s'adresse à la Gateway, jamais à un agent.
- Tout ce qui doit survivre — résultats, état, signaux — réside dans Redis.
- Les agents sont invisibles les uns pour les autres.

> <span class="term">Terme exact :</span> le flux de résultats est un **Redis Stream** —
> un journal ordonné, qui survit aux crashs et se relit depuis n'importe quel point.

<!-- ~1 min -->

---

## Cinq changements structurels

| | <span class="avant">v0.4.5</span> | <span class="apres">v1</span> |
|---|---|---|
| **Communication** | appel direct à l'agent | tout transite par Gateway + Redis |
| **Mémoire** | aucune — un crash efface le travail | journal durable, flux rejouable |
| **Arrêt** | fonctionnel mais grégaire | ciblé par tâche, ~2 ms |
| **API** | une par agent | une seule, trois actions |
| **Dépendances** | quatre systèmes lourds | Redis, seul |

<!-- ~1 min. Chaque ligne est reprise dans les diapos suivantes. -->

---

## 1 — Le découplage

<span class="avant">v0.4.5 :</span> le client appelait l'agent en direct. Agent
indisponible, connexion interrompue : la requête était perdue.

<span class="apres">v1 :</span> la Gateway écrit la demande dans Redis ; l'agent la
prend quand il est prêt ; le client lit les résultats quand il le décide.

> Deux systèmes qui ne s'attendent pas ne peuvent pas se bloquer mutuellement.

<span class="term">Terme exact :</span> **découplage producteur/consommateur**. Il fonde
l'isolation des agents et la **scalabilité horizontale** — toute Gateway peut servir
tout client.

<!-- ~1 min -->

---

## 2 — La durabilité

<span class="avant">v0.4.5 :</span> les résultats partaient directement vers le client.
Une déconnexion au mauvais moment imposait de tout recommencer.

<span class="apres">v1 :</span> chaque fragment de réponse est écrit dans Redis avant
d'être lu. Le client peut se déconnecter, puis reprendre le flux où il l'avait laissé.

> Ce qui est écrit avant d'être lu ne peut pas être perdu.

<span class="term">Terme exact :</span> **resume via `from_seq`** — chaque message est
numéroté ; le client redemande à partir du n° X. Un numéro manquant est une perte
**détectée**, jamais silencieuse.

<!-- ~1 min -->

---

## 3 — Les signaux

<span class="avant">v0.4.5 :</span> l'arrêt fonctionnait, mais il était grégaire — il ne
visait pas une tâche précise.

<span class="apres">v1 :</span> l'annulation désigne une tâche, atteint l'agent quelle
que soit la machine où il s'exécute, en 1 à 2 ms, sur un canal distinct du flux de
données. Les ordres urgents sont prioritaires.

<span class="term">Terme exact :</span> **pub/sub Redis**, hors du chemin des données —
le contrôle et les résultats ne se disputent jamais la même voie.

<!-- ~45s -->

---

## 4 — Une API, moins de systèmes

<span class="avant">v0.4.5 :</span> chaque agent exposait sa propre API — logique métier
et transport indissociables. La montée en charge exigeait Taskiq, RabbitMQ, SurrealDB
et un saut réseau interne.

<span class="apres">v1 :</span> la Gateway expose la seule API — trois actions :
*démarrer une tâche, suivre son flux, envoyer un signal*. L'agent ne contient que du
métier. Les quatre systèmes annexes ont été supprimés.

> Chaque système supprimé est une panne qui ne se produira pas.

<span class="term">Terme exact :</span> **separation of concerns** — le transport évolue
sans toucher aux agents, et réciproquement.

<!-- ~1 min -->

---

## Ce que l'architecture garantit

| Garantie | Mécanisme |
|---|---|
| Aucune perte de travail | journal écrit avant lecture |
| Reprise en cours de tâche | rejeu depuis un numéro de message |
| Annulation fiable et ciblée | signaux inter-machines, ~2 ms |
| Agents isolés | communication exclusivement via la plateforme |
| Intégration uniforme | une porte, trois actions, un format unique |
| Tenue sous surcharge | **admission control**, **backpressure**, **circuit breaker**, **bulkhead** |
| Exploitation allégée | une seule dépendance externe : Redis |

> Les erreurs sont des messages du flux comme les autres : visibles, enregistrées,
> rejouables.

<!-- ~1.5 min -->

---

## La mesure

Plateforme hébergée, 50 utilisateurs simultanés, 20 minutes :

- **2 734 conversations complètes, zéro échec**
- Débit et latence stables du début à la fin
- Transport : premier octet en **86 ms** en médiane

Une réponse complète prend ~21 s : l'essentiel est le temps de réflexion du modèle IA
(~14 s avant son premier mot). La plateforme ajoute moins d'un dixième de seconde.

> La latence perçue appartient au modèle. L'infrastructure, elle, est mesurée.

<!-- ~1 min -->

---

## Sécurité — le modèle

Chaque appel aux services répond désormais à deux questions :

- **Qui appelle ?** — l'appelant présente son **`task-id`** ; le serveur le résout en
  identité : `{ utilisateur, mission, organisation }`.
- **En a-t-il le droit ?** — vérifié ressource par ressource, avant tout accès.

La fenêtre est courte : le `task-id` n'est valide que pendant que la tâche est active.
Une tâche terminée ne donne plus aucun accès.

<span class="term">Terme exact :</span> décision centralisée, **fail-closed** — tout cas
inconnu est un refus. Appliqué sur les six services gRPC.

<!-- ~1.5 min. L'identité vit le temps de la tâche, pas davantage. -->

---

## Sécurité — les règles par ressource

Deux modes de contrôle : **owner** (propriétaire uniquement) et **access**
(propriétaire, ou même organisation, ou partage explicite).

| Ressource | Règle |
|---|---|
| **Missions** | strictement privées — owner uniquement |
| **Setups** (configurations d'agent) | `PRIVATE` (owner) · `INTERNAL` (organisation) · `PUBLIC` (partage explicite) |
| **Modules** (agents) | partagés à l'organisation |
| **Fichiers & storage** | héritent de leur contexte : mission → owner ; setup → règles du setup |

Garanties vérifiées : refus par défaut, requêtes protégées contre l'injection, secrets
jamais journalisés en clair.

<!-- ~1.5 min -->

---

## Sécurité — ce que le SDK prend en charge

Côté agent, la sécurité n'est pas un effort : elle est **automatique**.

- Chaque appel sortant porte l'identité de la tâche — `x-task-id`, `x-setup-id`,
  `x-mission-id` — injectée par un **interceptor** gRPC. Un développeur ne peut ni
  l'oublier, ni la contourner : elle accompagne l'appel, systématiquement.
- Un refus du serveur arrive dans le SDK comme une erreur explicite —
  `PermissionDeniedError` — jamais masqué, jamais dégradé en silence.
- La visibilité d'un setup (`PRIVATE` / `INTERNAL` / `PUBLIC`) se gère par le SDK ;
  toute valeur invalide est rejetée.

> La règle de droit la plus fiable est celle que personne n'a besoin d'appliquer à la main.

<!-- ~1 min. Le point : la propagation d'identité est structurelle, pas disciplinaire. -->

---

## La limite actuelle

La vérification s'arrête aujourd'hui à l'équipe technique : tests automatiques,
intégration sur Redis réel, test de charge. Au-delà, personne ne valide :

- le parcours complet d'un **utilisateur final**, end-to-end
- sur l'**environnement de production**
- avec de vrais comptes — donc les permissions décrites ci-dessus

Certains défauts n'existent que là : configuration réseau du serveur, usages
simultanés réels, règles de droits. Un incident récent l'a confirmé : deux défauts
invisibles en local, révélés uniquement par un usage réel.

> Un système n'est pas validé par ceux qui l'ont construit.

<!-- ~1.5 min -->

---

## Proposition : la validation en production

1. Des **scénarios end-to-end écrits ensemble** : démarrer, se déconnecter et
   reprendre, annuler, changer de configuration
2. Une **passe de permissions** : chaque rôle vérifie ce qu'il voit — et ce qu'il ne
   doit pas voir
3. Une passe **avant chaque release** — courte, systématique, tracée

Le rôle de l'équipe non technique est central : tester en utilisateur réel, sans
connaissance du fonctionnement interne — c'est précisément ce regard qui révèle ce que
les développeurs ne voient plus.

<!-- ~1 min -->

---

## À retenir

- **v1 est une plateforme** : une porte, trois actions, un journal durable.
  **Incompatible avec la v0.4.5** — la migration est obligatoire.
- Sa valeur tient à des changements de structure : découplage, durabilité, signaux
  ciblés, API unique — et à la suppression de quatre systèmes.
- **Mesurée** : 2 734 conversations sous charge, zéro échec.
- **Sécurisée par tâche** : identité éphémère, refus par défaut, règles par ressource,
  propagation automatique dans le SDK.
- Il reste une étape, et elle nous concerne tous : la **validation en production**,
  en utilisateur réel, permissions comprises.

<!-- ~45s -->
