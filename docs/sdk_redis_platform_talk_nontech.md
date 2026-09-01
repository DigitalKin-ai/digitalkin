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
  table { font-size: 21px; }
  blockquote { font-size: 23px; }
  pre { font-size: 18px; line-height: 1.3; }
  .term { color: #5b8def; font-weight: bold; }
---

<!--
VERSION NON-TECHNIQUE de docs/sdk_redis_platform_talk.md.
Public : produit, business, direction — sans bagage d'ingénierie.
Approche : explications en français simple portées par une analogie, avec les VRAIS
termes techniques présentés en encarts « 🔧 terme réel » pour que le public reparte
avec un vocabulaire exact et réutilisable. Tout ce qui est dit est vrai (aucune invention).
Durée visée : ~15 minutes. ~13 diapos ≈ 1 min chacune + marge.
-->

# DigitalKin, sous le capot

## Comment marche notre plateforme

<br>

Le parcours : **ce qui a changé** · **comment ça marche** · **est-ce que ça tient la charge** · **ce que ça change pour nous**

<!-- ~30s. Promesse : vous repartez en comprenant ET capable d'en parler. -->

---

## L'idée centrale

Nous avons transformé une **boîte à outils** en **plateforme**.

- **Avant :** chaque agent était un outil autonome auquel on parlait directement.
- **Aujourd'hui :** un **standard unique** plus une **base de données live** relient tout.

Et on y est arrivé en **retirant** de la complexité — pas en en rajoutant.

> <span class="term">🔧 Terme réel :</span> on est passé d'une *librairie* (du code qu'on appelle
> directement) à une *plateforme orientée services* (une passerelle devant, une colonne
> vertébrale de messages derrière).

<!-- ~1 min. La thèse. L'encart donne les deux premiers vrais mots. -->

---

## L'analogie : un standard et une base live

Imaginez le central d'une grande organisation :

```
   Vous           Le standard         La DB live        Agent
 (client)  ──────►  (Gateway)  ──────►  (Redis)  ──────► (module)
     ▲                                      │
     └────────  résultats, au fil de l'eau ◄┘
```

- **Le standard → la Gateway** — le point d'entrée unique ; il oriente, il ne garde rien
- **La base de données live → Redis** — tout y est gardé, dans l'ordre, et relu en temps réel
- **Les agents → les modules** — les ouvriers qui font le vrai travail

> <span class="term">🔧 Terme réel :</span> cette « base live » est **en mémoire** (ultra-rapide) ;
> le flux lui-même est un **Redis Stream** — un journal en ajout-seul (*append-only*) qui survit
> aux crashs et se relit depuis n'importe quel point, **dans la limite de sa rétention**.

<!-- ~1.5 min. L'analogie porte tout le talk. On plante « Redis Stream ». -->

---

## Ce qui n'allait pas avant

L'ancienne méthode revenait à **appeler directement le poste d'un ouvrier** :

- 📞 Occupé ou ligne coupée → votre requête **disparaissait**.
- 🧠 **Aucune mémoire :** un crash en cours de tâche = travail **perdu**.
- 🛑 Le **bouton « stop »** ne marchait pas de façon fiable entre machines.
- 🏗️ Pour passer à l'échelle, il fallait **quatre systèmes lourds en plus**.

> <span class="term">🔧 Terme réel :</span> ces quatre-là étaient **Taskiq, RabbitMQ, SurrealDB**
> et un **gRPC loopback** — une file de messages, une base de données, un saut réseau interne.
> Tout est désormais **supprimé**.

<!-- ~1.5 min. Nommer les systèmes supprimés est vrai ET précis, donc crédible. -->

---

## La nouvelle méthode, en une image

1. Vous parlez à **un seul** standard — toujours la même porte.
2. Il écrit votre requête dans la **base live**.
3. Un agent la récupère, travaille, et **renvoie les résultats** — morceau par morceau.
4. Vous lisez ces résultats **au fur et à mesure**, en direct.

Le standard lui-même **ne stocke rien**. Tout l'important vit en sécurité dans la base live.

> <span class="term">🔧 Terme réel :</span> la connexion est un **stream bidirectionnel** — vous et
> le serveur échangez des messages sur une même ligne ouverte, au lieu d'un simple aller-retour requête/réponse.

<!-- ~1 min. « Une seule porte » + « résultats en direct ». On plante « stream bidirectionnel ». -->

---

## Trois choses que vous gagnez

1. **Rien ne se perd** — le travail est sauvegardé dès qu'il est produit.
2. **Reconnexion à un flux en cours** — si la connexion saute, vous rouvrez le flux de *cette
   tâche* et **rejouez ses messages**, tant qu'ils sont dans la fenêtre de rétention (≈ les 1000
   derniers, ≈ 10 min). Au-delà, c'est perdu.
3. **Un vrai bouton stop** — l'annulation est instantanée, même entre machines.

> <span class="term">🔧 Termes réels :</span> **durabilité** (sauvé avant même que vous le lisiez),
> **resume via `from_seq`** (rejouer le flux d'une tâche depuis un point donné, dans la limite de
> la rétention Redis), et **signaux inter-processus** (un *cancel* qui atteint l'agent où qu'il
> tourne — en ~**1–2 millisecondes**).

<!-- ~1.5 min. Les gains côté client. Trois vrais mots, tous défendables. -->

---

## Pourquoi une base de données live au milieu ?

Parce que l'**ouvrier** et le **client** n'ont pas besoin d'être là au même moment.

- L'agent continue de produire même si **vous êtes parti**.
- Vous pouvez **revenir** et reprendre le flux encore disponible.
- Les agents restent **isolés** — l'un ne peut pas atteindre ni casser l'autre.

> <span class="term">🔧 Terme réel :</span> c'est le **découplage producteur/consommateur**. Ils
> communiquent **via Redis**, jamais en direct — ce qui permet aussi de **scaler horizontalement**
> (ajouter des standards en parallèle ; n'importe lequel peut servir n'importe quel client).

<!-- ~1 min. « Découplage producteur/consommateur » + « scalabilité horizontale ». Vrai et impressionnant. -->

---

## Encaisser la surcharge, proprement

La plateforme absorbe la surcharge sans casser. Les vrais mots pour décrire le *comment* :

| En français simple | 🔧 Le terme réel |
|---|---|
| « Ne pas accepter plus qu'on ne peut traiter » | **admission control** |
| « Ralentir l'écrivain si le lecteur ne suit pas » | **backpressure** |
| « Arrêter d'appeler un service mort au lieu d'attendre » | **circuit breaker** |
| « Plafonner chaque service pour qu'un seul n'affame pas les autres » | **bulkhead** |

> Les quatre sont réels, les quatre sont en prod aujourd'hui — et c'est *toute* la couche de
> sécurité. Gardée petite exprès : chaque survivant **mérite sa place sur le chemin critique**.

<!-- ~1.5 min. La diapo « se sentir tech » : 4 vrais mots, réutilisables, honnêtement glosés. -->

---

## On a gagné en supprimant

La plus grosse décision a été la **soustraction**.

- Supprimé **quatre systèmes lourds** (la file de messages, la base, le saut réseau interne…).
- Supprimé beaucoup de machinerie maison difficile à maintenir.

Résultat : **plus léger, plus rapide, plus simple à exploiter** — moins de pièces qui peuvent casser.

> <span class="term">🔧 Terme réel :</span> on a tout remplacé par une **API à 3 actions** au-dessus
> de Redis. Si une fonctionnalité ne se justifiait pas face à ça, elle a été coupée.

<!-- ~1 min. Contre-intuitif et mémorable : progresser en retirant. -->

---

## Est-ce que ça tient vraiment la charge ?

Un **test de charge en conditions réelles**, contre la plateforme hébergée en production :

- **50 utilisateurs simultanés**, pendant **20 minutes d'affilée**
- **2 734 conversations complètes** · **zéro échec** 🎯
- **Vitesse stable du début à la fin** — aucun ralentissement

> <span class="term">🔧 Chiffres réels :</span> **2,28 appels/s, ~62 messages/s**, et le **p50 du
> « time to first byte »** de notre transport ≈ **86 ms** (moins d'un dixième de seconde).
> *(p50 = le cas typique, au milieu du peloton.)*

<!-- ~1.5 min. La diapo preuve. Pause sur « zéro échec ». Définir p50 pour qu'ils le réutilisent. -->

---

## Alors, où passe le temps ?

Une réponse IA complète prend environ **21 secondes**.

- La quasi-totalité, c'est le **modèle IA qui réfléchit** (son « time to first token » ≈ 14 s).
- **Notre plateforme** ajoute bien moins d'un **dixième de seconde**.

> Traduction : la plateforme est **rapide**. L'attente ressentie vient du **modèle**, pas du
> transport — <span class="term">🔧</span> la traîne mesurée est de la **latence côté modèle**, pas la nôtre.

<!-- ~1 min. Recadre les « 21 secondes » pour que personne n'accuse la plateforme. -->

---

## Les compromis, en toute honnêteté

Rien n'est gratuit. Ce qu'on **paie** :

| On assume… | …et ça vaut le coup parce que |
|---|---|
| Un système central dont on dépend — un <span class="term">🔧 point unique de défaillance (SPOF)</span> | C'est lui qui donne durabilité + mémoire ; on l'exploite en **HA Redis** (haute disponibilité) |
| Quelques **millisecondes** par étape | Invisibles face aux **secondes** de l'IA |

Ce qu'on **récupère** : durabilité, reconnexion, isolation, vrai *cancel* inter-processus.

<!-- ~1 min. Montre la maturité : on connaît les coûts (SPOF, HA) et on les a choisis. -->

---

## Ce que ça change pour nous

- Une **façon simple et propre de se brancher** — une porte, trois actions.
- Une plateforme qu'on peut **faire grandir et sur laquelle bâtir des produits**, pas juste une démo.
- **Plus rapide et plus légère** qu'avant — parce qu'on a retiré, pas empilé.

<br>

> **Nous en avons fait une plateforme en la simplifiant.**

<!-- ~1 min. On conclut sur la thèse. -->
