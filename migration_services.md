# Migration Guide: Services Singleton Refactoring

## Contexte

Le SDK `digitalkin` a refactorisé ses services pour passer d'instances **per-request** a des **singletons partages**. L'objectif : reduire l'overhead memoire et CPU (9 instances + 7 channels gRPC par requete -> singletons partages).

### Ce qui a change dans le SDK

| Avant | Apres |
|-------|-------|
| `BaseStrategy.__init__(mission_id, setup_id, setup_version_id)` | `BaseStrategy.__init__()` (plus d'IDs) |
| Services recrees a chaque requete | Singletons partages, initialises une fois |
| `context.storage` = `StorageStrategy` | `context.storage` = `BoundStorageStrategy` |
| `context.filesystem` = `FilesystemStrategy` | `context.filesystem` = `BoundFilesystemStrategy` |
| `context.user_profile` = `UserProfileStrategy` | `context.user_profile` = `BoundUserProfileStrategy` |
| `GrpcRegistry(mission_id, setup_id, setup_version_id, client_config)` | `GrpcRegistry(client_config)` |
| Channels gRPC crees par instance | Pool de channels partage (`GrpcClientWrapper._shared_channels`) |

---

## Impact sur les modules downstream

### API publique preservee (zero changement requis)

Les appels suivants **ne changent pas** :

```python
# Toujours identique dans les triggers/mixins
await context.storage.store(collection, record_id, data)
await context.storage.read(collection, record_id)
await context.filesystem.upload_files(files)
await context.filesystem.get_file(file_id, context="mission")
await context.filesystem.get_files(filters)
await context.filesystem.delete_files(filters)
await context.user_profile.get_user_profile()
await context.cost.add(...)
await context.communication.call_module(...)

# Session IDs toujours accessibles
context.session.mission_id
context.session.setup_id
context.session.setup_version_id
```

Les `BoundStrategy` wrappers injectent le `RequestContext` de maniere transparente. **Le code appelant n'a pas besoin de passer `ctx` manuellement.**

### Configuration du module (zero changement requis)

La declaration `services_config_strategies` et `services_config_params` reste identique :

```python
class MyModule(ToolModule[...]):
    services_config_strategies: ClassVar[dict[str, ServicesStrategy | None]] = {}
    services_config_params: ClassVar[dict[str, dict[str, Any | None] | None]] = {
        "storage": {"config": {...}, "client_config": client_config},
        "filesystem": {"client_config": client_config},
        "cost": {"config": {...}, "client_config": client_config},
        "communication": {"client_config": client_config},
        "user_profile": {"client_config": client_config},
        "registry": {"client_config": client_config},
    }
```

---

## Changements requis

### 1. Type hints : `FilesystemStrategy` -> `BoundFilesystemStrategy`

Si du code utilise `FilesystemStrategy`, `StorageStrategy` ou `UserProfileStrategy` comme **type hint** pour un parametre recu depuis `context.filesystem` / `context.storage` / `context.user_profile`, il faut mettre a jour le type.

**Avant :**

```python
from digitalkin.services.filesystem.filesystem_strategy import FilesystemStrategy

class Filesystem:
    def __init__(self, filesystem: FilesystemStrategy) -> None:
        self.filesystem = filesystem
```

**Apres :**

```python
from digitalkin.services.bound_strategies import BoundFilesystemStrategy

class Filesystem:
    def __init__(self, filesystem: BoundFilesystemStrategy) -> None:
        self.filesystem = filesystem
```

> **Note :** Les types `FileFilter`, `FilesystemRecord`, `UploadFileData` restent dans `digitalkin.services.filesystem.filesystem_strategy` — seul le type de la **strategy elle-meme** change.

#### Fichiers concernes dans tool-document-manager

| Fichier | Ligne | Changement |
|---------|-------|------------|
| `utils/filesystem.py` | L6-9 | Remplacer `FilesystemStrategy` par `BoundFilesystemStrategy` dans l'import |
| `utils/filesystem.py` | L17 | `filesystem: FilesystemStrategy` -> `filesystem: BoundFilesystemStrategy` |

#### Fichiers concernes dans tool-rag-methods

| Fichier | Changement |
|---------|------------|
| `services/index_cache.py` | `FilesystemStrategy` -> `BoundFilesystemStrategy` (import + type hints) |
| `services/document_cache.py` | `FilesystemStrategy` -> `BoundFilesystemStrategy` (import + type hints) |
| `services/summarizer.py` | `FilesystemStrategy` -> `BoundFilesystemStrategy` (import + type hints) |
| `triggers/add_documents_trigger.py` | `FilesystemStrategy` -> `BoundFilesystemStrategy` (import + type hints) |
| `triggers/summary_trigger.py` | `FilesystemStrategy` -> `BoundFilesystemStrategy` (import + type hints) |

### 2. Imports des types Bound

Nouveaux imports disponibles :

```python
from digitalkin.services.bound_strategies import (
    BoundFilesystemStrategy,
    BoundStorageStrategy,
    BoundUserProfileStrategy,
)
```

### 3. GrpcRegistry direct (si utilise hors SDK)

Si du code instancie `GrpcRegistry` directement :

**Avant :**
```python
registry = GrpcRegistry(mission_id, setup_id, setup_version_id, client_config)
```

**Apres :**
```python
registry = GrpcRegistry(client_config)
```

---

## Ce qui ne change PAS

- **Appels de methodes sur les services** : meme signature publique
- **`FileFilter`, `FilesystemRecord`, `UploadFileData`, `StorageRecord`** : imports et usage identiques
- **`context.session.mission_id/setup_id/setup_version_id`** : toujours accessibles
- **`context.cost`, `context.communication`, `context.agent`, `context.registry`** : inchanges
- **`CostStrategy`** : reste per-request, aucun changement
- **`ServicesConfig`, `ServicesStrategy`, `ServicesMode`** : imports et usage identiques
- **Mixins (`StorageMixin`, `FilesystemMixin`)** : fonctionnent sans changement

---

## Resume rapide

| Action | Requis ? |
|--------|----------|
| Mettre a jour `digitalkin` dans `pyproject.toml` | Oui |
| Changer les appels `context.storage.store(...)`, etc. | Non |
| Changer `services_config_params` / `services_config_strategies` | Non |
| Mettre a jour les type hints `FilesystemStrategy` -> `BoundFilesystemStrategy` | Oui (pour mypy) |
| Mettre a jour les type hints `StorageStrategy` -> `BoundStorageStrategy` | Oui (si utilise) |
| Mettre a jour les type hints `UserProfileStrategy` -> `BoundUserProfileStrategy` | Oui (si utilise) |
| Changer `GrpcRegistry(m, s, sv, config)` -> `GrpcRegistry(config)` | Oui (si utilise directement) |
| Changer le code des triggers/handlers | Non |
