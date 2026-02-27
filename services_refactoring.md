# Refactoring Services : Singletons partages + RequestContext

## Probleme

### Constat

Un load test sur `tool-document-manager` montre un scaling lineaire : **7s pour 1 user, 168s pour 100 users**, alors que le CPU est a seulement 0.3 vCPU. Le temps augmente lineairement non pas a cause du CPU ou du reseau, mais a cause de l'overhead de creation d'objets par requete.

### Cause racine

A chaque requete (`StartModule`), le SDK cree :

- **9 instances de services** (storage, cost, filesystem, registry, communication, agent, identity, snapshot, user_profile)
- **7 channels gRPC** (un par service remote)
- **3 asyncio tasks** (main + heartbeat + signal listener)

```
StartModule
  -> Module.__init__()
    -> _init_strategies()
      -> 9x ServicesConfig.init_strategy(mission_id, setup_id, setup_version_id)
        -> 9 new instances
        -> 7 new grpc.aio.Channel()
```

Chaque channel gRPC implique un handshake TCP + HTTP/2 + potentiellement TLS. Pour 100 requetes concurrentes, cela donne 700 channels, 900 instances de services, le tout en memoire simultanement.

### Pourquoi les IDs etaient dans les constructeurs

L'ancienne architecture stockait `mission_id`, `setup_id`, `setup_version_id` dans chaque instance de service via `BaseStrategy.__init__()`. Ces IDs sont ensuite utilises dans les appels gRPC vers le gateway (ex: `ReadRecordRequest(mission_id=self.mission_id, ...)`).

Le constat cle : **le gateway applique l'isolation par mission/setup cote serveur**. Le SDK client n'a pas besoin d'instances separees pour chaque requete — il suffit de passer les IDs au moment de l'appel.

## Solution

### Principe

Transformer les services en **singletons partages** qui recoivent le contexte de requete a chaque appel, comme un pool de connexions DB partage ou chaque query recoit le `user_id` en parametre.

### Architecture

```
AVANT (per-request) :
  StartModule -> Module.__init__() -> 9x new Service(mission_id, ...) -> 9 instances, 7 channels

APRES (singletons partages) :
  Demarrage serveur -> ServicesConfig.init_shared_services() -> 8 singletons, 7 channels (partages)
  StartModule -> Module.__init__() -> RequestContext(mission_id, ...) + 1 CostStrategy -> ModuleContext
```

### Composants

#### 1. RequestContext (`base_strategy.py`)

Objet leger (`__slots__`) qui porte les IDs de requete :

```python
class RequestContext:
    __slots__ = ("mission_id", "setup_id", "setup_version_id")

    def __init__(self, mission_id: str, setup_id: str, setup_version_id: str) -> None:
        self.mission_id = mission_id
        self.setup_id = setup_id
        self.setup_version_id = setup_version_id
```

#### 2. BaseStrategy sans IDs (`base_strategy.py`)

Le constructeur ne prend plus aucun parametre d'identification :

```python
class BaseStrategy(ABC):
    def __init__(self) -> None:
        """Initialize the strategy."""
```

#### 3. Channel pool partage (`grpc_client_wrapper.py`)

Un seul channel gRPC par adresse cible, partage entre toutes les instances de services. HTTP/2 multiplex les requetes sur un seul channel.

```python
class GrpcClientWrapper:
    _shared_channels: ClassVar[dict[str, grpc.aio.Channel]] = {}

    @classmethod
    def _get_shared_channel(cls, config: ClientConfig) -> grpc.aio.Channel:
        key = config.address
        if key not in cls._shared_channels:
            cls._shared_channels[key] = cls._create_channel(config)
        return cls._shared_channels[key]
```

#### 4. Services Grpc* : `ctx` en parametre de methode

Les methodes qui utilisaient `self.mission_id` prennent maintenant `ctx: RequestContext` en premier parametre :

```python
# Avant
async def _read(self, collection: str, record_id: str) -> StorageRecord | None:
    req = ReadRecordRequest(mission_id=self.mission_id, ...)

# Apres
async def _read(self, ctx: RequestContext, collection: str, record_id: str) -> StorageRecord | None:
    req = ReadRecordRequest(mission_id=ctx.mission_id, ...)
```

Services concernes : `GrpcStorage`, `GrpcFilesystem`, `GrpcUserProfile`.

Services non concernes (n'utilisent pas les IDs) : `Agent`, `Identity`, `Snapshot`, `Registry`, `Communication`.

#### 5. Bound* wrappers (`bound_strategies.py`)

Pour que l'API publique reste identique (`context.storage.store(collection, record_id, data)` sans passer `ctx`), des wrappers injectent le `RequestContext` automatiquement :

```python
class BoundStorageStrategy:
    __slots__ = ("_ctx", "_service")

    def __init__(self, service: StorageStrategy, ctx: RequestContext) -> None:
        self._service = service
        self._ctx = ctx

    async def store(self, collection, record_id, data, data_type="OUTPUT") -> StorageRecord:
        return await self._service.store(self._ctx, collection, record_id, data, data_type=data_type)

    async def read(self, collection, record_id) -> StorageRecord | None:
        return await self._service.read(self._ctx, collection, record_id)
    # ...
```

Trois wrappers crees : `BoundStorageStrategy`, `BoundFilesystemStrategy`, `BoundUserProfileStrategy`.

#### 6. ServicesConfig : shared vs per-request (`services_config.py`)

```python
_PER_REQUEST_SERVICES: frozenset[str] = frozenset({"cost"})

class ServicesConfig:
    def init_shared_services(self) -> None:
        """Appele une fois au demarrage. Cree les singletons."""
        for name in self._valid_strategy_names:
            if name in _PER_REQUEST_SERVICES:
                continue
            strategy_class = strategy[self.mode.value]
            self._shared_services[name] = strategy_class(...)

    def init_strategy(self, name, mission_id, setup_id, setup_version_id):
        """Retourne le singleton partage ou cree un Cost per-request."""
        if name in self._shared_services:
            return self._shared_services[name]
        return strategy_class(mission_id, setup_id, setup_version_id, ...)
```

#### 7. BaseModule._init_strategies : wrapping (`_base_module.py`)

A chaque requete, cree un `RequestContext` et wrape les singletons :

```python
def _init_strategies(self, mission_id, setup_id, setup_version_id):
    ctx = RequestContext(mission_id, setup_id, setup_version_id)
    result = {}
    for service_name in self.services_config.valid_strategy_names():
        service = self.services_config.init_strategy(service_name, mission_id, setup_id, setup_version_id)
        if service_name == "storage":
            result[service_name] = BoundStorageStrategy(service, ctx)
        elif service_name == "filesystem":
            result[service_name] = BoundFilesystemStrategy(service, ctx)
        elif service_name == "user_profile":
            result[service_name] = BoundUserProfileStrategy(service, ctx)
        else:
            result[service_name] = service
    return result
```

### Cas special : CostStrategy

`CostStrategy` reste **per-request** car il accumule des compteurs de cout (`_limits`, `_accumulated`) lies a la session. C'est le seul service dans `_PER_REQUEST_SERVICES`.

## Categorisation des services

| Service | Utilise les IDs | State per-session | Approche |
|---------|----------------|-------------------|----------|
| Agent | Non | Non | Singleton direct |
| Identity | Non | Non | Singleton direct |
| Snapshot | Non | Non | Singleton direct |
| Registry | Non | Non | Singleton direct |
| Communication | Non | Non | Singleton direct |
| Storage | mission_id | _record_locks (dict) | Singleton + ctx param |
| Filesystem | mission_id + setup_id | Non | Singleton + ctx param |
| UserProfile | mission_id | Non | Singleton + ctx param |
| Cost | mission_id + setup_version_id | _limits + _accumulated | Per-request (inchange) |

## Gains attendus

| Metrique | Avant | Apres |
|----------|-------|-------|
| Instances de services par requete | 9 | 1 (CostStrategy uniquement) |
| Channels gRPC total | 7 x N requetes | 7 (partages) |
| Memoire par requete | ~80-130KB | ~10-20KB (RequestContext + BoundStrategy) |
| Handshakes TCP/TLS | 7 x N requetes | 7 (une fois au demarrage) |

## Fichiers modifies

### Infrastructure
- `src/digitalkin/services/base_strategy.py` — RequestContext + BaseStrategy sans IDs
- `src/digitalkin/grpc_servers/utils/grpc_client_wrapper.py` — Channel pool partage
- `src/digitalkin/services/services_config.py` — init_shared_services() + init_strategy() modifie
- `src/digitalkin/services/bound_strategies.py` — **Nouveau** : BoundStorage/Filesystem/UserProfile
- `src/digitalkin/modules/_base_module.py` — _init_strategies wrape les singletons
- `src/digitalkin/models/module/module_context.py` — Types Bound* dans ModuleContext

### Services adaptes (ctx en parametre)
- `src/digitalkin/services/storage/storage_strategy.py`
- `src/digitalkin/services/storage/default_storage.py`
- `src/digitalkin/services/storage/grpc_storage.py`
- `src/digitalkin/services/filesystem/filesystem_strategy.py`
- `src/digitalkin/services/filesystem/default_filesystem.py`
- `src/digitalkin/services/filesystem/grpc_filesystem.py`
- `src/digitalkin/services/user_profile/user_profile_strategy.py`
- `src/digitalkin/services/user_profile/default_user_profile.py`
- `src/digitalkin/services/user_profile/grpc_user_profile.py`

### Services adaptes (constructeur simplifie, pas de ctx)
- `src/digitalkin/services/registry/registry_strategy.py`
- `src/digitalkin/services/registry/grpc_registry.py`
- `src/digitalkin/services/communication/default_communication.py`
- `src/digitalkin/services/communication/grpc_communication.py`

### Cas special
- `src/digitalkin/services/cost/cost_strategy.py` — Garde les IDs dans le constructeur (per-request)

### Initialisation
- `src/digitalkin/core/job_manager/base_job_manager.py` — Appel init_shared_services()
- `src/digitalkin/core/job_manager/taskiq_broker.py` — Appel init_shared_services()
- `src/digitalkin/grpc_servers/module_servicer.py` — GrpcRegistry(client_config)
- `src/digitalkin/grpc_servers/module_server.py` — GrpcRegistry(client_config)
