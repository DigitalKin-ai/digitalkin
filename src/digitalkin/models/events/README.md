# DigitalKin Agent Events

Ce module définit des modèles d'événements Pydantic **autonomes et framework-agnostiques** pour les exécutions d'agents IA.

## Motivation

Avant cette architecture, DigitalKin dépendait directement d'Agno pour les événements d'agent. Cette dépendance créait un couplage fort qui :
- Rendait difficile l'utilisation d'autres frameworks (LangChain, AutoGen, etc.)
- Liait l'évolution de DigitalKin à celle d'Agno
- Compliquait les tests et la maintenance

## Architecture

### Modèles d'événements (`agent_events.py`)

Les événements sont organisés hiérarchiquement :

```
BaseAgentRunEvent (base)
├── RunStartedEvent
├── RunContentEvent
├── RunCompletedEvent
├── RunErrorEvent
├── ReasoningContentDeltaEvent
├── ToolCallStartedEvent
├── ToolCallCompletedEvent
└── ToolCallErrorEvent
```

Tous les événements partagent :
- `event`: Type d'événement (enum `AgentRunEvent`)
- `timestamp`: Horodatage optionnel
- `metadata`: Métadonnées additionnelles optionnelles

### Types d'événements

#### Lifecycle Events
- **RunStartedEvent**: Début d'une exécution d'agent
- **RunCompletedEvent**: Fin réussie d'une exécution
- **RunErrorEvent**: Erreur durant l'exécution

#### Content Events
- **RunContentEvent**: Contenu produit par l'agent (texte, reasoning)

#### Reasoning Events
- **ReasoningContentDeltaEvent**: Contenu de raisonnement en streaming

#### Tool Call Events
- **ToolCallStartedEvent**: Début d'un appel d'outil
- **ToolCallCompletedEvent**: Fin réussie d'un appel d'outil
- **ToolCallErrorEvent**: Erreur durant l'appel d'outil

## Utilisation

### 1. Dans un adaptateur de framework (ex: Agno)

```python
from digitalkin.models.events import RunStartedEvent, RunContentEvent
from agno.run.agent import BaseAgentRunEvent as AgnoEvent

def agno_to_digitalkin_event(agno_event: AgnoEvent) -> BaseAgentRunEvent:
    """Convertit un événement Agno en événement DigitalKin."""
    if agno_event.event == RunEvent.run_started:
        return RunStartedEvent(
            run_id=agno_event.run_id,
            thread_id=agno_event.thread_id,
        )
    elif agno_event.event == RunEvent.run_content:
        return RunContentEvent(
            content=str(agno_event.content),
            reasoning_content=agno_event.reasoning_content,
        )
    # ...
```

### 2. Dans un trigger avec AgUiMixin

```python
from digitalkin.mixins import AgUiMixin
from template_archetype.agents.agno_adapter import agno_to_digitalkin_event

class MessageTrigger(BaseTrigger, AgUiMixin):
    async def execute(self, context, input_data):
        # Stream events from Agno agent
        async for agno_event in agent.arun(message, stream=True):
            # Convert Agno event to DigitalKin event
            dk_event = agno_to_digitalkin_event(agno_event)

            # Send via AgUiMixin (converts to AG-UI protocol)
            await self.send_message(context, dk_event)
```

## Avantages

### 🔓 Découplage
DigitalKin ne dépend plus d'Agno ou de tout autre framework spécifique.

### 🔌 Extensibilité
Support facile de nouveaux frameworks via des adaptateurs :
- Agno → DigitalKin (implémenté)
- LangChain → DigitalKin (à implémenter)
- Custom Agent → DigitalKin (à implémenter)

### 🧪 Testabilité
Les événements peuvent être créés et testés indépendamment.

### 🎯 Clarté
Interface claire et documentée pour les événements d'agent.

## Extension

Pour ajouter un nouveau type d'événement :

1. **Ajouter l'enum** dans `AgentRunEvent` :
```python
class AgentRunEvent(str, Enum):
    # ...
    NEW_EVENT_TYPE = "new_event_type"
```

2. **Créer le modèle** dans `agent_events.py` :
```python
class NewEventTypeEvent(BaseAgentRunEvent):
    event: AgentRunEvent = Field(AgentRunEvent.NEW_EVENT_TYPE)
    custom_field: str = Field(..., description="...")
```

3. **Exporter** dans `__init__.py`

4. **Adapter dans AgUiMixin** (si nécessaire pour AG-UI)

## Roadmap

- [x] **Implémenter les événements de reasoning complets** ✅
  - `ReasoningStartedEvent`
  - `ReasoningContentDeltaEvent`
  - `ReasoningStepEvent`
  - `ReasoningCompletedEvent`
- [ ] Ajouter les événements de step/stage
- [ ] Créer des adaptateurs pour LangChain, AutoGen
- [ ] Ajouter des événements de métriques (latence, tokens)
