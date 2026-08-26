"""Registry Toolkit: three agent-facing managers (Tools / Services / Kins).

Each manager exposes a single tool grouping CRUD + search (+ create/load where
relevant) as discriminated actions over the setup and registry services. All three
are setups of a distinct ``module_type`` and share the same actions and plumbing
(see :mod:`digitalkin.community.agno.toolkits.registry.base`).
"""
