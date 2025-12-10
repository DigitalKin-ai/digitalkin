"""Built-in SDK triggers.

These triggers are automatically registered without requiring discovery.
They provide standard functionality available to all modules.

Note: These are internal triggers. External code should not import them directly.
Use UtilityRegistry.get_builtin_triggers() to access the trigger classes.
"""

# No public exports - all triggers are internal
# Access via: UtilityRegistry.get_builtin_triggers()
__all__: list[str] = []
