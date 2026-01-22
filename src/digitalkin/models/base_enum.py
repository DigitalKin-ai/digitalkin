from __future__ import annotations

from typing import Generic, TypeVar, get_args, get_origin

from typing_extensions import Self

T = TypeVar("T", bound="BaseEnum")
P = TypeVar("P")  # Type for proto enum


class BaseEnum(Generic[P]):
    """Base enumeration mixin with protobuf conversion methods."""

    @classmethod
    def _get_proto_enum(cls) -> type[P] | None:
        """Get the proto enum type from the generic parameter.

        Returns:
            The proto enum type, or None if not found.
        """
        for base in getattr(cls, "__orig_bases__", ()):
            origin = get_origin(base)
            if origin is BaseEnum or (origin is not None and issubclass(origin, BaseEnum)):
                args = get_args(base)
                if args:
                    return args[0]
        msg = "Proto enum type not found in generic parameters."
        raise AttributeError(msg)

    def to_proto(self) -> P | None:
        """Convertit en l'enum protobuf correspondant.

        Retourne :
            La valeur de l'enum protobuf, ou l'élément 0 si UNSPECIFIED, ou None si échec.
        """
        try:
            proto_enum = self.__class__._get_proto_enum()
            if proto_enum is None:
                return None
            if self.name == "UNSPECIFIED":
                # Retourne l'élément d'index 0 de l'enum proto
                return next(iter(proto_enum.__dict__.values()))
            return getattr(proto_enum, self.name)
        except (AttributeError, IndexError):
            return None

    @classmethod
    def from_proto(cls, proto_value: P) -> Self:
        """Crée une enum à partir d'une valeur d'enum protobuf.

        Args:
            proto_value: La valeur de l'enum protobuf à convertir.

        Returns:
            La valeur d'enum correspondante, ou UNSPECIFIED si conversion échoue ou si proto_value est l'élément 0.
        """
        try:
            proto_enum = cls._get_proto_enum()
            if proto_value == next(iter(proto_enum.__dict__.values())):
                return cls["UNSPECIFIED"]
            return cls[proto_enum.Name(proto_value)]
        except (KeyError, ValueError, AttributeError, IndexError):
            return cls["UNSPECIFIED"]
