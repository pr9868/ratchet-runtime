"""Public Ratchet error hierarchy."""


class RatchetError(RuntimeError):
    """Base class for Ratchet contract violations and storage failures."""


class ReadOnlyRatchetError(RatchetError):
    """A mutating operation was attempted through a read-only Ratchet view."""


class InstanceNotFound(RatchetError):
    """The requested instance has not been initialized."""


class LeaseBusy(RatchetError):
    """A live tenure already owns the instance."""


class FenceRejected(RatchetError):
    """A stale, expired, or otherwise invalid fencing token was presented."""


class AttemptConflict(RatchetError):
    """An attempt identity or immutable binding conflicts with durable state."""


class PositionRegression(RatchetError):
    """A proposed source position is older than its committed position."""


class UnknownComparator(RatchetError):
    """No monotonic comparator is registered for a position type."""


class CheckpointConflict(RatchetError):
    """An immutable checkpoint was replaced or is invalid for the tier."""


class EffectConflict(RatchetError):
    """An effect lifecycle transition or idempotency binding is invalid."""


class ReadbackRequired(RatchetError):
    """An ambiguous effect needs verified read-back before it may proceed."""


class BundleRejected(RatchetError):
    """A run commit bundle is incomplete, inconsistent, or not verified."""


class CohortIncomplete(BundleRejected):
    """Only part of a declared commit cohort is present."""


class AlreadyCommitted(RatchetError):
    """The attempt has already committed a different immutable bundle."""
