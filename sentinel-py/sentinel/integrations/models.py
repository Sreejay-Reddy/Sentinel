from django.db import models

class SentinelLease(models.Model):

    class Status(models.TextChoices):
        CLAIMED = "claimed"
        EXECUTING = "executing"
        COMPLETED = "completed"
        RECONCILING = "reconciling"

    key = models.TextField(
        primary_key=True
    )

    owner_id = models.TextField()

    lease_expires_at = models.DateTimeField()

    lease_updated_at = models.DateTimeField(
        null=True,
        blank=True,
    )

    hard_expires_at = models.DateTimeField(
        null=True,
        blank=True,
    )

    execution_result = models.JSONField(
        null=True,
        blank=True,
    )

    status = models.CharField(
        max_length=32,
        choices=Status.choices,
        default=Status.CLAIMED,
    )

    fencing_token = models.BigIntegerField(
        default=1
    )

    ttl_ms = models.IntegerField(
        default=3000
    )

    class Meta:
        db_table = "sentinel_leases"
        indexes = [
            models.Index(
                fields=["lease_expires_at"],
                name="idx_sentinel_expiry"
            )
        ]

class SentinelEvent(models.Model):
    class Event(models.TextChoices):
        ACQUIRED = "acquired", "Acquired"
        REJECTED = "rejected", "Rejected"
        EXECUTING = "executing", "Executing"
        COMPLETED = "completed", "Completed"
        EXPIRED = "expired", "Expired"
        RECONCILING = "reconciling", "Reconciling"

    id = models.BigAutoField(primary_key=True)

    key = models.TextField()

    event = models.CharField(
        max_length=32,
        choices=Event.choices,
    )

    occurred_at = models.DateTimeField()

    fencing_token = models.BigIntegerField(
        null=True,
        blank=True,
    )

    owner_id = models.TextField(
        null=True,
        blank=True,
    )

    class Meta:
        db_table = "sentinel_events"

        indexes = [
            models.Index(
                fields=["key", "occurred_at", "fencing_token"],
                name="idx_sentinel_events",
            )
        ]

        ordering = ["occurred_at", "id"]

    def __str__(self):
        return f"{self.key} [{self.event}]"