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

    class Meta:
        db_table = "sentinel_leases"
        indexes = [
            models.Index(
                fields=["lease_expires_at"],
                name="idx_sentinel_expiry"
            )
        ]