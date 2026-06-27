from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("integrations", "0002_add_ttl_ms"),
    ]

    operations = [
        migrations.CreateModel(
            name="SentinelEvent",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        primary_key=True,
                        serialize=False,
                    ),
                ),
                ("key", models.TextField()),
                (
                    "event",
                    models.CharField(
                        max_length=32,
                        choices=[
                            ("acquired", "Acquired"),
                            ("rejected", "Rejected"),
                            ("executing", "Executing"),
                            ("completed", "Completed"),
                            ("expired", "Expired"),
                            ("reconciling", "Reconciling"),
                        ],
                    ),
                ),
                ("occurred_at", models.DateTimeField()),
                (
                    "fencing_token",
                    models.BigIntegerField(
                        blank=True,
                        null=True,
                    ),
                ),
                (
                    "owner_id",
                    models.TextField(
                        blank=True,
                        null=True,
                    ),
                ),
            ],
            options={
                "db_table": "sentinel_events",
                "ordering": ["occurred_at", "id"],
            },
        ),
        migrations.AddIndex(
            model_name="sentinelevent",
            index=models.Index(
                fields=["key", "occurred_at", "fencing_token"],
                name="idx_sentinel_events",
            ),
        ),
        migrations.RunSQL(
            """
            ALTER TABLE sentinel_events
            ALTER COLUMN occurred_at
            SET DEFAULT now();
            """,
            reverse_sql="""
            ALTER TABLE sentinel_events
            ALTER COLUMN occurred_at
            DROP DEFAULT;
            """,
        ),
    ]