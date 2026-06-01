# migrations/0001_initial.py

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.RunSQL(
            """
            CREATE SEQUENCE IF NOT EXISTS sentinel_token_seq;
            """,
            reverse_sql="""
            DROP SEQUENCE IF EXISTS sentinel_token_seq;
            """
        ),
        
        migrations.CreateModel(
            name="SentinelLease",
            fields=[
                ("key", models.TextField(primary_key=True, serialize=False)),
                ("owner_id", models.TextField()),
                ("lease_expires_at", models.DateTimeField()),
                ("lease_updated_at", models.DateTimeField(blank=True, null=True)),
                ("hard_expires_at", models.DateTimeField(blank=True, null=True)),
                ("execution_result", models.JSONField(blank=True, null=True)),
                (
                    "status",
                    models.CharField(
                        max_length=32,
                        choices=[
                            ("claimed", "claimed"),
                            ("executing", "executing"),
                            ("completed", "completed"),
                            ("reconciling", "reconciling"),
                        ],
                        default="claimed",
                    ),
                ),
                ("fencing_token", models.BigIntegerField(default=1)),
            ],
            options={
                "db_table": "sentinel_leases",
            },
        ),
        migrations.AddIndex(
            model_name="sentinellease",
            index=models.Index(
                fields=["lease_expires_at"],
                name="idx_sentinel_expiry",
            ),
        ),

        migrations.RunSQL(
            """
            ALTER TABLE sentinel_leases
            ALTER COLUMN status
            SET DEFAULT 'claimed';
            """,
            reverse_sql="""
            ALTER TABLE sentinel_leases
            ALTER COLUMN status
            DROP DEFAULT;
            """
        ),
    ]