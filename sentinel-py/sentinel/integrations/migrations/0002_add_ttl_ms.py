from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("integrations", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="sentinellease",
            name="ttl_ms",
            field=models.IntegerField(default=3000),
        ),
    ]