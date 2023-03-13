from django.db import models
from django.db.models import Q

__all__ = [
    "MaterializedViewRefreshLog",
    "MaterializedViewMigrations",
]


class MaterializedViewRefreshLog(models.Model):
    updated_at = models.DateTimeField(auto_now_add=True, db_index=True)
    duration = models.DurationField(null=True)
    failed = models.BooleanField(default=False)
    view_name = models.CharField(max_length=255)


class MaterializedViewMigrations(models.Model):
    applied = models.DateTimeField(auto_now_add=True)
    app = models.CharField(max_length=255)
    view_name = models.CharField(max_length=255)
    hash = models.CharField(max_length=255)
    deleted = models.BooleanField(default=False)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["app", "view_name"], condition=Q(deleted=False), name="one_active_view_per_model"
            ),
        ]

    def save(self, *args, **kwargs):
        if not self.deleted:
            MaterializedViewMigrations.objects.filter(app=self.app, view_name=self.view_name, deleted=False).update(
                deleted=True
            )
        return super().save(*args, **kwargs)
