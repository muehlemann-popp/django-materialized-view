import factory
from factory.django import DjangoModelFactory

from django_materialized_view.models import MaterializedViewMigrations


class MaterializedViewMigrationsFactory(DjangoModelFactory):
    class Meta:
        model = MaterializedViewMigrations

    app = factory.Faker("first_name")
    view_name = factory.Faker("first_name")
    hash = factory.Faker("uuid4")
