from django.db import models

from django_materialized_view.base_model import MaterializedViewModel

from ..my_test_model import MyTestModel


class MaterializedViewFromQueryset(MaterializedViewModel):
    create_pkey_index = True  # if you need add unique field as a primary key and create indexes

    my_test_model = models.ForeignKey(MyTestModel, on_delete=models.DO_NOTHING, primary_key=True)
    count = models.IntegerField()

    @staticmethod
    def get_query_from_queryset():
        # define this method only in case use queryset as a query for materialized view.
        # Method must return Queryset
        return MyTestModel.objects.values(my_test_model_id=models.F("id")).annotate(count=models.Count("test_field"))
