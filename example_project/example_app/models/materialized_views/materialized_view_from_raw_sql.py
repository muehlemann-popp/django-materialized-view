from django.db import models

from django_materialized_view.base_model import MaterializedViewModel

from ..my_test_model import MyTestModel


class MaterializedViewFromRawSql(MaterializedViewModel):
    """
    In this case Materialized view processor will search SQL query for materialized view in:
    example_app/models/materialized_views/sql_files/materializedviewfromrawsql.sql
    because get_query_from_queryset method not defined
    """

    create_pkey_index = True  # if you need add unique field as a primary key and create indexes

    my_test_model = models.ForeignKey(MyTestModel, on_delete=models.DO_NOTHING, primary_key=True)
    count = models.IntegerField()
