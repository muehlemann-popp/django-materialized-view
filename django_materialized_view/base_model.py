import datetime
import logging
import time
from typing import Callable, Dict, Optional, Union, Tuple

from django.conf import settings
from django.db import DEFAULT_DB_ALIAS, connections, models
from django.db.models import QuerySet
from django.db.models.base import ModelBase

__all__ = [
    "MaterializedViewModel",
    "DBViewsRegistry",
]

from django_materialized_view.models import MaterializedViewRefreshLog

logger = logging.getLogger(__name__)

DBViewsRegistry: Dict[str, "MaterializedViewModel"] = {}


class DBViewModelBase(ModelBase):
    def __new__(mcs, *args, **kwargs):
        new_class = super().__new__(mcs, *args, **kwargs)
        assert new_class._meta.managed is False, "For DB View managed must be se to false"  # noqa
        if new_class._meta.abstract is False:  # noqa
            DBViewsRegistry[new_class._meta.db_table] = new_class  # noqa
        return new_class


class DBMaterializedView(models.Model, metaclass=DBViewModelBase):
    """
    Children should define:
        view_definition - define the view, can be callable or attribute (string)
    """

    view_definition: Union[Callable, str, dict]

    class Meta:
        managed = False
        abstract = True

    @classmethod
    def refresh(cls, using: Optional[str] = None, concurrently: bool = False):
        """
        concurrently option requires an index and postgres db
        """
        using = using or DEFAULT_DB_ALIAS
        with connections[using].cursor() as cursor:
            if concurrently:
                cursor.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY %s;" % cls._meta.db_table)
            else:
                cursor.execute("REFRESH MATERIALIZED VIEW %s;" % cls._meta.db_table)


class MaterializedViewModel(DBMaterializedView):
    """
    1. create class and inherit from `MaterializedViewModel`
    EXAMPLE:
    ----------------------------------------------------------------
    from django.db import models
    from core.models.materialized_view import MaterializedViewModel

    class JiraTimeestimateChangelog(MaterializedViewModel):
        create_pkey_index = True  # if you need add unique field as a primary key and create indexes

        class Meta:
            managed = False

        # if create_pkey_index = True you must identify primary_key=True
        item = models.OneToOneField("jira.Item", on_delete=models.DO_NOTHING, primary_key=True, db_column="id")
        from_seconds = models.IntegerField()
        to_seconds = models.IntegerField()
        type = models.CharField(max_length=255)
    ----------------------------------------------------------------

    2. create materialized view query .sql file
        1. run migrate_with_views command for getting your new sql file name and path
           ./manage.py migrate_with_views
        2. you will get file path in your console
           [Errno 2] No such file or directory:
           '..../models/materialized_view/sql_files/jiradetailedstatusitem.sql'
           - please create SQL file and put it to this directory
        3. create file on suggested path with suggested name
        4. run again `./manage.py migrate_with_views` -
            this command will run default migrate command and apply materialized views

    3. add your materialized view to update cron job
       - `JiraTimeestimateChangelog.refresh()`
    """

    create_pkey_index = False

    class Meta:
        managed = False
        abstract = True

    @classmethod
    def refresh(cls, using: Optional[str] = None, concurrently: Optional[bool] = None) -> None:
        log = MaterializedViewRefreshLog(
            view_name=cls.get_tablename(),
        )
        try:
            start_time = time.monotonic()
            if concurrently is None:
                concurrently = cls.create_pkey_index is True
            super().refresh(using=using, concurrently=concurrently)
            end_time = time.monotonic()
            log.duration = datetime.timedelta(seconds=end_time - start_time)
        except Exception:  # noqa
            log.failed = True
            logging.exception(f"failed to refresh materialized view {cls.get_tablename()}")
        log.save()

    @classmethod
    def view_definition(cls) -> Tuple[str, tuple]:
        return cls.__get_query()

    @classmethod
    def __create_index_for_primary_key(cls) -> str:
        try:
            if not cls.create_pkey_index:
                return ""
            if cls._meta.pk.db_column:
                primary_key_field = cls._meta.pk.db_column
            else:
                primary_key_field = cls._meta.pk.attname
            return f"CREATE UNIQUE INDEX {cls.get_tablename()}_pkey ON {cls.get_tablename()} ({primary_key_field})"
        except Exception as exc:
            print(exc)
            exit(-1)

    @classmethod
    def __get_sql_file_path(cls) -> str:
        return (
            f"{settings.BASE_DIR}/{cls.__get_app_label()}"
            f"/models/materialized_views/sql_files/{cls.__get_class_name()}.sql"
        )

    @staticmethod
    def get_query_from_queryset() -> QuerySet:
        """
        redefine this method if you need create materialized view based on queryset instead sql file
        example:
        @staticmethod
        def get_query_from_queryset() -> QuerySet:
            return User.objects.all()
        """
        pass

    @classmethod
    def __get_query(cls, *args) -> Tuple[str, tuple]:
        queryset = cls.get_query_from_queryset()
        if isinstance(queryset, QuerySet):
            query, args = queryset.query.sql_with_params()
            sql_query = f"{query}; {cls.__create_index_for_primary_key()}"
            return sql_query, args
        try:
            with open(cls.__get_sql_file_path(), "r") as sql_file:
                sql_query = f"{sql_file.read()}; {cls.__create_index_for_primary_key()}"
        except FileNotFoundError as exc:
            raise FileNotFoundError(f"{exc}, - please create SQL file and put it to this directory")
        return sql_query, args

    @classmethod
    def __get_class_name(cls) -> str:
        return cls.__name__.casefold()

    @classmethod
    def __get_app_label(cls) -> str:
        return cls._meta.app_label

    @classmethod
    def get_tablename(cls) -> str:
        return f"{cls.__get_app_label()}_{cls.__get_class_name()}"
