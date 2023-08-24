import hashlib
import logging
from collections import OrderedDict, defaultdict
from typing import Dict, List, Optional, Set, Tuple

from django.apps import apps
from django.db import InternalError, ProgrammingError, connection

from django_materialized_view.base_model import DBViewsRegistry, MaterializedViewModel
from django_materialized_view.models import MaterializedViewMigrations
from django_materialized_view.sql_functions import dictfetchall

logger = logging.getLogger(__name__)


class MaterializedViewsProcessor:
    CREATE_COMMAND_TEMPLATE = "CREATE MATERIALIZED VIEW %s AS %s;"
    DELETE_VIEW_COMMAND_TEMPLATE = "DROP MATERIALIZED VIEW IF EXISTS %s %s;"
    CASCADE = "CASCADE"
    MATERIALIZED_VIEW_FIELD_NAME = "materialized_view"
    REF_TABLE_FIELD_NAME = "ref_table"

    def __init__(
        self,
    ):
        self.__views_to_be_created = set()
        self.__views_to_be_recreated = set()
        self.__views_to_be_deleted = set()
        self.__created_views = set()
        self.__recreated_views = set()
        self.__deleted_views = set()
        self.__recreation_priority = defaultdict(int)

    def process_materialized_views(self) -> None:
        self.mark_to_be_applied_new_views()
        self.mark_to_be_deleted_old_views()
        self.create_views()
        self.recreate_views()
        self.delete_views()

    def add_view_to_be_created(self, view_name: str) -> None:
        self.__views_to_be_created.add(view_name)

    def add_view_to_be_recreated(self, view_name: str) -> None:
        self.__views_to_be_recreated.add(view_name)

    def add_view_to_be_deleted(self, view_name: str) -> None:
        self.__views_to_be_deleted.add(view_name)

    def mark_to_be_applied_new_views(self) -> None:
        view_models = self.__get_current_view_models()
        for (app_label, model_name), view_model in view_models.items():
            view_name = self.__get_view_name(app_label, model_name)
            actual_view_definition, args = self.__get_actual_view_definition(view_name)
            actual_view_definition_hash = self.__get_hash_from_string(
                actual_view_definition % args if args else actual_view_definition
            )
            previous_view_definition_hash = self.__get_previous_view_definition_hash(app_label, model_name)

            if previous_view_definition_hash is None:
                self.add_view_to_be_created(view_name)
            elif not self.__is_same_views(previous_view_definition_hash, actual_view_definition_hash):
                self.add_view_to_be_recreated(view_name)

    def mark_to_be_deleted_old_views(self) -> None:
        views_migrations = MaterializedViewMigrations.objects.filter(deleted=False)
        for view_migration in views_migrations:
            view_name = self.__get_view_name(view_migration.app, view_migration.view_name)
            if view_name not in DBViewsRegistry:
                self.add_view_to_be_deleted(view_name)

    def create_views(self) -> None:
        logger.info(f"Creating views. {self.__views_to_be_created}")
        for view_name in tuple(self.__views_to_be_created):
            logger.info(f"Trying to create view. {view_name}")

            if view_name in self.__created_views:
                logger.info(f"Skip creating. View already created. {view_name}")
                self.__remove_view_from_creation_list(view_name)
                continue

            created = self._create_view(view_name)
            if not created:
                logger.info(f"Unable to create view. {view_name}")
                continue

            logger.info(f"View created. {view_name}")
            self.__add_view_to_created_list(view_name)
            self.__remove_view_from_creation_list(view_name)

        if self.__views_to_be_created:
            logger.debug(f"Views to be created still exist. Trying to create. {self.__views_to_be_created}")
            self.create_views()

    def recreate_views(self) -> None:
        sorted_views = self.get_view_list_sorted_by_dependencies(self.__views_to_be_recreated)
        self.__views_to_be_recreated = set(sorted_views)

        logger.info(f"Recreating views. {sorted_views}")
        for view in sorted_views:
            logger.info(f"Recreating view. {view}")
            recreated = self._recreate_view(view, delete_cascade=True)
            logger.info(f"Recreating view {'success' if recreated else 'failed'}. {view}")

    def delete_views(self) -> None:
        logger.info(f"Deleting old views. {self.__views_to_be_deleted}")
        for view_name in tuple(self.__views_to_be_deleted):
            deleted = self._delete_view(view_name)
            if deleted:
                app, model_view_name = self.__separate_app_name_and_view_name(view_name)
                MaterializedViewMigrations.objects.filter(app=app, view_name=model_view_name).update(deleted=True)
                logger.info(f"View {view_name} deleted")
                self.__add_view_to_deleted_list(view_name)
                self.__remove_view_from_deletion_list(view_name)

    def get_view_list_sorted_by_dependencies(self, views: Set[str]) -> OrderedDict[str, int]:
        recreation_list = list(views)
        for view in recreation_list:
            dependencies = self.__get_prioritized_views(view)
            for dependency in dependencies:
                if dependency not in recreation_list:
                    # append new dependencies and iterate over them
                    recreation_list.append(dependency)

        sorted_views = OrderedDict(sorted(self.__recreation_priority.items(), key=lambda item: item[1], reverse=True))
        return sorted_views

    def _create_view(self, view_name: str) -> bool:
        logger.debug(f"Creating view: {view_name}")

        view_definition, args = self.__get_actual_view_definition(view_name)
        with connection.cursor() as cursor:
            try:
                cursor.execute(self.CREATE_COMMAND_TEMPLATE % (view_name, view_definition), args)
            except ProgrammingError as exc:
                logger.debug(f"Unable to create view: {view_name}. Error: {exc.args}")
                if "already exists" in exc.args[0]:
                    logger.debug(f"Marking to recreate view: {view_name}. Marking to recreate view. Error: {exc.args}")
                    self.add_view_to_be_recreated(view_name)
                    self.__remove_view_from_creation_list(view_name)
                    return False
                elif "does not exist" in exc.args[0]:
                    return False
                else:
                    raise exc
        logger.debug(f"View created: {view_name} ")
        logger.debug(f"Creating migration: {view_name}")
        app, model_view_name = self.__separate_app_name_and_view_name(view_name)
        actual_view_definition_hash = self.__get_hash_from_string(view_definition % args)
        migration = MaterializedViewMigrations(app=app, view_name=model_view_name, hash=actual_view_definition_hash)
        migration.save()
        logger.debug(f"Migration created: {view_name}. migration_id={migration.pk}")
        return True

    def _recreate_view(self, view_name: str, delete_cascade: bool) -> bool:
        logger.debug(f"Recreating view. {view_name}")
        deleted = self._delete_view(view_name, cascade=delete_cascade)
        if not deleted:
            logger.debug(f"Unable to recreate view. {view_name}")
            return False
        created = self._create_view(view_name)
        if created:
            logger.debug(f"View recreated. {view_name}")
            self.__add_view_to_recreated_list(view_name)
            self.__remove_view_from_recreation_list(view_name)
        logger.debug(f"Unable to recreate view. {view_name}")
        return created

    def _delete_view(self, view_name: str, cascade: bool = False) -> bool:
        logger.debug(f"Deleting view: {view_name}")
        with connection.cursor() as cursor:
            try:
                cursor.execute(self.DELETE_VIEW_COMMAND_TEMPLATE % (view_name, self.CASCADE if cascade else ""))
            except InternalError as exc:
                logger.debug(f"Unable to delete view: {view_name}. Error: {exc.args}")
                dependencies = self.__get_prioritized_views(view_name)
                if not dependencies:
                    raise exc

                allowed_to_delete = True

                for view in dependencies:
                    if view not in self.__views_to_be_deleted:
                        allowed_to_delete = False
                        break

                if allowed_to_delete:
                    deleted = self._delete_view(view_name, cascade=True)
                    return deleted
                logger.debug(f"Unable to delete. View {view_name} has related view. dependencies: {dependencies}")
                raise exc

        logger.debug(f"{view_name} view deleted")
        return True

    def __add_view_to_created_list(self, view_name: str) -> None:
        self.__created_views.add(view_name)

    def __add_view_to_recreated_list(self, view_name: str) -> None:
        self.__recreated_views.add(view_name)

    def __add_view_to_deleted_list(self, view_name: str) -> None:
        self.__deleted_views.add(view_name)

    def __remove_view_from_creation_list(self, view_name: str) -> None:
        if view_name in self.__views_to_be_created:
            self.__views_to_be_created.remove(view_name)

    def __remove_view_from_recreation_list(self, view_name: str) -> None:
        if view_name in self.__views_to_be_recreated:
            self.__views_to_be_recreated.remove(view_name)

    def __remove_view_from_deletion_list(self, view_name: str) -> None:
        if view_name in self.__views_to_be_deleted:
            self.__views_to_be_deleted.remove(view_name)

    def __get_ref_views(self, view_name: str) -> List[str]:
        related_views = self.__get_related_views()
        return [
            view_obj[self.MATERIALIZED_VIEW_FIELD_NAME]
            for view_obj in related_views
            if view_obj[self.REF_TABLE_FIELD_NAME] == view_name
        ]

    def __get_actual_view_definition(self, view_name: str) -> str:
        view_model = DBViewsRegistry[view_name]
        if callable(view_model.view_definition):
            raw_view_definition, args = view_model.view_definition()
        else:
            raise ValueError("view_definition must be callable")
        view_definition = self.__get_cleaned_view_definition_value(raw_view_definition)
        return view_definition, args

    def __prioritize_view(self, view: str, related_views: List[str], dependencies_story: set[str]) -> None:
        if related_views:
            dependencies_story.update(related_views)
            self.__recreation_priority[view] += 1
        else:
            self.__recreation_priority[view] += 0

        for related_view in related_views:
            ref_views = self.__get_ref_views(related_view)
            self.__prioritize_view(view, ref_views, dependencies_story)

    def __get_prioritized_views(self, view_name: str) -> set[str]:
        ref_views = self.__get_ref_views(view_name)
        dependencies_story = set()  # type: ignore
        self.__prioritize_view(view_name, ref_views, dependencies_story)
        return dependencies_story

    def __get_related_views(self) -> List[Dict[str, str]]:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT DISTINCT pg_class.oid::regclass                AS {self.MATERIALIZED_VIEW_FIELD_NAME},
                                pg_depend.refobjid::regclass::varchar AS {self.REF_TABLE_FIELD_NAME} --table in relation
                FROM pg_depend -- objects that depend on a table
                         JOIN pg_rewrite -- rules depending on a table
                              ON pg_rewrite.oid = pg_depend.objid
                         JOIN pg_class -- views for the rules
                              ON pg_class.oid = pg_rewrite.ev_class
                         JOIN pg_namespace -- schema information
                              ON pg_namespace.oid = pg_class.relnamespace
                WHERE                                                   -- filter materialized views only
                  -- dependency must be a rule depending on a relation
                    pg_depend.classid = 'pg_rewrite'::regclass
                  AND pg_depend.refclassid = 'pg_class'::regclass  -- referenced objects in pg_class (tables and views)
                  AND pg_depend.deptype = 'n'                      -- normal dependency
                  -- qualify object
                  AND pg_namespace.nspname NOT IN ('pg_catalog', 'information_schema', 'gp_toolkit') -- system schemas
                  AND NOT (pg_class.oid = pg_depend.refobjid)
                """
            )
            row = dictfetchall(cursor)
        return row

    @staticmethod
    def __get_cleaned_view_definition_value(view_definition: str) -> str:
        assert isinstance(
            view_definition, str
        ), "View definition must be callable and return Tuple[str, Optional[tuple]]."
        return view_definition.strip()

    @staticmethod
    def __get_hash_from_string(string: str) -> str:
        string = string.replace('"', "").replace(" ", "").replace("\n", "").replace("'", "").lower()
        return hashlib.md5(string.encode()).hexdigest()

    @staticmethod
    def __get_previous_view_definition_hash(app_label: str, view_name: str) -> Optional[str]:
        try:
            hash_obj = MaterializedViewMigrations.objects.get(app=app_label, view_name=view_name, deleted=False)
        except MaterializedViewMigrations.DoesNotExist:
            return None
        return hash_obj.hash

    @staticmethod
    def __is_same_views(previous_hash: str, actual_hash: str) -> bool:
        if not isinstance(previous_hash, str):
            raise TypeError("previous_hash must be a string")
        if not isinstance(actual_hash, str):
            raise TypeError("actual_hash must be a string")
        return previous_hash == actual_hash

    @staticmethod
    def __get_current_view_models() -> Dict[Tuple[str, str], MaterializedViewModel]:
        view_models = {}
        for app_label, models_list in apps.all_models.items():
            for model_name, model_class in models_list.items():
                if model_class._meta.db_table in DBViewsRegistry:  # noqa
                    key = (app_label, model_name)
                    view_models[key] = model_class
        return view_models

    @staticmethod
    def __get_view_name(app_label: str, model_name: str) -> str:
        return "_".join((app_label, model_name))

    @staticmethod
    def __separate_app_name_and_view_name(full_view_name: str) -> Tuple[str, str]:
        app_name, view_name = full_view_name.rsplit("_", 1)
        return app_name, view_name
