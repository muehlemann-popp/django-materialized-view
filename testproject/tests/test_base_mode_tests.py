import hashlib
from collections import OrderedDict
from unittest.mock import MagicMock, call

import pytest
from django.db import InternalError
from pytest_django.asserts import assertNumQueries

from django_materialized_view.base_model import DBViewsRegistry
from django_materialized_view.models import MaterializedViewMigrations
from django_materialized_view.processor import MaterializedViewsProcessor
from testproject.tests.factories import MaterializedViewMigrationsFactory


class TestMaterializedViewsProcessor:
    def setup_method(self):
        self.view_processor = MaterializedViewsProcessor()

    def test__get_view_name__success(self):
        app_name = "test_app"
        view_name = "test_view_name"
        full_view_name = self.view_processor._MaterializedViewsProcessor__get_view_name(app_name, view_name)
        assert full_view_name == f"{app_name}_{view_name}"

    def test__separate_app_name_and_view_name__success(self):
        test_app_name = "app"
        test_view_name = "viewname"
        full_view_name = f"{test_app_name}_{test_view_name}"
        app_name, view_name = self.view_processor._MaterializedViewsProcessor__separate_app_name_and_view_name(
            full_view_name
        )
        assert (test_app_name, test_view_name) == (app_name, view_name)

    def test__get_current_view_models__success(self):
        view_models = self.view_processor._MaterializedViewsProcessor__get_current_view_models()
        assert isinstance(view_models, dict)
        assert set(DBViewsRegistry.values()) == set(view_models.values())
        assert set([tuple(i.split("_")) for i in DBViewsRegistry.keys()]) == set(view_models.keys())

    def test__is_same_views__success(self):
        result = self.view_processor._MaterializedViewsProcessor__is_same_views("test", "test")
        assert result is True

    def test__is_same_views__invalid(self, subtests):
        with subtests.test(msg="return false"):
            result = self.view_processor._MaterializedViewsProcessor__is_same_views("test", "test1")
            assert result is False
        with subtests.test(msg="raise TypeError with msg 'previous_hash must be a string'"):
            with pytest.raises(TypeError) as exc:
                self.view_processor._MaterializedViewsProcessor__is_same_views(1, "test1")
            assert exc.value.args == ("previous_hash must be a string",)
        with subtests.test(msg="raise TypeError with msg 'actual_hash must be a string'"):
            with pytest.raises(TypeError) as exc:
                self.view_processor._MaterializedViewsProcessor__is_same_views("test", 1)
            assert exc.value.args == ("actual_hash must be a string",)

    @pytest.mark.django_db
    def test__get_previous_view_definition_hash__success(self):
        test_app_name = "app"
        test_view_name = "viewname"
        MaterializedViewMigrationsFactory()
        migration_obj = MaterializedViewMigrationsFactory(app=test_app_name, view_name=test_view_name)
        result = self.view_processor._MaterializedViewsProcessor__get_previous_view_definition_hash(
            test_app_name, test_view_name
        )
        assert result == migration_obj.hash

    @pytest.mark.django_db
    def test__get_previous_view_definition_hash__return_none(self):
        test_app_name = "app"
        test_view_name = "viewname"
        MaterializedViewMigrationsFactory()
        with assertNumQueries(1):
            result = self.view_processor._MaterializedViewsProcessor__get_previous_view_definition_hash(
                test_app_name, test_view_name
            )
        assert result is None

    def test__get_hash_from_string__success(self):
        string = "test_string"
        string_hash = hashlib.md5(string.encode()).hexdigest()
        test_string = f'"{string}   "\n'
        result = self.view_processor._MaterializedViewsProcessor__get_hash_from_string(test_string)
        assert result == string_hash

    def test__get_cleaned_view_definition_value__success(self):
        string = "test_string"
        result = self.view_processor._MaterializedViewsProcessor__get_cleaned_view_definition_value(string)
        assert result == string

    def test__get_cleaned_view_definition_value__invalid(self):
        with pytest.raises(AssertionError) as exc:
            self.view_processor._MaterializedViewsProcessor__get_cleaned_view_definition_value(1)
        assert exc.value.args == ("View definition must be callable and return Tuple[str, Optional[tuple]].",)

    @pytest.mark.django_db
    def test__get_related_views__success(self):
        with assertNumQueries(1):
            result = self.view_processor._MaterializedViewsProcessor__get_related_views()
        assert set([i["materialized_view"] for i in result]) == set(DBViewsRegistry.keys())

    def test__get_prioritized_views__success(self, mocker):
        test_view_name = "test"
        mocker.patch.object(
            MaterializedViewsProcessor, "_MaterializedViewsProcessor__get_ref_views", side_effect=([test_view_name], [])
        )
        result = self.view_processor._MaterializedViewsProcessor__get_prioritized_views("test_view_name")
        assert result == {test_view_name}

    def test__prioritize_view__success(self, mocker):
        test_view_name = "test"
        test_view_name_two = "test3"
        test_related_views = ["test1", "test2"]
        test_dependencies_story = set()

        mocker.patch.object(
            MaterializedViewsProcessor,
            "_MaterializedViewsProcessor__get_ref_views",
            side_effect=([test_view_name], [test_view_name_two], [], []),
        )
        self.view_processor._MaterializedViewsProcessor__prioritize_view(
            view=test_view_name,
            related_views=test_related_views,
            dependencies_story=test_dependencies_story,
        )
        assert test_dependencies_story == {test_view_name, test_view_name_two, *test_related_views}
        assert self.view_processor._MaterializedViewsProcessor__recreation_priority == {test_view_name: 3}

    def test__get_actual_view_definition__success(self, mocker, subtests):
        test_view_definition = "test"
        with subtests.test(msg="view_definition callable"):
            test_view_mock = MagicMock()
            test_view_mock.view_definition.return_value = ("test raw query", ())
            DBViewsRegistry[test_view_definition] = test_view_mock
            get_cleaned_view_mock = mocker.patch.object(
                MaterializedViewsProcessor,
                "_MaterializedViewsProcessor__get_cleaned_view_definition_value",
                return_value=test_view_definition,
            )

            result = self.view_processor._MaterializedViewsProcessor__get_actual_view_definition("test")

            get_cleaned_view_mock.assert_called_once_with("test raw query")
            assert result == (test_view_definition, ())

    def test__get_ref_views__success(self, mocker):
        ref_view_name = "test_ref_view"
        test_mt_view_name = "test_mt_view"
        test_ref_views = [
            {
                self.view_processor.MATERIALIZED_VIEW_FIELD_NAME: test_mt_view_name,
                self.view_processor.REF_TABLE_FIELD_NAME: ref_view_name,
            },
            {
                self.view_processor.MATERIALIZED_VIEW_FIELD_NAME: "else",
                self.view_processor.REF_TABLE_FIELD_NAME: "else",
            },
        ]
        get_related_views_mock = mocker.patch.object(
            MaterializedViewsProcessor, "_MaterializedViewsProcessor__get_related_views", return_value=test_ref_views
        )

        result = self.view_processor._MaterializedViewsProcessor__get_ref_views(ref_view_name)

        get_related_views_mock.assert_called_once()
        assert result == [test_mt_view_name]

    def test__remove_view_from_deletion_list__success(self):
        test_mt_view_name = "test_mt_view"
        self.view_processor._MaterializedViewsProcessor__views_to_be_deleted.add(test_mt_view_name)

        self.view_processor._MaterializedViewsProcessor__remove_view_from_deletion_list(test_mt_view_name)

        assert self.view_processor._MaterializedViewsProcessor__views_to_be_deleted == set()

    def test__remove_view_from_recreation_list__success(self):
        test_mt_view_name = "test_mt_view"
        self.view_processor._MaterializedViewsProcessor__views_to_be_recreated.add(test_mt_view_name)

        self.view_processor._MaterializedViewsProcessor__remove_view_from_recreation_list(test_mt_view_name)

        assert self.view_processor._MaterializedViewsProcessor__views_to_be_recreated == set()

    def test__remove_view_from_creation_list__success(self):
        test_mt_view_name = "test_mt_view"
        self.view_processor._MaterializedViewsProcessor__views_to_be_created.add(test_mt_view_name)

        self.view_processor._MaterializedViewsProcessor__remove_view_from_creation_list(test_mt_view_name)

        assert self.view_processor._MaterializedViewsProcessor__views_to_be_created == set()

    def test__add_view_to_deleted_list__success(self):
        test_mt_view_name = "test_mt_view"

        self.view_processor._MaterializedViewsProcessor__add_view_to_deleted_list(test_mt_view_name)

        assert self.view_processor._MaterializedViewsProcessor__deleted_views == {test_mt_view_name}

    def test__add_view_to_recreated_list__success(self):
        test_mt_view_name = "test_mt_view"

        self.view_processor._MaterializedViewsProcessor__add_view_to_recreated_list(test_mt_view_name)

        assert self.view_processor._MaterializedViewsProcessor__recreated_views == {test_mt_view_name}

    def test__add_view_to_created_list__success(self):
        test_mt_view_name = "test_mt_view"

        self.view_processor._MaterializedViewsProcessor__add_view_to_created_list(test_mt_view_name)

        assert self.view_processor._MaterializedViewsProcessor__created_views == {test_mt_view_name}

    @pytest.mark.django_db
    def test__delete_view__success(self, subtests):
        test_mt_view_name = "test_mt_view"

        with subtests.test(msg="deletion success"):
            with assertNumQueries(1):
                result = self.view_processor._delete_view(test_mt_view_name)

            assert result is True

    def test__delete_view__raises_error(self, mocker):
        test_mt_view_name = "test_mt_view"
        test_exception_message = "test_exception"

        connection_mock = mocker.patch("django.db.connection.cursor")
        connection_mock().__enter__().execute.side_effect = InternalError(test_exception_message)
        get_prioritized_views_mock = mocker.patch.object(
            MaterializedViewsProcessor, "_MaterializedViewsProcessor__get_prioritized_views", return_value=[]
        )

        with pytest.raises(InternalError) as exc:
            self.view_processor._delete_view(test_mt_view_name)

        get_prioritized_views_mock.assert_called_once_with(test_mt_view_name)
        assert exc.value.args == (test_exception_message,)

    def test__delete_view__returns_false(self, mocker):
        test_mt_view_name = "test_mt_view"
        test_mt_view_name_two = "test_mt_view_two"

        test_exception_message = "test_exception"

        connection_mock = mocker.patch("django.db.connection.cursor")
        connection_mock().__enter__().execute.side_effect = [InternalError(test_exception_message), None]
        get_prioritized_views_mock = mocker.patch.object(
            MaterializedViewsProcessor,
            "_MaterializedViewsProcessor__get_prioritized_views",
            return_value=[test_mt_view_name_two],
        )
        self.view_processor._MaterializedViewsProcessor__views_to_be_deleted.add(test_mt_view_name_two)

        result = self.view_processor._delete_view(test_mt_view_name)

        get_prioritized_views_mock.assert_called_once_with(test_mt_view_name)
        assert result is True

    def test__delete_view__raises_error_not_allowed_to_delete(self, mocker):
        test_mt_view_name = "test_mt_view"
        test_mt_view_name_two = "test_mt_view_two"

        test_exception_message = "test_exception"

        connection_mock = mocker.patch("django.db.connection.cursor")
        connection_mock().__enter__().execute.side_effect = InternalError(test_exception_message)
        get_prioritized_views_mock = mocker.patch.object(
            MaterializedViewsProcessor,
            "_MaterializedViewsProcessor__get_prioritized_views",
            return_value=[test_mt_view_name_two],
        )

        with pytest.raises(InternalError) as exc:
            self.view_processor._delete_view(test_mt_view_name)

        get_prioritized_views_mock.assert_called_once_with(test_mt_view_name)
        assert exc.value.args == (test_exception_message,)

    def test__recreate_view__success(self, mocker):
        test_mt_view_name = "test_mt_view"

        delete_view_mock = mocker.patch.object(MaterializedViewsProcessor, "_delete_view", return_value=True)
        create_view_mock = mocker.patch.object(MaterializedViewsProcessor, "_create_view", return_value=True)
        self.view_processor._MaterializedViewsProcessor__views_to_be_recreated.add(test_mt_view_name)

        result = self.view_processor._recreate_view(test_mt_view_name, delete_cascade=False)

        delete_view_mock.assert_called_once_with(test_mt_view_name, cascade=False)
        create_view_mock.assert_called_once_with(test_mt_view_name)
        assert result is True
        assert self.view_processor._MaterializedViewsProcessor__views_to_be_recreated == set()
        assert self.view_processor._MaterializedViewsProcessor__recreated_views == {test_mt_view_name}

    def test__recreate_view__returns_false(self, mocker):
        test_mt_view_name = "test_mt_view"

        delete_view_mock = mocker.patch.object(MaterializedViewsProcessor, "_delete_view", return_value=False)
        create_view_mock = mocker.patch.object(MaterializedViewsProcessor, "_create_view", return_value=True)
        self.view_processor._MaterializedViewsProcessor__views_to_be_recreated.add(test_mt_view_name)

        result = self.view_processor._recreate_view(test_mt_view_name, delete_cascade=False)

        delete_view_mock.assert_called_once_with(test_mt_view_name, cascade=False)
        create_view_mock.assert_not_called()
        assert result is False
        assert self.view_processor._MaterializedViewsProcessor__views_to_be_recreated == {test_mt_view_name}
        assert self.view_processor._MaterializedViewsProcessor__recreated_views == set()

    @pytest.mark.django_db
    def test__create_view__success(self, mocker, subtests):
        test_app_name = "app"
        test_view_name = "viewname"
        full_view_name = f"{test_app_name}_{test_view_name}"

        view_definition = "SELECT * FROM pg_depend", ()
        get_actual_view_definition_mock = mocker.patch.object(
            MaterializedViewsProcessor,
            "_MaterializedViewsProcessor__get_actual_view_definition",
            return_value=view_definition,
        )

        with assertNumQueries(3):
            result = self.view_processor._create_view(full_view_name)

        assert result is True
        get_actual_view_definition_mock.assert_called_once_with(full_view_name)
        assert MaterializedViewMigrations.objects.filter(app=test_app_name, view_name=test_view_name).count() == 1

        with subtests.test(msg="returns False"):
            with assertNumQueries(1):
                result = self.view_processor._create_view(full_view_name)
            assert result is False
            assert self.view_processor._MaterializedViewsProcessor__views_to_be_recreated == {full_view_name}
            assert self.view_processor._MaterializedViewsProcessor__views_to_be_created == set()

    def test__get_view_list_sorted_by_dependencies__success(self, mocker):
        views = ["test_view", "test_view2"]
        depend_views = ["test_depend_view"]

        get_prioritized_views_mock = mocker.patch.object(
            MaterializedViewsProcessor,
            "_MaterializedViewsProcessor__get_prioritized_views",
            side_effect=[depend_views, [], []],
        )
        self.view_processor._MaterializedViewsProcessor__recreation_priority = {"test": 2, "test2": 1, "test3": 5}

        sorted_views = self.view_processor.get_view_list_sorted_by_dependencies(views)

        assert get_prioritized_views_mock.call_count == 3
        assert get_prioritized_views_mock.call_args_list == [
            call("test_view"),
            call("test_view2"),
            call("test_depend_view"),
        ]
        assert sorted_views == OrderedDict({"test3": 5, "test": 2, "test2": 1})

    @pytest.mark.django_db
    def test__delete_views__success(self, mocker):
        view_name = "test"
        self.view_processor._MaterializedViewsProcessor__views_to_be_deleted = {view_name}
        delete_view_mock = mocker.patch.object(
            MaterializedViewsProcessor,
            "_delete_view",
            return_value=True,
        )
        migration = MaterializedViewMigrationsFactory()
        separate_name_mock = mocker.patch.object(
            MaterializedViewsProcessor,
            "_MaterializedViewsProcessor__separate_app_name_and_view_name",
            return_value=(migration.app, migration.view_name),
        )

        with assertNumQueries(1):
            self.view_processor.delete_views()

        migration.refresh_from_db()
        assert migration.deleted is True
        assert self.view_processor._MaterializedViewsProcessor__views_to_be_deleted == set()
        assert self.view_processor._MaterializedViewsProcessor__deleted_views == {view_name}
        delete_view_mock.assert_called_once_with(view_name)
        separate_name_mock.assert_called_once_with(view_name)

    def test__recreate_views__success(self, mocker):
        sorted_views = OrderedDict({"test3": 5, "test": 2, "test2": 1})
        self.view_processor._MaterializedViewsProcessor__views_to_be_recreated = {"test"}

        get_view_list_sorted_by_dependencies_mock = mocker.patch.object(
            MaterializedViewsProcessor,
            "get_view_list_sorted_by_dependencies",
            return_value=sorted_views,
        )
        recreate_view_mock = mocker.patch.object(
            MaterializedViewsProcessor,
            "_recreate_view",
            return_value=True,
        )

        self.view_processor.recreate_views()

        get_view_list_sorted_by_dependencies_mock.assert_called_once_with({"test"})
        assert self.view_processor._MaterializedViewsProcessor__views_to_be_recreated == set(sorted_views)
        assert recreate_view_mock.call_count == 3
        assert recreate_view_mock.call_args_list == [
            call("test3", delete_cascade=True),
            call("test", delete_cascade=True),
            call("test2", delete_cascade=True),
        ]

    def test__create_views__success(self, mocker):
        view_name = "test"
        self.view_processor._MaterializedViewsProcessor__views_to_be_created = {view_name}

        create_view_mock = mocker.patch.object(
            MaterializedViewsProcessor,
            "_create_view",
            return_value=True,
        )

        self.view_processor.create_views()

        create_view_mock.assert_called_once_with(view_name)
        assert self.view_processor._MaterializedViewsProcessor__views_to_be_created == set()
        assert self.view_processor._MaterializedViewsProcessor__created_views == {view_name}

    def test__create_views__already_created(self, mocker):
        view_name = "test"
        self.view_processor._MaterializedViewsProcessor__views_to_be_created = {view_name}
        self.view_processor._MaterializedViewsProcessor__created_views = {view_name}

        create_view_mock = mocker.patch.object(
            MaterializedViewsProcessor,
            "_create_view",
            return_value=True,
        )

        self.view_processor.create_views()

        create_view_mock.assert_not_called()
        assert self.view_processor._MaterializedViewsProcessor__views_to_be_created == set()
        assert self.view_processor._MaterializedViewsProcessor__created_views == {view_name}

    def test__create_views__not_created(self, mocker):
        view_name = "test"
        self.view_processor._MaterializedViewsProcessor__views_to_be_created = {view_name}

        create_view_mock = mocker.patch.object(
            MaterializedViewsProcessor,
            "_create_view",
            side_effect=[False, True],
        )

        self.view_processor.create_views()

        assert create_view_mock.call_args_list == [call(view_name), call(view_name)]
        assert self.view_processor._MaterializedViewsProcessor__views_to_be_created == set()
        assert self.view_processor._MaterializedViewsProcessor__created_views == {view_name}

    @pytest.mark.django_db
    def test__mark_to_be_deleted_old_views__success(self):
        view_name = "test"
        self.view_processor._MaterializedViewsProcessor__views_to_be_created = {view_name}
        migration = MaterializedViewMigrationsFactory()

        self.view_processor.mark_to_be_deleted_old_views()

        assert self.view_processor._MaterializedViewsProcessor__views_to_be_deleted == {
            f"{migration.app}_{migration.view_name}"
        }
