from unittest.mock import call

from django.db import NotSupportedError

from django_materialized_view.management.commands.migrate_with_views import Command, extract_mv_name
from django_materialized_view.processor import MaterializedViewsProcessor


class TestExtractMvName:
    def test__extract_mv_name__success(self):
        expected_string = "some_materialized_name"
        test_string = f"rule _RETURN on materialized view {expected_string} depends on column"
        result = extract_mv_name(test_string)
        assert result == expected_string


class TestCommand:
    def setup_method(self):
        self.command = Command()

    def test__handle__handle_error(self, mocker, db):
        view_name = "test_view_name"
        view_name_two = "test_view_name_two"
        error_message = f"rule _RETURN on materialized view {view_name} depends on column some column"

        call_command_mock = mocker.patch("django_materialized_view.management.commands.migrate_with_views.call_command")
        call_command_mock.side_effect = [NotSupportedError(error_message), None]

        extract_mv_name_mock = mocker.patch(
            "django_materialized_view.management.commands.migrate_with_views.extract_mv_name"
        )
        extract_mv_name_mock.return_value = view_name

        get_sorted_list_mock = mocker.patch.object(MaterializedViewsProcessor, "get_view_list_sorted_by_dependencies")
        get_sorted_list_mock.return_value = [view_name_two]

        add_view_to_be_deleted_mock = mocker.patch.object(MaterializedViewsProcessor, "add_view_to_be_deleted")
        delete_views_mock = mocker.patch.object(MaterializedViewsProcessor, "delete_views")

        view_processor_mock = mocker.patch.object(MaterializedViewsProcessor, "process_materialized_views")

        self.command.handle()

        assert call_command_mock.call_args_list == [call("migrate", ()), call("migrate", ())]
        assert extract_mv_name_mock.call_args_list == [call(error_message)]
        assert call(view_name) in add_view_to_be_deleted_mock.call_args_list
        assert call(view_name_two) in add_view_to_be_deleted_mock.call_args_list
        assert delete_views_mock.call_args_list == [call()]
        assert view_processor_mock.call_args_list == [call(), call()]
        assert self.command.views_to_be_recreated == {view_name, view_name_two}

    def test__handle__handle_without_error(self, mocker, db):
        call_command_mock = mocker.patch("django_materialized_view.management.commands.migrate_with_views.call_command")
        view_processor_mock = mocker.patch.object(MaterializedViewsProcessor, "process_materialized_views")

        self.command.handle()
        call_command_mock.assert_called_once_with("migrate", ())
        view_processor_mock.assert_called_once_with()
