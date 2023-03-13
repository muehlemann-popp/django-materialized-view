import re

from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.db import NotSupportedError

from django_materialized_view.processor import MaterializedViewsProcessor


def extract_mv_name(line):
    """
    >>> extract_mv_name('rule _RETURN on materialized view some_materialized_name depends on column')
    'some_materialized_name'
    """
    pattern = re.compile(r"rule _RETURN on materialized view (\w+) depends on column")
    match = pattern.search(line)
    if match:
        return match.group(1)
    else:
        return None


class Command(BaseCommand):
    help = "Applies migrations if need removes materialized views and then recreates them"
    views_to_be_recreated = set()  # type: ignore
    view_processor = MaterializedViewsProcessor()

    def add_arguments(self, parser):
        parser.add_argument("args", nargs="*")

    def handle(self, *args, **kwargs):
        try:
            call_command("migrate", args)
        except NotSupportedError as exc:
            print()  # need for new line
            mv_name = extract_mv_name(exc.args[0])
            if not mv_name:
                raise exc
            self.views_to_be_recreated.add(mv_name)
            sorted_views = self.view_processor.get_view_list_sorted_by_dependencies(self.views_to_be_recreated)
            self.views_to_be_recreated.update(sorted_views)
            for view in self.views_to_be_recreated:
                self.view_processor.add_view_to_be_deleted(view)
            self.view_processor.delete_views()
            self.handle(*args, **kwargs)
        self.view_processor.process_materialized_views()
