[![GitHub Actions](https://github.com/muehlemann-popp/django-materialized-view/workflows/Test/badge.svg)](https://github.com/muehlemann-popp/django-materialized-view/actions)
[![codecov](https://codecov.io/gh/muehlemann-popp/django-materialized-view/branch/main/graph/badge.svg?token=02FP3IS41T)](https://codecov.io/gh/muehlemann-popp/django-materialized-view)
[![GitHub Actions](https://github.com/muehlemann-popp/django-materialized-view/workflows/Release/badge.svg)](https://github.com/muehlemann-popp/django-materialized-view/actions)
![GitHub](https://img.shields.io/github/license/muehlemann-popp/django-materialized-view)
![GitHub last commit](https://img.shields.io/github/last-commit/muehlemann-popp/django-materialized-view)

[![Supported Django versions](https://img.shields.io/pypi/djversions/django-materialized-view.svg)](https://pypi.python.org/pypi/django-materialized-view)
![PyPI](https://img.shields.io/pypi/v/django-materialized-view)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/django-materialized-view)
![PyPI - Downloads](https://img.shields.io/pypi/dm/django-materialized-view)


Materialized View support for the Django framework. Django (in general) does not support materialized views by the default
therefor migrations are not created automatically with `./manage.py makemigrations`.
This library provides new `manage.py` command: `./manage.py migrate_with_views`.
This command is to be used instead of the default one `migrate`.

This command automatically finds your materialized view models and keeps them up to date.
In case when materialized view is a parent view for another materialized view, use `migrate_with_views` command
in order to change query of parent materialized view.
`migrate_with_views` command finds all related materialized views and recreates them sequentially.

## Contents

* [Requirements](#requirements)
* [Installation](#installation)
* [Usage](#Usage)
  * [Create class and inherit from MaterializedViewModel](#create-class-and-inherit-from-materializedviewmodel)
  * [Add materialized view query (You can create a materialized view either from Raw SQL or from a queryset)](#add-materialized-view-query-you-can-create-a-materialized-view-either-from-raw-sql-or-from-a-queryset)
    * [Create materialized view from Raw SQL](#create-materialized-view-from-raw-sql)
    * [Create materialized view query from Queryset](#create-materialized-view-query-from-queryset)
  * [Use refresh method to update materialized view data](#use-refresh-method-to-update-materialized-view-data)


## Requirements

django-materialized-view has been tested with:

* Django: 4.0, 4.1
* Python: 3.9, 3.10, 3.11
* Postgres >= 13

## Installation

Via pip into a `virtualenv`:

```bash
pip install django-materialized-view
```

In `settings.py` add the following:

```python
INSTALLED_APPS = (
    ...
    'django_materialized_view'
)
```
Before running migrate:

```bash
python manage.py migrate
```

Then you can use new migrate command instead of the default one:
```bash
python manage.py migrate_with_views
```

This command will automatically begin interception of materialized view models,
and proceed to create/delete/update your view on your DB if required.

## Usage

1. ### Create class and inherit from `MaterializedViewModel`

    EXAMPLE:
    ```python
    from django.db import models
    from django_materialized_view.base_model import MaterializedViewModel

    class MyViewModel(MaterializedViewModel):
        create_pkey_index = True  # if you need add unique field as a primary key and create indexes

        class Meta:
            managed = False

        # if create_pkey_index=True you must add argument primary_key=True
        item = models.OneToOneField("app.ItemModel", on_delete=models.DO_NOTHING, primary_key=True, db_column="id")
        from_seconds = models.IntegerField()
        to_seconds = models.IntegerField()
        type = models.CharField(max_length=255)

        # ATTENTION: this method must be a staticmethod or classmethod
        @staticmethod
        def get_query_from_queryset():
            # define this method only in case use queryset as a query for materialized view.
            # Method must return Queryset
            pass
    ```
2. ### Add materialized view query (You can create a materialized view either from Raw SQL or from a queryset)
   - #### Create materialized view from Raw SQL
      1. run django default `makemigrations` command for creating model migrations if necessary:
         ```
         ./manage.py makemigrations
         ```
      2. run `migrate_with_views` command for getting your new sql file name and path:
          ```
          ./manage.py migrate_with_views
          ```
      3. you will get file path in your console
         ```
         [Errno 2] No such file or directory: '.../app/models/materialized_views/sql_files/myviewmodel.sql' - please create SQL file and put it to this directory
         ```
      4. create file on suggested path with suggested name
      5. run again django command `migrate_with_views`:
         ```
         ./manage.py migrate_with_views
         ```
         this command will run the default `migrate` command and apply materialized views

   - #### Create materialized view query from Queryset
      1. run django default `makemigrations` command for creating model migrations if necessary:
         ```
         ./manage.py makemigrations
         ```
      2. add to your materialized view model the method `get_query_from_queryset`:
          ```python
         # ATTENTION: this method must be a staticmethod or classmethod
         @staticmethod
           def get_query_from_queryset():
               return SomeModel.objects.all()
          ```
      3. run django command `migrate_with_views`:
         ```
         ./manage.py migrate_with_views
         ```
         This command will run default `migrate` command and apply materialized views
3. ### Use `refresh` method to update materialized view data.
    1. For updating concurrently:
       ```
       MyViewModel.refresh()
       ```
    2. For updating non-concurrently:
       ```
       MyViewModel.refresh(concurrently=Fasle)
       ```
    Note: All refreshes will be logged in to the model MaterializedViewRefreshLog:
    ```python
    class MaterializedViewRefreshLog(models.Model):
        updated_at = models.DateTimeField(auto_now_add=True, db_index=True)
        duration = models.DurationField(null=True)
        failed = models.BooleanField(default=False)
        view_name = models.CharField(max_length=255)
    ```

## Development
- #### Release CI triggered on tags. To release new version, create the release with new tag on GitHub

- #### For integration with pytest add following fixture:

    ```python
    @pytest.fixture(scope="session")
    def django_db_setup(django_db_setup, django_db_blocker):
        with django_db_blocker.unblock():
            view_processor = MaterializedViewsProcessor()
            view_processor.process_materialized_views()
    ```
