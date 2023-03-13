# -*- coding: utf-8
"""
Settings for test.
"""
from __future__ import unicode_literals, absolute_import
import os

DEBUG = True
USE_TZ = True

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = "**************************************************"

# Note, this package only works with PostgreSQL due to the JSONField
DB_ENGINE = os.environ.get("DB_ENGINE", "postgresql")

DATABASES = {
    "default": {
        "ENGINE": f"django.db.backends.{DB_ENGINE}",
        "NAME": os.environ.get("DB_NAME", "postgres"),
        "USER": os.environ.get("DB_USER", 'postgres'),
        "PASSWORD": os.environ.get("DB_PASSWORD", "postgres"),
        "HOST": os.environ.get("DB_HOST", "127.0.0.1"),
        "PORT": os.environ.get("DB_PORT", 5432),
        "ATOMIC_REQUESTS": True
    },
}

ROOT_URLCONF = "testproject.urls"

INSTALLED_APPS = [
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sites",
    'django_materialized_view'
]