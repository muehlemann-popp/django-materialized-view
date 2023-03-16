# -*- coding: utf-8
"""
Settings for test.
"""
from __future__ import absolute_import, unicode_literals

import os

DEBUG = True
USE_TZ = True

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = "**************************************************"

DB_ENGINE = os.environ.get("DB_ENGINE", "postgresql")

DATABASES = {
    "default": {
        "ENGINE": f"django.db.backends.{DB_ENGINE}",
        "NAME": os.environ.get("DB_NAME", "postgres"),
        "USER": os.environ.get("DB_USER", "postgres"),
        "PASSWORD": os.environ.get("DB_PASSWORD", "postgres"),
        "HOST": os.environ.get("DB_HOST", "127.0.0.1"),
        "PORT": os.environ.get("DB_PORT", 5432),
    },
}

ROOT_URLCONF = "testproject.urls"

INSTALLED_APPS = [
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sites",
    "django_materialized_view",
]
