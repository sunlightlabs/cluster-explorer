from django.conf.urls.defaults import patterns, include, url
from django.contrib.staticfiles.urls import staticfiles_urlpatterns
from explorer.views import *

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns('',
    url(r'^', include('explorer.urls')),
)

urlpatterns += staticfiles_urlpatterns()
urlpatterns += staticfiles_urlpatterns()