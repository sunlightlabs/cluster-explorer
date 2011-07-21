from django.conf.urls.defaults import patterns, include, url

urlpatterns = patterns('',
    url(r'j$', 'explorer.views.ajax', name='home'),
    url(r'(?P<step>\d+)/(?P<cluster>\d+)$', 'explorer.views.cluster', name='home'),
    url(r'(?P<step>\d+)$', 'explorer.views.index', name='home'),
    #url(r'(?P<step>\d+)/(?P<cluster>\d+)/(?P<doc>\d+)$', 'explorer.views.index', name='home'),
    url(r'$', 'explorer.views.index', name='home'),
)
