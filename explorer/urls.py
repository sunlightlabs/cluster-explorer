from django.conf.urls.defaults import patterns, include, url

urlpatterns = patterns('',
    url(r'j$', 'explorer.views.ajax', name='home'),
    #api
    url(r'api/(?P<step>\d+)/(?P<cluster>\d+)/(?P<doc>\d+)/?$', 'explorer.views.api', name='home'),
    url(r'api/(?P<step>\d+)/(?P<cluster>\d+)/?$', 'explorer.views.api', name='home'),
    url(r'api/(?P<step>\d+)/?$', 'explorer.views.api', name='home'),
    #index
    url(r'(?P<step>\d+)/(?P<cluster>\d+)/(?P<doc>\d+)$', 'explorer.views.index', name='home'),
    url(r'(?P<step>\d+)/(?P<cluster>\d+)$', 'explorer.views.index', name='home'),
    url(r'(?P<step>\d+)$', 'explorer.views.index', name='home'),
    
    url(r'$', 'explorer.views.index', name='home'),
)
