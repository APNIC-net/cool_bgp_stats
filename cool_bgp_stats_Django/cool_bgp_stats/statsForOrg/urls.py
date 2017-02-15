from django.conf.urls import url

from . import views

urlpatterns = [
    # ex: /statsForOrg/
    url(r'^$', views.index, name='index'),
    # ex: /statsForOrg/A924128D/1983
    url(r'^(?P<opaque_id>[A-Z0-9]+)/(?P<year>(19|20)\d\d)/$', views.getJSON, name='getJSON'),
#    # ex: /polls/5/results/
#    url(r'^(?P<question_id>[0-9]+)/results/$', views.results, name='results'),
#    # ex: /polls/5/vote/
#    url(r'^(?P<question_id>[0-9]+)/vote/$', views.vote, name='vote'),
]