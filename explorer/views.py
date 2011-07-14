#let's see which approach produces less code
"""
from django.views.generic import TemplateView
class Index(TemplateView):
    template_name = "explorer/index.html"
    
    def get_context_data(self, **kwargs):
        context = super(Index, self).get_context_data(**kwargs)
        context['cluster']
"""
from django.shortcuts import render_to_response
import settings
from cluster.hierarchy import *

def get_clusters(step = 0, cluster = 0, doc = 0):
    all_docs = load_hierarchy(settings.PROJECT_ROOT+"/cftc.1k.complete") #Hierarchy should probably eventually be a model
    count = {
        "steps"     : len(all_docs),
        "clusters"  : len(all_docs[step]),
        "docs"      : len(all_docs[step][cluster]),
    }
    params = {
        "step"      : step,
        "cluster"   : cluster,
        "doc"       : doc,
    }
    return { "all_docs" : all_docs[step], "count" : count, "params" : params }

def index(request, step = 0, cluster = 0, doc = 0):
    response_dict = get_clusters(step,cluster,doc)
    return render_to_response(
        "index.html",
        response_dict
    )

def ajax(request): #ditch this for checking headers in the method above (HATEOAS!)
    response_dict = get_clusters(int(request.GET['step']),0,0) #don't hardcode
    return render_to_response(
        "cluster-info.html", #DRY this up at some point, do client-side templating, or something else
        response_dict
    )