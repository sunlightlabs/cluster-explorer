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
from django.http import HttpResponse
import settings
from cluster.hierarchy import *
import json
from helpers import doc_to_dict

all_docs = load_hierarchy(settings.PROJECT_ROOT+"/cftc.1k.complete") #Hierarchy should probably eventually be a model

def get_clusters(step = "0", cluster = "0", doc = "0"):
    count = {
        "steps"     : len(all_docs),
        "clusters"  : len(all_docs[int(step)]),
        "docs"      : len(all_docs[int(step)][int(cluster)]),
    }
    params = {
        "step"      : step,
        "cluster"   : cluster,
        "doc"       : doc,
    }
    doc_preview = [doc_to_dict(i,1,140) for i in all_docs[int(step)]]
    return { "all_docs" : doc_preview, "count" : count, "params" : params }

def index(request, step = 0, cluster = 0, doc = 0):
    response_dict = get_clusters(step,cluster,doc)
    return render_to_response(
        "index.html",
        response_dict
    )

def ajax(request): #ditch this for checking headers in the method above (HATEOAS!)
    response_dict = get_clusters(int(request.GET['step']),0,0) #don't hardcode
    response_dict = json.dumps(response_dict)
    return HttpResponse(response_dict, mimetype="application/json")