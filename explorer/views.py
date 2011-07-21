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

def _process_request(): #not implemented yet, here to avoid repetition
    """checks what/where the request is coming from to determine what to do"""
    pass

def _get_step(step = 0, cluster = 0):
    count = { # You can probably do this in the template
        "steps"     : len(all_docs),
        "clusters"  : len(all_docs[int(step)]),
    }
    params = { #this is available with a RequestContext, get rid of it
        "step"      : int(step)+1,
        "cluster"   : cluster,
    }
    doc_preview = [doc_to_dict(i,1,20) for i in all_docs[int(step)]]
    return { "step" : doc_preview, "count" : count, "params" : params }

def _get_cluster(step = 0, cluster = 0):
    return_dict = doc_to_dict(all_docs[int(step)][int(cluster)],10,20)
    return_dict['id'] = cluster
    return { "cluster" : return_dict }

def _get_doc():
    pass

def index(request, step = 0, cluster = 0, doc = 0):
    response_dict = _get_step(step,cluster)
    if request.is_ajax():
        response_dict = json.dumps(response_dict)
        return HttpResponse(response_dict, mimetype="application/json")
    else:
        return render_to_response(
            "index.html",
            response_dict
        )

def cluster(request, step = 0, cluster = 0):
    response_dict = _get_cluster(step,cluster)
    if request.is_ajax():
        response_dict = json.dumps(response_dict)
        return HttpResponse(response_dict, mimetype="application/json")
    else:
        return render_to_response(
            "index.html",
            response_dict
        )

def doc():
    pass

#build only the request context you need
def out(request, step, cluster, doc):
    """
    if request.is_ajax()
    
    if doc:
        response_dict['doc']        = _get_doc(doc)
    if cluster:
        response_dict['cluster']    = _get_cluster(cluster)
    if step:
        response_dict['step']       = _get_step(step)
    else:
        response_dict['step']       = _get_step(0)
    """
    pass