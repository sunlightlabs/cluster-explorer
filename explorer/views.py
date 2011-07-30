from django.shortcuts import render_to_response
from django.http import HttpResponse
import settings
from cluster.hierarchy import *
import json
from helpers import doc_to_dict

all_docs = load_hierarchy(settings.PROJECT_ROOT+"/cftc.1k.complete") #Hierarchy should probably eventually be a model

def _get_params(step = None, cluster=None, doc=None):
    if step:
        step = int(step)
    if cluster:
        cluster = int(cluster)
    if doc:
        doc = int(doc)
    return {'step':step,'cluster':cluster,'doc':doc}

def _get_step(step = 0, cluster = 0):
    count = { # You can probably do this in the template
        "steps"     : len(all_docs),
        "clusters"  : len(all_docs[int(step)]),
    }
    params = { #this is available with a RequestContext, get rid of it probably
        "step"      : int(step)+1,
        "cluster"   : cluster,
    }
    clusters = [doc_to_dict(i,1,20) for i in all_docs[int(step)]]
    return {"clusters":clusters, "params":params, "count":count} 

def _get_cluster(step = 0, cluster = 0, limit = 0):
    
    return_dict = doc_to_dict(all_docs[int(step)][int(cluster)],limit,20)
    return_dict['id'] = cluster
    return return_dict

def _get_doc(step = 0, cluster = 0, doc = 0):
    return_dict = doc_to_dict( [all_docs[int(step)][int(cluster)][int(doc)]] )
    return_dict['id'] = doc
    return return_dict

def index(request, step = None, cluster = None, doc = None):
    response_dict = {}
    response_dict['params']         = _get_params(step,cluster,doc)
    if doc:
        response_dict['doc']        = _get_doc(int(step),int(cluster),int(doc))        
    if cluster:
        try:
            limit = int(request.GET['limit'])
        except:
            limit = 10
        response_dict['cluster']    = _get_cluster(int(step),int(cluster))
    if step:
        response_dict['step']       = _get_step(int(step)-1)
    else:
        response_dict['step']       = _get_step()

    return render_to_response("index.html",response_dict)

def api(request, step = None, cluster = None, doc = None):
    response_dict = {}
    response_dict['params']         = _get_params(step,cluster,doc)
    if doc:
        response_dict['doc']        = _get_doc(int(step),int(cluster),int(doc))
    elif cluster:
        try:
            limit = int(request.GET['limit'])
        except:
            limit = 0
        response_dict['cluster']    = _get_cluster(int(step),int(cluster), limit)
    elif step:
        response_dict['step']       = _get_step(int(step)-1)
    else:
        response_dict['step']       = _get_step()

    response_dict = json.dumps(response_dict)
    return HttpResponse(response_dict, mimetype="application/json")