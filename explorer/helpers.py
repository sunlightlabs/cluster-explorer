from django.template.defaultfilters import truncatewords

def doc_to_dict(doc_set, doc_limit = 0, doc_truncate = 0):
    result = dict(count=len(doc_set), docs=doc_set)
    
    if doc_limit:
        result['docs'] = result['docs'][:int(doc_limit)]

    if doc_truncate:
        truncated_docs = list()
        for doc in result['docs']:
            trunc_doc = dict(doc)
            trunc_doc['text'] = truncatewords(doc['text'],int(doc_truncate))
            truncated_docs.append(trunc_doc)
        result['docs'] = truncated_docs
    
    return result
