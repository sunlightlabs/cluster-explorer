from django.template.defaultfilters import truncatewords

def doc_to_dict(doc_set, doc_limit = 0, doc_truncate = 0):
    return_dict = {
        "count": len(doc_set),
        "docs" : []
    }
    if doc_limit > 0:
        doc_set = doc_set[:int(doc_limit)]

    for index, doc in enumerate(doc_set):
        if doc_truncate > 0:
            text = truncatewords(doc.text,int(doc_truncate))
        else:
            text = doc.text
        to_dict = {
            "date"    : doc.date,
            "name"    : doc.name,
            "org"     : doc.org,
            "text"    : text
        }
        return_dict['docs'].append(to_dict)
    return return_dict