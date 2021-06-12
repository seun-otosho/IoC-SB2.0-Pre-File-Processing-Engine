def worksheet_datatype(name, fn):
    # for 2 in 1 files
    if 'allndvdl' in fn and 'subj' in name.lower():
        return 'ndvdl', name

    elif 'allndvdl' in fn and ('credit' in name.lower() or 'facility' in name.lower()):
        return 'ndvdlfac', name

    elif 'allcorp' in fn and 'subj' in name.lower():
        return 'corp', name

    elif 'allcorp' in fn and ('credit' in name.lower() or 'facility' in name.lower()):
        return 'corpfac', name

    elif 'cons' in name.lower() and 'subj' in name.lower():
        return 'ndvdl', name

    # 4 sheets in a file
    elif ('corp' in name.lower() and 'faci' in name.lower()) or ('comm' in name.lower() and 'faci' in name.lower()):
        return ('corpfac', name) if fn and '_all' in fn else ('fac', name)

    elif ('cons' in name.lower() or 'individual' in name.lower()) and 'faci' in name.lower():
        return 'ndvdlfac', name

    elif ('corp' in name.lower() and 'subj' in name.lower()) or ('comm' in name.lower() and 'subj' in name.lower()):
        return 'corp', name

    # facility data
    elif 'corp' in name.lower() and 'credit' in name.lower():
        return ('corpfac', name) if fn and '_all' in fn else ('fac', name)

    elif 'individual' in name.lower() and ('credit' in name.lower() or 'facility' in name.lower()):
        return ('ndvdlfac', name) if fn and '_all' in fn else ('fac', name)

    elif 'credit' in name.lower() and ('info' in name.lower() or 'fac' in name.lower()):
        return 'fac', name

    elif 'credit' in name.lower() and 'info' in name.lower():
        return 'fac', name

    # subject data
    elif 'cons' in name.lower() and 'borrow' in name.lower():
        return 'ndvdl', name
    elif 'individual' in name.lower() and 'borrow' in name.lower():
        return 'ndvdl', name

    elif 'individual' in name.lower() and 'subject' in name.lower():
        return 'ndvdl', name

    elif 'corporate' in name.lower() and 'borrow' in name.lower():
        return 'corp', name

    elif 'corporate' in name.lower() and 'subject' in name.lower():
        return 'corp', name
    
    # un updated data files
    elif 'corp' in name.lower() and 'update' in fn.lower():
        return 'corpfac', name
    elif 'cons' in name.lower() and 'update' in fn.lower():
        return 'ndvdlfac', name

    #
    elif 'principal' in name.lower() and 'officers' in name.lower():
        """ or ('principal' in name.lower() and 'subj' in name.lower())"""
        return 'prnc', name

    #
    elif 'guarantor' in name.lower() and 'information' in name.lower():
        """ or ('principal' in name.lower() and 'subj' in name.lower())"""
        return 'grntr', name
