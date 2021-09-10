def worksheet_datatype(name, fn):
    # for 2 in 1 files
    name_lower = name.lower()
    if 'allndvdl' in fn and 'subj' in name_lower:
        return 'ndvdl', name

    elif 'allndvdl' in fn and ('credit' in name_lower or 'facility' in name_lower):
        return 'ndvdlfac', name

    elif 'allcorp' in fn and 'subj' in name_lower:
        return 'corp', name

    elif 'allcorp' in fn and ('credit' in name_lower or 'facility' in name_lower):
        return 'corpfac', name

    elif 'cons' in name_lower and 'subj' in name_lower:
        return 'ndvdl', name

    # 4 sheets in a file
    elif ('corp' in name_lower and 'faci' in name_lower) or ('comm' in name_lower and 'faci' in name_lower):
        return ('corpfac', name) if fn and '_all' in fn else ('fac', name)

    elif ('cons' in name_lower or 'individual' in name_lower) and 'faci' in name_lower:
        return 'ndvdlfac', name

    elif ('corp' in name_lower and 'subj' in name_lower) or ('comm' in name_lower and 'subj' in name_lower):
        return 'corp', name

    # facility data
    elif 'corp' in name_lower and 'credit' in name_lower:
        return ('corpfac', name) if fn and '_all' in fn else ('fac', name)

    elif 'individual' in name_lower and ('credit' in name_lower or 'facility' in name_lower):
        return ('ndvdlfac', name) if fn and '_all' in fn else ('fac', name)

    elif 'credit' in name_lower and ('info' in name_lower or 'fac' in name_lower):
        return 'fac', name

    elif 'credit' in name_lower and 'info' in name_lower:
        return 'fac', name

    # subject data
    elif 'cons' in name_lower and 'borrow' in name_lower:
        return 'ndvdl', name
    elif 'individual' in name_lower and 'borrow' in name_lower:
        return 'ndvdl', name

    elif 'individual' in name_lower and 'subject' in name_lower:
        return 'ndvdl', name

    elif 'corporate' in name_lower and 'borrow' in name_lower:
        return 'corp', name

    elif 'corporate' in name_lower and 'subject' in name_lower:
        return 'corp', name

    # un updated data files
    elif 'corp' in name_lower and 'update' in fn.lower():
        return 'corpfac', name
    elif 'cons' in name_lower and 'update' in fn.lower():
        return 'ndvdlfac', name

    #
    elif 'principal' == name_lower or 'principals' == name_lower or 'officers' == name_lower or (
            'principal' in name_lower and 'officers' in name_lower):
        return 'prnc', name

    #
    elif 'guarantor' == name_lower or 'guarantors' == name_lower or (
            ('guarantor' in name_lower or 'guarantors' in name_lower) and 'information' in name_lower):
        return 'grntr', name
