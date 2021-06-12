"""

level 4

principal and interest
    overdue days and overdue days > 0
    overdue amt and overdue amt > 0

    overdue amt and overdue days > 0
    overdue days and overdue amt > 0

date acct closed <= current date / date reported / file creation date
date loan first disbursed <= current date / date reported / file creation date
last | principal payment date <= current date / date reported / file creation date
interest payment date <= current date / date reported / file creation date
approved date <= current date / date reported / file creation date

date loan first disbursed <= date acct closed
date loan first disbursed <= last payment date
date loan first disbursed <= date planned close
date loan first disbursed >= approved date

date loan restructured <= date acct closed

maturity date >= approved date





principal payment date <= date acct closed
principal payment date > approved date
principal payment date > date loan first disbursed


interest payment date <= date acct closed
interest payment date > approved date
interest payment date > date loan first disbursed


all segments
    facility branch == subject branch









date of birth < current date / date reported / file creation date
date of birth < last payment date
date of birth < date loan first disbursed
date of birth < date loan restructured
date of birth < last payment date
date of birth < date planned close

date of birth < principal payment date
date of birth < interest payment date
date of birth < date loan first disbursed
date of birth < date loan restructured
date of birth < approved date


consent status null default is 001 not provided
    consent status == '003'
        consent from date and consent to data

        consent to date >= current date / date reported and vice versa
        consent to date >= consent from date and vice versa
        consent to date < current date



loan type == 'credit card'
    primary card number


first name
last name
name or (first name and last name)




"""
