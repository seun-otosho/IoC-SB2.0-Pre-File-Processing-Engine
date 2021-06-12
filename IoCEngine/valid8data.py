def over_dues(df):
    # df['overdue_amt'] = df.overdue_amt.apply()
    df[(df.overdue_days.isnull() & df.overdue_amt.isnull()),
       'status'
    ] = 'Reject'

    df[(df.overdue_days.isnull() & df.overdue_amt.isnull()),
       'errors'
    ] = ['columns 3 and 4 are empty']
