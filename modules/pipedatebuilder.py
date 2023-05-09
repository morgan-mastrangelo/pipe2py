# pipedatebuilder.py
#

from pipe2py import util

from datetime import datetime, timedelta

def pipe_datebuilder(context, _INPUT, conf, **kwargs):
    """This source builds a date and yields it forever.
    
    Keyword arguments:
    context -- pipeline context
    _INPUT -- XXX
    conf:
        DATE -- date
    
    Yields (_OUTPUT):
    date
    """
    for item in _INPUT:
        date = util.get_value(conf['DATE'], item, **kwargs)
        try:
            date = float(date)
        except ValueError: pass
        if type(date) == float or type(date) == int:
            date = datetime.utcfromtimestamp(date)
        else:
            date = str(date).lower()
            if date.endswith(' day') or date.endswith(' days'):
                count = int(date.split(' ')[0])
                date = (datetime.utcnow() + timedelta(days=count))
            elif date.endswith(' year') or date.endswith(' years'):
                count = int(date.split(' ')[0])
                date = datetime.utcnow()
                date = date.replace(year = date.year + count)
            elif date == 'today':
                date = datetime.utcnow()
            elif date == 'tomorrow':
                date = (datetime.utcnow() + timedelta(days=1))
            elif date == 'yesterday':
                date = (datetime.utcnow() + timedelta(days=-1))
            elif date == 'now':  #todo is this allowed by Yahoo?
                date = datetime.utcnow()
            else:
                for df in util.ALTERNATIVE_DATE_FORMATS:
                    try:
                        date = datetime.strptime(date, df)
                        break
                    except:
                        pass
                else:
                    #todo: raise an exception: unexpected date format
                    pass
            
        yield date
