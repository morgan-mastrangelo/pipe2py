# pipestrconcat.py  #aka stringbuilder
#

from pipe2py import util

def pipe_strconcat(context, _INPUT, conf, **kwargs):
    """This source builds a string.
    
    Keyword arguments:
    context -- pipeline context
    _INPUT -- source generator
    conf:
        part -- parts
    
    Yields (_OUTPUT):
    string
    """
    if not isinstance(conf['part'], list):    #todo do we need to do this anywhere else?
        conf['part'] = [conf['part']]

    for item in _INPUT:
        s = ""
        for part in conf['part']:
            try:
                p = util.get_value(part, item, **kwargs)
                if p is None: p = ""
                s += p
            except AttributeError:
                continue  #ignore if the item is referenced but doesn't have our source field (todo: issue a warning if debugging?)
            except TypeError, e:
                if context.verbose:
                    print "pipe_strconcat: "+str(e)
    
        yield s

