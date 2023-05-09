# pipexpathfetchpage.py
#

import urllib2
import re
from pipe2py import util


def pipe_xpathfetchpage(context, _INPUT, conf, **kwargs):
    """XPath Fetch Page module

    _INPUT -- not used since this does not have inputs.

    conf:
       URL -- url object contain the URL to download
       xpath -- xpath to extract
       html5 -- use html5 parser?
       useAsString -- emit items as string?

       Description: http://pipes.yahoo.com/pipes/docs?doc=sources#XPathFetchPage

       TODOS:
        - don't retrieve pages larger than 1.5MB
        - don't retrieve if page is not indexable.
    """
    urls = conf['URL']
    if not isinstance(urls, list):
        urls = [urls]

    for item in _INPUT:
        for item_url in urls:
            url = util.get_value(item_url, item, **kwargs)
            if context.verbose:
                print "XPathFetchPage: Preparing to download:",url
                
            try:
                content = util.fetch_url(url)
        
                xpath = util.get_value(conf["xpath"], _INPUT, **kwargs)
                html5 = False
                useAsString = False
                if "html5" in conf:
                    html5 = util.get_value(conf["html5"], _INPUT, **kwargs) == "true"
                if "useAsString" in conf:
                    useAsString = util.get_value(conf["useAsString"], _INPUT, **kwargs) == "true"
                
                
                if html5:
                    #from lxml.html import html5parser
                    #root = html5parser.fromstring(content)
                    from html5lib import parse
                    root = parse(content, treebuilder='lxml', namespaceHTMLElements=False)
                else:
                    from lxml import etree
                    root = etree.HTML(content)
                res_items = root.xpath(xpath)
                
                if context.verbose:
                    print "XPathFetchPage: found count items:",len(res_items)
                    if len(res_items) == 0:
                        print "Content: %s" % content
                for res_item in res_items:
                    i = util.etree_to_pipes(res_item) #TODO xml_to_dict(res_item)                    
                    if context.verbose:
                        print "--------------item data --------------------"
                        print i
                        print "--------------EOF item data ----------------"
                    if useAsString:
                        yield { "content" : unicode(i) }
                    else:
                        yield i
        
            except Exception, e:
                if context.verbose:
                    print "XPathFetchPage: failed to retrieve from:", url
        
                    print "----------------- XPathFetchPage -----------------"
                    import traceback
                    traceback.print_exc()
                    print "----------------- XPathFetchPage -----------------"
                raise

        if item == True: #i.e. this is being fed forever, i.e. not in a loop, so we just yield our item once
            break
            