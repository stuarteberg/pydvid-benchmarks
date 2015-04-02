import re

class DvidUrlFields(object):
    """
    Parse the url to a dvid volume into components for hostname, uuid, dataname, and a dict of query_args.
    
    Example:
    
        >>> url_fields = DivdUrlFields("http://emdata1:8000/api/repo/deadbeef/grayscale?throttle=on")
        >>> url_fields.hostname
        'emdata1:8000'
        >>> url_fields.uuid
        'deadbeef'
        >>> url_fields.dataname
        'grayscale'
        >>> url_fields.query_args
        {'throttle': 'on'}
    """
    def __init__(self, url_string):
        self.url_string = url_string

        # Parse
        url_format = "^protocol://hostname/api/repo/uuid/dataname(\\?query_string)?"
        for field in ['protocol', 'hostname', 'uuid', 'dataname', 'query_string']:
            url_format = url_format.replace( field, '(?P<' + field + '>[^?]+)' )
        match = re.match( url_format, url_string )
        if not match:
            raise Exception("Could not parse dvid url: {}\n".format( url_string ))
            
        for k,v in match.groupdict().items():
            setattr(self, k, v)

        if self.query_string:
            self.query_args = dict( map(lambda s: s.split('='), self.query_string.split('&')) )
        else:
            self.query_args = {}

    def __str__(self):
        return self.url_string

if __name__ == "__main__":
    import doctest
    doctest.testmod()
