import os

def allowed_file(filename, extensions_dict={'csv', 'gml', 'xml', 'json', 'xlsx', 'txt', 'kml', 'ttl'}):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in extensions_dict
