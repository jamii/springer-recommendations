import os
import flask
import urllib
import ujson
import plyvel

import recommendations

app = flask.Flask('Springer Recommendations')

recs_db = plyvel.DB(os.path.join(recommendations.data_dir, 'recs_db'), create_if_missing=True)

def load_recs(filename):
    for line in open(filename, 'r'):
        doi, recs = ujson.loads(line)
        recs_db.put(doi.encode('utf8'), ujson.dumps(recs))

@app.route('/recommendations/<path:doi>')
def get_recommendations(doi):
    print "Got: ", doi
    try:
        print ujson.loads(recs_db.get(doi.encode('utf8')))
        recs = ujson.loads(recs_db.get(doi.encode('utf8')))
    except:
        recs = []
    return flask.jsonify(recommendations=recs)

if __name__ == '__main__':
    # load_recs(os.path.join(recommendations.data_dir, 'raw_recs'))
    app.run(port=8000)
