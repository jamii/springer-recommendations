import flask
import urllib

import db

app = flask.Flask('Springer Recommendations')

scores = db.SingleValue('live', 'scores')

@app.route('/recommendations/<path:doi>')
def recommendations(doi):
    print "Got: ", doi
    try:
        return flask.jsonify(recommendations=scores.get(doi))
    except KeyError:
        return flask.jsonify(recommendations=[])

if __name__ == '__main__':
    app.run(port=8000)
