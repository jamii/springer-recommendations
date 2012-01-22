import json

import mr

def cliqueness(dois):
    """Percentage of recommendations for this set which are not in this set"""
    dois = set(dois)
    inside = 0
    outside = 0
    for doi in dois:
        try:
            recommendations = json.load(open(mr.result_filename('recommendations', doi)))
            for (score, recommendation) in recommendations:
                if recommendation in dois:
                    inside += 1
                else:
                    outside += 1
        except IOError:
            # file doesn't exist, presumably we have no data for this doi
            pass
    return float(inside) / (inside+outside) # !!! -1 is temporary
