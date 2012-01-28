import json
import os
import os.path
import itertools

import mr

def cliqueness(build_name, dois):
    """Percentage of recommendations for this set which are not in this set"""
    dois = set(dois)
    inside = 0
    outside = 0
    for doi in dois:
        try:
            recommendations = json.load(open(mr.result_filename(build_name, 'recommendations', doi)))
            for (score, recommendation) in recommendations:
                if recommendation in dois:
                    inside += 1
                else:
                    outside += 1
        except IOError:
            # file doesn't exist, presumably we have no data for this doi
            pass
    return float(inside) / (inside+outside)

class RegressionError(Exception):
    pass

def regression(old_build_name, new_build_name):
    num_files = 0

    old_walk = os.walk(mr.result_directory(old_build_name))
    new_walk = os.walk(mr.result_directory(new_build_name))
    for ((old_root, old_dirs, old_files), (new_root, new_dirs, new_files)) in itertools.izip(old_walk, new_walk):
        for (old_dir, new_dir) in itertools.izip(old_dirs, new_dirs):
            if old_dir != new_dir:
                print 'Directory structure does not match:\n%s\n%s' % (os.path.join(old_root, old_dir), os.path.join(new_root, new_dir))
                raise RegressionError()
        for (old_file, new_file) in itertools.izip(old_files, new_files):
            if old_file != new_file:
                print 'Filenames do not match:\n%s\n%s' % (os.path.join(old_root, old_file), os.path.join(new_root, new_file))
                raise RegressionError()
            old_result = json.load(open(os.path.join(old_root, old_file)))
            new_result = json.load(open(os.path.join(new_root, new_file)))
            if old_result != old_result:
                print 'File contents do not match:\n%s\n%s' % (os.path.join(old_root, old_file), os.path.join(new_root, new_file))
                raise RegressionError()
            num_files += 1

    print 'Regression test passed for builds:\n%s\n%s\n(%i files compared)' % (old_build_name, new_build_name, num_files)
