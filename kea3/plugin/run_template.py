
import argparse
from collections import defaultdict
import copy
from datetime import datetime
import glob
import logging
import re
import subprocess as sp

import fantail
import jinja2
import leip
from path import Path
import sh

from kea3.job import K3Job

lg = logging.getLogger('k3.run')

TEMPLATE = None



@leip.arg('arguments', nargs=argparse.REMAINDER)
@leip.arg('-n', '--jobstorun', help='no of jobs to run', type=int)
@leip.arg('-j', '--threads', help='no of jobs to run in parallel',
          type=int, default=1)
@leip.flag('-d', '--dryrun', help='do not run')
@leip.flag('-r', '--force', help='force run, regardless of time check')
@leip.arg('template')
@leip.command
def run(app, args):
    job = K3Job(app, args, args.template, args.arguments)
    job.prepare()

    # expand - generate a subjob for possible io/globs
    jobstorun = args.jobstorun

    if args.threads == 1:
        for i, newjob in enumerate(job.expand()):
            if jobstorun is not None and i >= jobstorun:
                break
            newjob.run()
    else:
        import multiprocessing.dummy as mp
        import itertools

        def _runner(runjob):
            runjob.run()

        p = mp.Pool(args.threads)
        if jobstorun is not None:
            p.map(_runner, itertools.islice(job.expand(), jobstorun))
        else:
            p.map(_runner, job.expand())


@leip.commandName('show')
def k3_show(app, args):
    job = K3Job(app, args)
    job.prepare()
    print('mode: %s' % job.data.get('mode', 'map'))
    print('io')
    for io in job.data['io']:
        nof = ''
        if 'expanded' in io:
            nof = ' (#%d)' % len(io['expanded'])
        print('- %s: %s%s' % (io['name'], io['default'], nof))

    print('parameter')
    for par in job.data['parameters']:
        nof = ''
        if 'expanded' in par:
            nof = ' (#%d)' % len(par['expanded'])
        print('- %s: %s%s' % (par['name'], par['default'], nof))


@leip.arg('value')
@leip.arg('key')
@leip.commandName('set')
def k3_set(app, args):
    job = K3Job(app, args)
    job.prepare()
    # see if this is in io or parmeters
    k, v = args.key, args.value

    print(job.data[args.key])
