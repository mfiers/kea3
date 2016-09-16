import sys

import argparse
import logging
import leip
from xtermcolor import colorize as cz

from kea3.job import K3Job

lg = logging.getLogger('k3.run')

TEMPLATE = None

@leip.arg('arguments', nargs=argparse.REMAINDER)
@leip.arg('-n', '--jobstorun', help='no of jobs to run', type=int)
@leip.arg('-j',
          '--threads',
          help='no of jobs to run in parallel',
          type=int,
          default=1)
@leip.flag('-d', '--dryrun', help='do not run')
@leip.flag(
    '-B', '--always_run', dest='force', help='force run, regardless of checks')
@leip.arg('template')
@leip.command
def t(app, args):
    args.transient = True
    return run(app, args)
    

@leip.arg('arguments', nargs=argparse.REMAINDER)
@leip.arg('-n', '--jobstorun', help='no of jobs to run', type=int)
@leip.arg('-j',
          '--threads',
          help='no of jobs to run in parallel',
          type=int,
          default=1)
@leip.flag('-d', '--dryrun', help='do not run')
@leip.flag('-t', '--transient', help='do not copy the template')
@leip.flag(
    '-B', '--always_run', dest='force', help='force run, regardless of checks')
@leip.arg('template')
@leip.command
def run(app, args):

    # first - maintain a run.sh script
    if ('-h' not in sys.argv) and ('--help' not in sys.argv):
        with open('run.sh', 'a') as F:
            F.write('# %s\n' % " ".join(sys.argv))

    job = K3Job(app, args, args.template, args.arguments,
                transient = args.transient)

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


@leip.flag('-r', '--raw')
@leip.arg('template', default='.', nargs='?')
@leip.commandName('show')
def k3_show(app, args):
    job = K3Job(app, args, args.template)
    job.get_template()
    job.load_template()

    if args.raw:
        print(job.data.pretty())
        return

    c_green = 106
    c_red = 160
    c_blue = 26
    c_grey = 242

    print(cz('name:', ansi=c_red), end=" ")
    print(cz(job.name, ansi=c_green))
    print(cz('mode:', ansi=c_red), end=" ")
    print(cz(job.data.get('mode', 'map'), ansi=c_green))
    print(cz('io:', ansi=c_red))
    for io in job.data['io']:
        nof = ''
        if 'expanded' in io:
            nof = ' (#%d)' % len(io['expanded'])
        print('- %s: %s%s' %
              (cz(io['name'], ansi=c_blue), io.get('default', ''), nof))

    print(cz('parameter', ansi=c_red))
    for par in job.data['parameters']:
        nof = ''
        if 'expanded' in par:
            nof = ' (#%d)' % len(par['expanded'])
        print('- %s: %s%s' % (cz(par['name'], ansi=c_blue),
                              par.get('default', '<none>'), nof))

    print(cz(('-' * 30 + 'template' + '-' * 30), ansi=c_grey))
    print(job.data['template'])


@leip.arg('template')
@leip.commandName('update')
def k3_update(app, args):
    """Update local (in this wd) template file from another template
    file

    """
    pass


@leip.arg('value')
@leip.arg('key')
@leip.commandName('set')
def k3_set(app, args):
    job = K3Job(app, args)
    job.prepare()
    # see if this is in io or parmeters
    k, v = args.key, args.value

    print(job.data[args.key])
