#  -*- coding: utf-8 -*-
"""
Manage transactions
"""

import logging
import subprocess as sp

import leip

lg = logging.getLogger(__name__)


@leip.hook('prepare', 5)
def prep_transact(app):
    if not 'run' in app.leip_commands:
        return

    commands = [app.leip_commands['run'], app.leip_commands['pbs']]

    for rc in commands:
        parser = rc._leip_command_parser
        parser.add_argument(
            '--fts',
            '--force-transaction-save',
            action='store_true',
            help="ensure this transaction is saved, even when " +
            " the job is skipped")
        parser.add_argument(
            '--sts',
            '--skip-transaction-save',
            action='store_true',
            help="skip saving transcation")


def save_transaction(job):
    """ save transaction """
    cl = get_transaction_cl()
    lg.info("save transaction")
    print(job.conf)
    #sp.call(cl, shell=True)


def get_transaction_cl(job) -> str:
    """ get the save transaction command line """
    cl = 'mad ta add'.split()

    cl.append('--script "%s"' % job.main_script)

    for xc in job.data['executable']:
        cl.append('--executable %s' % xc)

    for io in job.data['io']:
        filename = job.ctx[io['name']]
        name = io['name']
        cat = io['cat']

        if isinstance(filename, str):
            cl.append(' --%s %s:%s' % (cat, name, filename))
        elif isinstance(filename, list):
            for fn in filename:
                cl.append(' --%s %s:%s' % (cat, name, fn))

    return " ".join(cl)


@leip.hook('pre_run')
def add_ta_to_epilog(app, job):
    mode = app.conf['plugin']['transact'].get('run_or_save', 'save')
    tacl = get_transaction_cl(job)
    if mode == 'run':
        if not app.trans['args'].sts:
            job.ctx['epilog'].append(tacl)
    elif mode == 'save':
        with open('mad.transaction.sh', 'a') as F:
            F.write("\n" + tacl + "\n")


@leip.hook('skip_run')
def skiprun(app, job):
    # save transaction if fts is specified
    job.save_scripts()
    if app.trans['args'].fts:
        save_transaction(job)


@leip.hook('dry_run')
def dryrun(app, job):
    skiprun(app, job)


@leip.hook('post_run')
def postrun(app, job):
    # save transaction
    if not app.trans['args'].sts:
        pass
        #save_transaction(job)
