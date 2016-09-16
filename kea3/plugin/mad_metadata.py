import os
import leip

from mad2.util import get_mad_file

MADAPP = None


def get_mad_app():
    global MADAPP
    if not isinstance(MADAPP, leip.app):
        MADAPP = leip.app(name='mad2', disable_commands=True)
    return MADAPP


@leip.hook('expanded')
def madexpand(app, job):
    for io in job.data['io']:
        iname = io['name']
        fname = job.ctx[iname]
        if not isinstance(fname, str):
            continue
        if os.path.exists(fname):
            madfile = get_mad_file(get_mad_app(), fname)
            d = {}
            for s in madfile.stack[::-1]:
                d.update(dict(s))
            if not 'mad' in job.ctx:
                job.ctx['mad'] = {}
            job.ctx['mad'][iname] = d
#    print(job.ctx)
