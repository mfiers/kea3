

import leip


@leip.hook('pre_run')
def module_prerun(app, job):
    """ prepend module loading """
    for module in job.data.get('modules', []):
        job.ctx['prolog'].insert(0, 'module load %s' % module)

