import argparse

import copy
from datetime import datetime
import glob
import logging
import re
import subprocess as sp

import fantail
import fantail.util
import jinja2
from path import Path

lg = logging.getLogger('k3.job')

TEMPLATE = None


class K3Job:
    def __init__(self, app, args, template='.', argv=[]):
        lg.debug("Instantiate job with template: %s", template)
        self.app = app
        self.runargs = args
        self.skipped = False
        self.argv = argv

        # template name this object got called with
        self.template = template

        # the context will be used for parameter expaions
        self.ctx = fantail.Fantail()

    @property
    def workdir(self):
        wd = Path('./k3') / self.name
        wd.makedirs_p()
        return wd

    @property
    def template_file(self):
        return self.workdir / 'template.k3'

    @property
    def cl_arg_file(self):
        return self.workdir / 'arguments.k3'

    def prepare(self):
        self.get_template()
        self.load_template()
        self.parse_arguments()
        self.prepare_io()

    def get_template(self):
        """
        Find, copy and load the template
        """
        if '~' in self.template:
            self.template = str(Path(self.template).expanduser())

        if Path(self.template).isdir():
            k3dir = Path(self.template) / 'k3'
            subdirs = k3dir.dirs() if k3dir.exists() else []
            if len(subdirs) == 0:
                lg.warning("No template found")
                exit(-1)
            if len(subdirs) > 1:
                lg.error("Multiple templates found: %s",
                         ", ".join([x.basename() for x in subdirs]))
                lg.error("Please specify one")
                exit()
            self.name = subdirs[0].basename()
            lg.info('template "%s" found in  %s', self.name, self.template)

            if not self.template == '.':
                # not pointing at the current folder
                # copy the template (& argument file)
                template_file = Path(self.template) / 'k3' / \
                    self.name / 'template.k3'

                if template_file != self.template_file:
                    template_file.copy(self.template_file)

        elif re.match('[A-Za-z_]\w*', self.template):
            # assume the template is already present
            self.name = self.template

        elif Path(self.template).exists() and self.template.endswith('.k3'):
            # template points to a file - get it
            lg.debug("Found template file: %s", self.template)
            template_file = Path(self.template)
            self.name = template_file.basename().replace('.k3', '')

            if template_file != self.template_file:

                # load & save
                self.data = fantail.yaml_file_loader(template_file)
                # check file references:
                for fref in 'template epilog prolog'.split():
                    tpath = self.data.get(fref, '')
                    if tpath.startswith('file://'):
                        tpath = tpath.replace('file://', '')
                        if not tpath.startswith('/'):
                            tpath = Path(template_file).dirname() / tpath

                        with open(tpath) as FT:
                            self.data[fref] = FT.read()

                lg.debug("Copying template_file to %s", self.template_file)
                self.save_template()
#                fantail.yaml_file_save(self.data, self.template_file)

        else:
            raise NotImplementedError("Need other source for templates: %s",
                                      self.template)

        lg.debug("Template name is: %s", self.name)
        if not self.template_file.exists():
            lg.error("No valid template found in %s", self.workdir)
            exit(-1)

        lg.debug("Found template: %s", self.name)

        self.ctx['template']['name'] = self.name

    def load_template(self):
        """
        Load the local template.
        """
        self.data = fantail.yaml_file_loader(self.template_file)
#        if self.data.get('template', '').startswith('file://'):
#            self.data['template'] = fantail.yaml_file_loader(
#                sefl.data['template'].replace('file://', '')

    def save_template(self):
        """
        Save the current data structure to the local template
        """

        self.data['template'] = fantail.util.literal_str(self.data['template'])
        fantail.yaml_file_save(self.data, self.template_file)

    def parse_arguments(self):
        def _process_parameter(parser, par, cl_args):
            a_arg, a_kwarg = [], {}
            parname = par['name']

            if parname in cl_args:
                default_value = cl_args[parname]
            elif 'default' in par:
                default_value = par['default']
            else:
                default_value = None

            if par.get('hidden_arg', False):
                a_kwarg['help'] = argparse.SUPPRESS
            elif 'help' in par:
                a_kwarg['help'] = par['help']
            if 'nargs' in par:
                if par['nargs'] == '*':
                    a_kwarg['nargs'] = argparse.REMAINDER
                else:
                    a_kwarg['nargs'] = par['nargs']
            if 'default' in par:
                a_arg.append('--' + parname)
                if not par.get('hidden_arg', False):
                    a_kwarg['help'] = \
                        (a_kwarg.get('help', '') +
                         ' (default: %s)' % par['default']).strip()
            else:
                a_arg.append(parname)

            if default_value is not None:
                a_kwarg['default'] = default_value

            if 'type' in par:
                a_kwarg['type'] = {'int': int, 'float': float}[par['type']]

            parser.add_argument(*a_arg, **a_kwarg)

        progname = 'k3 run %s' % self.name
        parser = argparse.ArgumentParser(prog=progname)

        for io in self.data['io']:
            _process_parameter(parser, io, self.data['cl_args'])

        for par in self.data['parameters']:
            _process_parameter(parser, par, self.data['cl_args'])

        # parse the command line args with the now populated parser
        self.args = parser.parse_args(self.argv)

        self.data['cl_args'].update(dict(self.args._get_kwargs()))

        for k, v in self.data['cl_args'].items():
            lg.debug('raw parameter: %s=%s', k, v)

        # expand variables & io parameters
        vctx = {}
        jinja_env = jinja2.Environment(undefined=jinja2.StrictUndefined)
        args_ok = set()
        iteration = 0
        last_no_args_ok = 0
        while True:
            iteration += 1
            for k, v in self.data['cl_args'].items():
                if not isinstance(v, str):
                    args_ok.add(k)
                    vctx[k] = v
                elif '{{' in v:
                    # attempt to expand
                    try:
                        nv = jinja_env.from_string(v).render(vctx)
                        # this seems to have worked - this var is ok
                        args_ok.add(k)
                        self.data['cl_args'][k] = nv
                        vctx[k] = nv
                        lg.debug('success fill for %s: %s -> %s', k, v, nv)

                    except jinja2.exceptions.UndefinedError:
                        lg.debug('(iteration %d - failed fill for %s: %s',
                                 iteration, k, v)
                        # somethings is still undefined - ignore
                        pass

                else:
                    vctx[k] = v
                    args_ok.add(k)

            lg.debug(('iteration: %d, no args: %d, args ok: %d, ' +
                      'args_ok_last_round: %d'), iteration,
                     len(self.data['cl_args']), len(args_ok), last_no_args_ok)

            # check if we're done
            if len(args_ok) == len(self.data['cl_args']):
                lg.debug("Parameter expansion, excellent, all is well")
                # done!
                break
            elif iteration > 5 or len(args_ok) == last_no_args_ok:
                lg.debug("cannot resolve parameters at the moment: %s" %
                         str(self.data['cl_args']))
                break
            else:
                last_no_args_ok = len(args_ok)
                # let's try another round
                pass

        for par in self.data['parameters']:
            lg.debug('par found: %s=%s', par['name'],
                     self.data['cl_args'][par['name']])
            par['pattern'] = self.data['cl_args'][par['name']]
            par['default'] = self.data['cl_args'][par['name']]

        for io in self.data['io']:
            io['default'] = self.data['cl_args'][io['name']]

        self.save_template()

    def prepare_io(self):
        """
        If there are glob patterns in the io fields - expand them now
        """

        glob_fields = []

        for io in self.data['io']:
            name = io['name']
            if 'cat' not in io:
                io['cat'] = {'in': 'input',
                             'ou': 'output',
                             'db': 'db'}.get(name.lower()[:2], 'unknown')

            value = self.data['cl_args'][name]

            io['pattern'] = value

            if '{*}' in value:
                glob_fields.append(name)

        if len(glob_fields) == 0:
            # nothing to expand - single run
            for io in self.data['io']:
                io['all'] = [io['pattern']]
            for par in self.data['parameters']:
                par['all'] = [par['pattern']]
            return

        if len(glob_fields) > 1:
            raise ValueError('more than one glob to unpack')

        # expand pattern

        gf = glob_fields[0]
        for io in self.data['io']:
            if gf == io['name']:
                pattern = io['pattern']
                break

        patloc = pattern.find('{*}')
        patstart = pattern[:patloc]
        patend = pattern[patloc + 3:]

        globpat = patstart + '*' + patend
        allfiles = glob.glob(globpat)

        lg.debug('io expansion of %d files', len(allfiles))

        for i, a in enumerate(allfiles):
            if len(patend) == 0:
                repl = a[len(patstart):]
            else:
                repl = a[len(patstart):-len(patend)]
            if i < 3:
                lg.debug(' (%d): %s - pattern: %s: %d %d', i, a, repl,
                         len(patend), len(a))

            # add a value to the 'expanded' list
            def _exp_add(d, v):
                if 'expanded' not in d:
                    d['expanded'] = []
                d['expanded'].append(v)

            for io in self.data['io']:
                name = io['name']
                if name == gf:
                    _exp_add(io, a)
                elif '{g}' in io['pattern']:
                    to_fill = io['pattern'].replace('{g}', repl)
                    _exp_add(io, to_fill)
            for par in self.data['parameters']:
                name = par['name']
                if isinstance(par['pattern'], str):
                    to_fill = par['pattern']
                    if '{g}' in str(par['pattern']):
                        to_fill = to_fill.replace('{g}', repl)
                    if '{*}' in str(par['pattern']):
                        to_fill = to_fill.replace('{*}', repl)
                    _exp_add(par, to_fill)

    def expand(self):

        self.ctx['epilog'] = []
        self.ctx['prolog'] = []

        if self.data.get('mode') in ['start', 'reduce']:
            lg.warning('%s mode - generate one job', self.data['mode'])
            self.ctx['i'] = 0
            for io in self.data['io']:
                if 'expanded' in io:
                    self.ctx[io['name']] = io['expanded']
                else:
                    self.ctx[io['name']] = io['pattern']
            for par in self.data['parameters']:
                if 'expanded' in par:

                    self.ctx[par['name']] = par['expanded']
                else:
                    self.ctx[par['name']] = par['pattern']

            self.app.run_hook('expanded', self)
            yield self
            return

        # assume map mode

        nojobs = set()
        expfields = []
        for io in self.data['io']:
            if 'expanded' in io:
                expfields.append(io['name'])
                nojobs.add(len(io['expanded']))
        for par in self.data['parameters']:
            if 'expanded' in par:
                expfields.append(par['name'])
                nojobs.add(len(par['expanded']))

        if len(nojobs) == 0:
            nojobs = 0
        elif len(nojobs) == 1:
            nojobs = list(nojobs)[0]
        else:
            raise Exception("Invalid job")

        lg.info("generating %d jobs", nojobs)

        for i in range(nojobs):
            # copying shallowly...
            newjob = copy.copy(self)
            # ...but ensure it has its own context
            newjob.ctx = fantail.Fantail()
            newjob.ctx.update(self.ctx)
            newjob.ctx['i'] = i

            # fill the io data into the ctx
            for io in self.data['io']:
                if 'expanded' in io:
                    newjob.ctx[io['name']] = io['expanded'][i]
                else:
                    newjob.ctx[io['name']] = io['pattern']

            for par in self.data['parameters']:
                if 'expanded' in par:
                    newjob.ctx[par['name']] = par['expanded'][i]
                else:
                    newjob.ctx[par['name']] = par['pattern']

            self.app.run_hook('expanded', newjob)
            yield newjob

    def check(self):

        if self.runargs.force:
            # force is true - run, don't check if this
            # might already have been done
            lg.debug("run - forced")
            return True

        latest_source_mtime = None
        earliest_output_mtime = None
        no_output = 0
        no_input = 0

        for io in self.data['io']:
            name = io['name']
            cat = io['cat']
            value = self.ctx[name]

            lg.info("%-10s %-10s %s", cat, name, value)

            if cat == 'executable':
                # do not check executables
                continue

            if isinstance(value, list):
                filenames = [Path(x) for x in value]
            else:
                filenames = [Path(value)]

            if cat == 'output':
                for f in filenames:
                    if not f.exists():
                        # an output file does not exist, run
                        lg.info('output file (%s) missing, run', f)

                        return True

            mtimes = [x.mtime for x in filenames]

            if cat == 'output':
                no_output += 1
                if earliest_output_mtime is None:
                    earliest_output_mtime = min(mtimes)
                else:
                    earliest_output_mtime = min(earliest_output_mtime,
                                                min(mtimes))
            else:
                no_input += 1
                if latest_source_mtime is None:
                    latest_source_mtime = max(mtimes)
                else:
                    latest_source_mtime = max(latest_source_mtime, max(mtimes))

        if (latest_source_mtime is None) or (earliest_output_mtime is None):
            return True

        if latest_source_mtime > earliest_output_mtime:
            lg.info("%d input file(s) newer than %d output file(s)", no_input,
                    no_output)
            return True
        else:
            lg.info("%d output file(s) newer than %d input file(s)", no_output,
                    no_input)
            return False

    def prep_save_scripts(self) -> None:

        stamp = datetime.utcnow()
        stamp = stamp.replace(microsecond=0)
        stamp = datetime.isoformat(stamp).replace(':', '').replace('-', '')

        self.ctx['stamp'] = stamp
        script_dir = self.workdir / 'script'
        script_dir.makedirs_p()

        self.prolog_script = script_dir \
            / ('%s__%s__%s.prolog.sh' % (self.name, self.ctx['i'], stamp))
        self.prolog_script = self.prolog_script.abspath()

        self.epilog_script = script_dir \
            / ('%s__%s__%s.epilog.sh' % (self.name, self.ctx['i'], stamp))
        self.epilog_script = self.epilog_script.abspath()

        self.main_script = (script_dir / (
            '%s__%s__%s_main' % (self.name, self.ctx['i'], stamp))).abspath()

    def save_scripts(self) -> dict:

        rv = {}

        if len(self.ctx['prolog']) > 0:
            with open(self.prolog_script, 'w') as F:
                F.write("#!/bin/bash\n\n")
                F.write("\n\n".join(self.ctx['prolog']))
                self.prolog_script.chmod('a+x')
                rv['prolog'] = self.prolog_script

        with open(self.main_script, 'w') as F:
            F.write(self.code)

        rv['main'] = self.main_script
        self.main_script.chmod('a+x')

        if len(self.ctx['epilog']) > 0:
            with open(self.epilog_script, 'w') as F:
                F.write("#!/bin/bash\n\n")
                F.write("\n\n".join(self.ctx['epilog']))
            self.epilog_script.chmod('a+x')
            rv['epilog'] = self.epilog_script

        return rv

    def executor(self, cl: list) -> int:
        cl = "; ".join(cl)
        lg.info('run: %s', cl)
        # print(cl)
        rc = sp.call(cl, shell=True)
        if rc != 0:
            lg.warning("Run finished with RC: %s", rc)
        else:
            lg.info("Run finished successfully")
        if rc == 0:
            self.app.run_hook('post_run', self)
        return rc

    def run(self) -> int:
        """ actually run """

        self.app.run_hook('pre_check', self)

        # first fix ctx - variables might still carry variables
        while True:
            no_changed = 0
            for k, v in self.ctx.items():
                if not isinstance(v, str):
                    continue
                if '{{' not in v and '{%' not in v:
                    continue
                # attempt render
                template = jinja2.Template(v)
                vv = template.render(self.ctx)
                if vv != v:
                    no_changed += 1
                    self.ctx[k] = vv
            if no_changed == 0:
                break

        template = self.data['template']
        _last_template = template
        _template_i = 0
        while True:
            _template_i += 1
            template = jinja2.Template(template)
            template = template.render(self.ctx)
            if '{{' not in template and '{%' not in template:
                break
            if template == _last_template:
                break
            if _template_i > 4:
                break
            _last_template = template

        self.code = template

        scripts = self.prep_save_scripts()

        self.app.run_hook('pre_run', self)

        scripts = self.save_scripts()

        cl = []

        if 'prolog' in scripts:
            cl.append('source %s' % scripts['prolog'])

        cl.append('%s' % scripts['main'])

        if 'epilog' in scripts:
            cl.append('%s' % scripts['epilog'])

        if not self.check():
            lg.info("skipping")
            self.skipped = True
            self.app.run_hook('skip_run', self)
            return

        lg.info("run job %d", self.ctx['i'])

        if self.runargs.dryrun:
            print(self.code)
            print("#" + '-' * 80)
            self.app.run_hook('dry_run', self)
            return 0
        else:
            # actually execute cl
            return self.executor(cl)
