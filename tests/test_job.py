
import os
import tempfile
import yaml

import leip
from path import Path
import pytest

from kea3.job import K3Job


def test_test():
    assert 1 == 1

    
def test_leip_app_instantiate():
    # see if the leip app loads properly
    app = leip.app(name='kea3')
    assert isinstance(app, leip.app)
    assert 'run' in app.leip_commands
    assert 'conf.show' in app.leip_commands

    
def test_k3_job_instantiate():
    app = leip.app(name='kea3')
    job = K3Job(app, [])
    assert isinstance(job, K3Job)


@pytest.fixture
def template_test_01():
    return Path(__file__).dirname() / 'data' / 'template' / 'test01.k3'


@pytest.fixture
def kea3_leip_app():
    return leip.app(name='kea3')


def test_k3_job_load_template(kea3_leip_app, template_test_01):
    app = kea3_leip_app
    job = K3Job(app, {}, template=template_test_01)
    assert isinstance(job, K3Job)
    with tempfile.TemporaryDirectory() as tmpdir:
        t = Path(tmpdir)
        os.chdir(tmpdir)
        job.get_template()

        k3dir = t / 'k3'
        template_dir = k3dir / job.name
        template_file = template_dir / 'template.k3'
        
        # is a k3 subdir created?
        assert k3dir.exists()
        assert template_dir.exists()
        assert template_file.exists()

        with open(template_file, 'r') as F:
            file_a = yaml.load(F)

        with open(template_test_01, 'r') as F:
            file_b = yaml.load(F)

        assert file_a['template'] == file_b['template']
        assert file_a['io'] == file_b['io']
