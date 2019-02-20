#!/usr/bin/env python

import shutil
from subprocess import check_call, check_output
import os


def build(workspace='.', twine_username=None, upload=False, run_tests_only=False):

    if not run_tests_only:
        print('--> Remove any old build or sdist folders')
        for subfolder in ['build', 'dist']:
            folder = os.path.join(workspace, subfolder)
            if os.path.exists(folder):
                shutil.rmtree(folder)

        print('-->  Build the wheel')
        check_call(['python3', 'setup.py', 'sdist', 'bdist_wheel'], cwd=workspace)

        wheels = [w for w in os.listdir(os.path.join(workspace, 'dist')) if w.endswith('whl')]
        assert len(wheels) == 1
        wheel = os.path.join('dist', wheels[0])

        print('--> Install the wheel')
        check_call(['pip3', 'install', wheel], cwd=workspace)

    print('--> Run unit-tests and coverage')
    check_call(['coverage', 'run', '--branch', '--omit', '*/lib/*,*/_version.py', '-m', 'unittest', 'discover',
                '-s', 'test/', '-p', '*_tests.py'], cwd=workspace)

    git_hash = check_output(['git', 'rev-parse', '--short', 'HEAD']).decode().strip()

    print('--> Generate the coverage html')
    check_call(['coverage', 'html', '-d', './coverage', '--title', 'Code Coverage for %s' % git_hash], cwd=workspace)
    print('View Coverage at: %s' % os.path.abspath(os.path.os.path.join(workspace, 'coverage/index.html')))

    print('--> Uploading coverage report to codecov')
    check_call(['codecov'], cwd=workspace)
    
    if upload and twine_username:
        print('--> Uploading pip package')
        check_call(['twine', 'upload', '--verbose', '--username', twine_username, 'dist/*'], cwd=workspace)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="Build FireX App")
    parser.add_argument('--workspace', default='.')
    parser.add_argument('--twine_username', default='firexdev')
    parser.add_argument('--upload', action='store_true')
    parser.add_argument('--run_tests_only', action='store_true')

    args, unknown = parser.parse_known_args()

    build(workspace=args.workspace,
          twine_username=args.twine_username,
          upload=args.upload,
          run_tests_only=args.run_tests_only)
