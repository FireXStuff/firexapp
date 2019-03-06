#!/usr/bin/env python

import shutil
from subprocess import check_call, check_output
import os


def build(workspace):
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


def get_git_hash_tags_and_files(workspace):
    git_hash = check_output(['git', 'rev-parse', '--short', 'HEAD'], cwd=workspace).decode().strip()
    print('Git Hash @ HEAD: %s' % git_hash)
    git_tags = [tag.decode() for tag in check_output(['git', 'tag', '-l', '--points-at', 'HEAD'],
                                                     cwd=workspace).splitlines()]
    if git_tags:
        print('Git tags @ HEAD: %s' % git_tags)
    files = [file.decode() for file in check_output(['git', 'diff-tree', '--no-commit-id', '--name-only', '-r', 'HEAD'],
                                                    cwd=workspace).splitlines()]
    print ('Files @ HEAD: %s' % '\n'.join(files))
    return git_hash, git_tags, files


def run_tests(workspace):
    print('--> Run unit-tests and coverage')
    check_call(['coverage', 'run', '-m', 'unittest', 'discover', '-s', 'test/', '-p', '*_tests.py'], cwd=workspace)


def upload_coverage_to_codecov(workspace):
    print('--> Uploading coverage report to codecov')
    check_call(['codecov'], cwd=workspace)


def generate_htmlcov(workspace, git_hash):
    print('--> Generate the coverage html')
    check_call(['coverage', 'html', '--title', 'Code Coverage for %s' % git_hash], cwd=workspace)
    print('View Coverage at: %s' % os.path.abspath(os.path.os.path.join(workspace, 'htmlcov/index.html')))


def upload_pip_pkg_to_pypi(twine_username, workspace):
    print('--> Uploading pip package')
    check_call(['twine', 'upload', '--verbose', '--username', twine_username, 'dist/*'], cwd=workspace)    


def build_sphinx_docs(workspace):
    print('--> Building the Docs')
    check_call(['sphinx-build', '-b', 'html', 'docs', 'html'], cwd=workspace)


def run(workspace='.', skip_build=None, upload_pip=None, upload_pip_if_tag=None, twine_username=None, skip_htmlcov=None,
        upload_codecov=None, skip_docs_build=None):

    git_hash, git_tags, files = get_git_hash_tags_and_files(workspace)

    if not skip_build:
        build(workspace)

    run_tests(workspace)

    if not skip_htmlcov:
        generate_htmlcov(workspace, git_hash)

    if upload_codecov:
        upload_coverage_to_codecov(workspace)

    if upload_pip or (upload_pip_if_tag and git_tags):
        upload_pip_pkg_to_pypi(twine_username, workspace)

    if not skip_docs_build:
        build_sphinx_docs(workspace)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--workspace', default='.')
    parser.add_argument('--skip_build', action='store_true')
    parser.add_argument('--upload_pip', action='store_true')
    parser.add_argument('--upload_pip_if_tag', action='store_true')
    parser.add_argument('--twine_username', default='firexdev')
    parser.add_argument('--skip_htmlcov', action='store_true')
    parser.add_argument('--upload_codecov', action='store_true')
    parser.add_argument('--skip_docs_build', action='store_true')

    args, unknown = parser.parse_known_args()

    run(**vars(args))
