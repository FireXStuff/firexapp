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
    print('Files @ HEAD: %s' % '\n'.join(files))
    return git_hash, git_tags, files


def run_tests(workspace):
    unit_cov_file = run_unit_tests(workspace)
    flow_cov_file = run_flow_tests(workspace)

    if flow_cov_file and os.path.exists(flow_cov_file):
        print('--> Merge coverage data')
        check_call(['coverage', 'combine', unit_cov_file, flow_cov_file], cwd=workspace)


def run_unit_tests(workspace):
    print('--> Run unit-tests and coverage')
    unit_test_dir = os.path.join(workspace, "tests", "unit_tests")
    check_call(['coverage', 'run', '-m', 'unittest', 'discover', '-s', unit_test_dir, '-p', '*_tests.py'],
               cwd=workspace)

    # unit test coverage file is located in the cwd
    return os.path.join(workspace, '.coverage')


def run_flow_tests(workspace):
    print('--> Run flow-tests and coverage')
    flow_test_dir = os.path.join(workspace, "tests", "integration_tests")
    if not os.path.exists(flow_test_dir):
        return None
    check_call(['flow_tests', "--coverage", "--tests", flow_test_dir], cwd=workspace)

    # flow test coverage file is located in the results dir
    return os.path.join(workspace, "results", ".coverage")


def upload_coverage_to_codecov(workspace):
    print('--> Uploading coverage report to codecov')
    check_call(['codecov'], cwd=workspace)


def generate_htmlcov(workspace, git_hash=None):
    if not git_hash:
        git_hash, _, __ = get_git_hash_tags_and_files(workspace)
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


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--workspace', default='.')
    parser.set_defaults(func=run)
    sub_parser = parser.add_subparsers()
    do_all = sub_parser.add_parser("all")
    do_all.add_argument('--skip_build', action='store_true')
    do_all.add_argument('--upload_pip', action='store_true')
    do_all.add_argument('--upload_pip_if_tag', action='store_true')
    do_all.add_argument('--twine_username', default='firexdev')
    do_all.add_argument('--skip_htmlcov', action='store_true')
    do_all.add_argument('--upload_codecov', action='store_true')
    do_all.add_argument('--skip_docs_build', action='store_true')
    do_all.set_defaults(func=run)

    upload = sub_parser.add_parser("upload_pip")
    upload.add_argument('--twine_username', default='firexdev')
    upload.set_defaults(func=upload_pip_pkg_to_pypi)

    functions = {
        "build": build,
        "tests": run_tests,
        "unit_test": run_unit_tests,
        "integration_tests": run_flow_tests,
        "cov_report": generate_htmlcov,
        "upload_codecov": upload_coverage_to_codecov,
        "docs": build_sphinx_docs
    }
    for name, func in functions.items():
        sub = sub_parser.add_parser(name)
        sub.set_defaults(func=func)

    args, unknown = parser.parse_known_args()

    arguments = dict(vars(args))
    arguments.pop("func", "")
    args.func(**arguments)


if __name__ == '__main__':
    main()
