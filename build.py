#!/usr/bin/env python

from subprocess import check_call, check_output
import os


def build(source, sudo=False):
    sudo_cmd = ['sudo'] if sudo else []
    print('--> Remove any old build or sdist folders')
    for subfolder in ['build', 'dist']:
        folder = os.path.join(source, subfolder)
        if os.path.exists(folder):
            cmd = sudo_cmd + ['rm', '-rf', folder]
            check_call(cmd, cwd=source)

    print('-->  Build the wheel')
    cmd = sudo_cmd + ['python3', 'setup.py', 'sdist', 'bdist_wheel']
    check_call(cmd, cwd=source)

    wheels = [w for w in os.listdir(os.path.join(source, 'dist')) if w.endswith('whl')]
    assert len(wheels) == 1
    wheel = os.path.join('dist', wheels[0])

    print('--> Install the wheel')
    cmd = sudo_cmd + ['pip3', 'install', wheel]
    check_call(cmd, cwd=source)


def get_git_hash_tags_and_files(source):
    git_hash = check_output(['git', 'rev-parse', '--short', 'HEAD'], cwd=source).decode().strip()
    print('Git Hash @ HEAD: %s' % git_hash)
    git_tags = [tag.decode() for tag in check_output(['git', 'tag', '-l', '--points-at', 'HEAD'],
                                                     cwd=source).splitlines()]
    if git_tags:
        print('Git tags @ HEAD: %s' % git_tags)
    files = [file.decode() for file in check_output(['git', 'diff-tree', '--no-commit-id', '--name-only', '-r', 'HEAD'],
                                                    cwd=source).splitlines()]
    print('Files @ HEAD: %s' % '\n'.join(files))
    return git_hash, git_tags, files


def run_tests(source, output_dir):
    unit_cov_file = run_unit_tests(source, output_dir)
    flow_cov_file = run_flow_tests(source, output_dir)

    if flow_cov_file and os.path.exists(flow_cov_file):
        print('--> Merge coverage data')
        check_call(['coverage', 'combine', unit_cov_file, flow_cov_file], cwd=output_dir)


def run_unit_tests(source, output_dir):
    coverage_file = os.path.join(output_dir, '.coverage')
    env = os.environ.copy()
    env['COVERAGE_FILE'] = coverage_file
    print('--> Run unit-tests and coverage')
    unit_test_dir = os.path.join(source, "tests", "unit_tests")
    check_call(['coverage', 'run', '-m', 'unittest', 'discover', '-s', unit_test_dir, '-p', '*_tests.py'],
               cwd=source, env=env)

    # unit test coverage file is located in the cwd
    return coverage_file


def run_flow_tests(source, output_dir):
    results_dir = os.path.join(output_dir, 'flow_test_results')
    print('--> Run flow-tests and coverage')
    flow_test_dir = os.path.join(source, "tests", "integration_tests")
    if not os.path.exists(flow_test_dir):
        return None
    check_call(['flow_tests', "--logs", results_dir, "--coverage", "--tests", flow_test_dir], cwd=source)

    # flow test coverage file is located in the results dir
    return os.path.join(results_dir, ".coverage")


def upload_coverage_to_codecov(source, output_dir, sudo=False):
    print('--> Copying .coverage into source directory')
    sudo_cmd = ['sudo'] if sudo else []
    coverage_report = os.path.join(output_dir, '.coverage')
    cmd = sudo_cmd + ['cp', coverage_report, source]
    check_call(cmd)

    print('--> Uploading coverage report to codecov')
    check_call(sudo_cmd + ['codecov'], cwd=source)


def generate_htmlcov(source, output_dir, git_hash=None):
    if not git_hash:
        git_hash, _, __ = get_git_hash_tags_and_files(source)
    print('--> Generate the coverage html')
    check_call(['coverage', 'html', '--title', 'Code Coverage for %s' % git_hash], cwd=output_dir)
    print('View Coverage at: %s' % os.path.abspath(os.path.os.path.join(output_dir, 'htmlcov/index.html')))


def upload_pip_pkg_to_pypi(source, twine_username):
    print('--> Uploading pip package')
    cmd = ['twine', 'upload', '--verbose', '--username', twine_username, 'dist/*']
    check_call(cmd, cwd=source)


def build_sphinx_docs(source, sudo=False):
    sudo_cmd = ['sudo'] if sudo else []
    print('--> Building the Docs')
    cmd = sudo_cmd + ['sphinx-build', '-b', 'html', 'docs', 'html']
    check_call(cmd, cwd=source)


def run(source='.', skip_build=None, upload_pip=None, upload_pip_if_tag=None, twine_username=None, skip_htmlcov=None,
        upload_codecov=None, skip_docs_build=None, sudo=False, output_dir='.'):

    git_hash, git_tags, files = get_git_hash_tags_and_files(source)

    if not skip_build:
        build(source, sudo)

    run_tests(source, output_dir)

    if not skip_htmlcov:
        generate_htmlcov(source, output_dir, git_hash)

    if upload_codecov:
        upload_coverage_to_codecov(source, output_dir, sudo)

    if upload_pip or (upload_pip_if_tag and git_tags):
        upload_pip_pkg_to_pypi(source, twine_username)

    if not skip_docs_build:
        build_sphinx_docs(source, sudo)


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--source', default='.')
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
    do_all.add_argument('--sudo', action='store_true')
    do_all.add_argument('--output_dir', default='.')
    do_all.set_defaults(func=run)

    upload = sub_parser.add_parser("upload_pip")
    upload.add_argument('--twine_username', default='firexdev')
    upload.set_defaults(func=upload_pip_pkg_to_pypi)

    output_dir_parser = argparse.ArgumentParser(add_help=False)
    output_dir_parser.add_argument('--output_dir', default='.')

    sudo_parser = argparse.ArgumentParser(add_help=False)
    sudo_parser.add_argument('--sudo', action='store_true')

    output_functions = {
        "tests": (run_tests, output_dir_parser),
        "unit_tests": (run_unit_tests, output_dir_parser),
        "integration_tests": (run_flow_tests, output_dir_parser),
        "cov_report": (generate_htmlcov, output_dir_parser),
        "upload_codecov": (upload_coverage_to_codecov, output_dir_parser, sudo_parser),
        "build": (build, sudo_parser),
        "docs": (build_sphinx_docs, sudo_parser),
    }
    for name, data in output_functions.items():
        func = data[0]
        sub = sub_parser.add_parser(name, parents=data[1:])
        sub.set_defaults(func=func)

    args, unknown = parser.parse_known_args()

    arguments = dict(vars(args))
    arguments.pop("func", "")
    args.func(**arguments)


if __name__ == '__main__':
    main()
