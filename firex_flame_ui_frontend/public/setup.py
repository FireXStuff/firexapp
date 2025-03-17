from setuptools import setup
import versioneer


def drop_dirty_if_exact_tag(version: str) -> str:
    # 0.29.41+1.g086ea28.dirty -> 0.29.41
    if '+' in version:
        # '0.10', '0.gcb0a42d.dirty' = '0.10+0.gcb0a42d.dirty'.split('+')
        version_parts = version.split('+', maxsplit=1)
        if (
            len(version_parts) > 1
            and version_parts[-1].endswith('.dirty')
        ):
            return version_parts[0] # the git tag.
    return version


setup(
    name='firex_flame_ui',
    # A built UI workspace is always dirty, since build artifacts are inside the git repo.
    # It's therefore necessary to treat the first dirty commit as a clean tag.
    version=drop_dirty_if_exact_tag(versioneer.get_version()),
    cmdclass=versioneer.get_cmdclass(),
    description='UI for FireX.',
    url='https://github.com/FireXStuff/firex-flame-ui',
    author='Core FireX Team',
    author_email='firex-dev@gmail.com',
    license='BSD-3-Clause',
    packages=['firex_flame_ui'],
    # 'npm run build' is configured to put build artifacts & files in this 'public' folder in the 'dist' directory.
    # Therefore, at python packaging time, both python files & UI build artifacts are in the same (current) directory.
    package_dir={'firex_flame_ui': './'},
    zip_safe=True,
    include_package_data=True,
    package_data={
        # Files available to consumers of this python package (UI build artifacts).
        # NOTE: these expressions must also be present in MANIFEST.in
        'firex_flame_ui': ['*.html', 'assets/*', 'COMMITHASH'],
    },
    entry_points={},
    )
