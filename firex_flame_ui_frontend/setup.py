from setuptools import setup
# import versioneer

setup(name='firex_flame_ui',
      # version=versioneer.get_version(),
      version='0.3',
      # cmdclass=versioneer.get_cmdclass(),
      description='UI for FireX.',
      url='https://github.com/FireXStuff/firex-flame-ui',
      author='Core FireX Team',
      author_email='firex-dev@gmail.com',
      license='BSD-3-Clause',
      packages=['firex_flame_ui'],
      # npm run build puts artifact in the 'dist' directory.
      package_dir={'firex_flame_ui': 'dist'},
      zip_safe=True,
      include_package_data=True,
      package_data={
        'firex_flame_ui': ['*.html', 'js/*.js', 'img/*', 'css/*.css'],
      },
      entry_points={},
      )

