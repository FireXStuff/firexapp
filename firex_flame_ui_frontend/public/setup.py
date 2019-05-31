from setuptools import setup
# import versioneer

setup(name='firex_flame_ui',
      # version=versioneer.get_version(),
      version='0.5',
      # cmdclass=versioneer.get_cmdclass(),
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
            'firex_flame_ui': ['*.html', 'js/*.js', 'img/*', 'css/*.css', 'COMMITHASH'],
      },
      entry_points={},
      )

