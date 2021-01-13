# Need fastentrypoints to monkey patch setuptools for faster console_scripts
# noinspection PyUnresolvedReferences
import fastentrypoints
from setuptools import setup, find_packages
import versioneer


setup(name='firexapp',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      description='Core firex application libraries',
      url='https://github.com/FireXStuff/firexapp',
      author='Core FireX Team',
      author_email='firex-dev@gmail.com',
      license='BSD-3-Clause',
      packages=find_packages(),
      package_data={
            'firexapp': ['testing/cloud-ci-install-configs.json']
      },
      zip_safe=True,
      install_requires=[
          "distlib",
          "firexkit",
          "tqdm<=4.29.1",
          "xmlrunner",
          "redis==3.5.0",
          "celery[redis]==4.3.0",
          "kombu==4.6.8",           # app.inspect.active seems to be returning None all the time for Kombu==4.6.4
          "psutil",
          "python-Levenshtein",
          "entrypoints",
          "colorlog==2.10.0",
          "beautifulsoup4",
          "vine==1.3.0",  # 5.0.0 breaks /celery/__init__.py module load.
      ],
      extras_require={
          'test': [
              'firex-keeper',
          ],
          'flame': [
              'firex-flame'
          ]
      },
      classifiers=[
          "Programming Language :: Python :: 3",
          "Operating System :: OS Independent",
          "License :: OSI Approved :: BSD License",
      ],
      entry_points={
          'console_scripts': ['firexapp = firexapp.application:main',
                              'flow_tests = firexapp.testing.test_infra:default_main',
                              'firex_shutdown = firexapp.submit.shutdown:main',
                              ],
      },
      )
