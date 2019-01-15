from setuptools import setup, find_packages
import os

# Determine the build number
build_file = os.path.join(os.path.dirname(__file__), "BUILD")
if os.path.exists(build_file):
    with open(build_file) as f:
        version_num = f.read()
else:
    version_num = "dev"

setup(name='firexapp',
      version='0.1.' + version_num,
      description='Core firex application libraries',
      url='https://github.com/FireXStuff/firexapp',
      author='Core FireX Team',
      author_email='firex-dev@gmail.com',
      license='BSD-3-Clause',
      packages=find_packages(),
      zip_safe=True,
      install_requires=[
          "distlib",
          "firexkit"
      ],
      classifiers=[
          "Programming Language :: Python :: 3",
          "Operating System :: OS Independent",
          "License :: OSI Approved :: BSD License",
      ],
      entry_points={
          'console_scripts': ['firexapp = firexapp.application:main', ]
      },
      )
