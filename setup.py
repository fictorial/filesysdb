from setuptools import setup, find_packages

with open('README.rst') as f:
  readme = f.read()

with open('LICENSE') as f:
  license = f.read()

setup(
  name='filesysdb',
  version='0.2.0',
  description='A filesystem-backed database.',
  long_description=readme,
  author='Brian Hammond',
  author_email='brian@fictorial.com',
  url='https://github.com/fictorial/filesysdb',
  license=license,
  packages=find_packages(exclude=('tests', 'docs'))
)
