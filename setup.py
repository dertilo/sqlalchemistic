from setuptools import setup, find_packages

with open('requirements.txt') as f:
    reqs = f.read()

setup(
    name='sqlutil',
    version='0.1',
    packages=['sqlutil'],
    license='LICENSE.txt',
    long_description=open('README.md').read(),
    install_requires=reqs.strip().split('\n'),
)