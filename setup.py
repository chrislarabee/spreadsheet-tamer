from setuptools import setup

setup(
    name='Data-Genius',
    url='',
    author='Chris Larabee',
    author_email='chris.larabee9@gmail.com',
    packages=['datagenius'],
    install_requires=['pandas', 'xlrd', 'pytest'],
    version=0.1,
    license='GNU',
    description=('A suite of classes and functions that attempt to '
                 'generalize and automate the basic steps of exploring '
                 'and cleaning data in any format and form'),
    long_description=open('README.md').read()
)
