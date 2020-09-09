from setuptools import setup

setup(
    name='Data-Genius',
    url='https://gitlab.com/mnhs/bipi/datagenius',
    author='Chris Larabee',
    author_email='chris.larabee9@gmail.com',
    packages=['datagenius', 'datagenius.io', 'datagenius.lib'],
    install_requires=[
        'pandas>=1.0.4', 'xlrd', 'SQLalchemy', 'recordlinkage',
        'google-api-python-client', 'google-auth-httplib2',
        'google-auth-oauthlib', 'oauth2client', 'numpy'
    ],
    version='0.4.7',
    license='GNU',
    description=('A suite of classes and functions that attempt to '
                 'generalize and automate the basic steps of exploring '
                 'and cleaning data in any format and form.'),
    long_description=open('README.md').read()
)
