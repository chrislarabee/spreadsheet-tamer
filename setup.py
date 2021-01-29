from setuptools import setup

setup(
    name="Data-Genius",
    url="https://gitlab.com/mnhs/bipi/datagenius",
    author="Chris Larabee",
    author_email="chris.larabee9@gmail.com",
    packages=["datagenius", "datagenius.io", "datagenius.lib", "datagenius.names"],
    install_requires=[
        "pandas>=1.0.4",
        "xlrd==1.2.0",
        "SQLalchemy==1.3.22",
        "recordlinkage==0.14",
        "google-api-python-client==1.12.8",
        "google-auth-httplib2==0.0.4",
        "google-auth-oauthlib==0.4.2",
        "oauth2client==4.1.3",
        "numpy==1.19.5",
        "PyYAML>=5.4.1",
    ],
    version="0.6.1",
    license="GNU",
    description=(
        "A suite of classes and functions that attempt to "
        "generalize and automate the basic steps of exploring "
        "and cleaning data in any format and form."
    ),
    long_description=open("README.md").read(),
)
