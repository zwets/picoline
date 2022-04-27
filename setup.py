from setuptools import find_packages, setup

NAME = 'picoline'
VERSION = '1.4.1'
DESCRIPTION = 'Simplistic workflow and job control for on-machine pipelines'
LONG_DESCRIPTION = DESCRIPTION
URL = 'https://github.com/zwets/picoline'
EMAIL = 'io@zwets.it'
AUTHOR = 'Marco van Zwetselaar'
REQUIRES_PYTHON = '>=3.8.0'
REQUIRED = [ 'psutil' ]
EXTRAS = { }

about = {'__version__': VERSION}

setup(
    name = NAME,
    version = VERSION,
    description = DESCRIPTION,
    long_description = LONG_DESCRIPTION,
    author = AUTHOR,
    author_email = EMAIL,
    python_requires = REQUIRES_PYTHON,
    url = URL,
    packages = find_packages(exclude=['tests']),
    install_requires = REQUIRED,
    extras_require = EXTRAS,
    include_package_data = True,
    license = 'Apache License, Version 2.0',
    classifiers = ['License :: OSI Approved :: Apache Software License'],
    zip_safe = False
    )
