from setuptools import setup

setup(
    # Needed to silence warnings (and to be a worthwhile package)
    name='coveopush',
    url='https://github.com/coveo-labs/SDK-Push-Python',
    author='Wim Nijmeijer',
    author_email='wnijmeijer@coveo.com',
    packages=['coveopush'],
    version='0.2',
    description='CoveoPush client',
    install_requires=[
        'requests',
        'jsonpickle'
    ]
)
