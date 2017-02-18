from setuptools import setup, find_packages

version = '0.1'

setup(
    name='ckanext-zipextractor',
    version=version,
    description='CKAN Extension - Zip Extractor',
    long_description='Extension for interfacing with the CKAN zip extractor microservice',
    classifiers=[],  # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    keywords='',
    author='Greg von Nessi',
    author_email='greg.vonnessi@linkdigital.com.au',
    url='',
    license='',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext', 'ckanext.zipextractor'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[],
    entry_points="""

        [ckan.plugins]
        zipextractor=ckanext.zipextractor.plugin:ZipExtractorPlugin

        [paste.paster_command]
        zipextractor = ckanext.zipextractor.cli:ZipExtractorCommand
    """,
)
