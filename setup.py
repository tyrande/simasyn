from setuptools import setup, find_packages

setup(
    name = "simasyn",
    version = "0.1.0",
    author = "Alan Shi",
    author_email = "alan@sinosims.com",

    packages = find_packages(), 
    include_package_data = True,

    url = "http://www.sinosims.com",
    description = "Simhub Asynchronous server",
    
    install_requires = ["MySQL_python", 'redis', 'apns_client'],
    entry_points = {
        'console_scripts': [ 'simasyn = simasyn.run:main' ]
    },
)
