from setuptools import setup

setup(
    name='tradeasystems_connector',
    version='1.0.0',
    packages=['', 'tradeasystems_connector', 'tradeasystems_connector.conf',
              'tradeasystems_connector.util', 'tradeasystems_connector.model', 'tradeasystems_connector.service'],
    url='',
    license='OPEN',
    author='javier_falces',
    author_email='javifalces@gmail.com',
    description='Layer  to download/trade with different sources brokers , persistence in PyStore'
)
