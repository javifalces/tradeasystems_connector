from setuptools import setup

setup(
    name='tradeasystems_connector',
    version='1.0.0',
    packages=['', 'dao', 'conf', 'data', 'util', 'model', 'tests', 'tests.dao', 'tests.broker', 'tests.service',
              'tests.historical_market_data', 'broker', 'service', 'historical_market_data'],
    url='',
    license='',
    author='javier_falces',
    author_email='javifalces@gmail.com',
    description='Layer  to download/trade with different sources brokers , persistence in PyStore'
)
