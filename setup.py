from setuptools import setup


setup(
    name='queue_consumer',
    version='0.1',
    description='Bounded processes & threads pool executor',
    url='https://github.com/schipiga/queue-consumer/',
    author='Sergei Chipiga <chipiga86@gmail.com>',
    author_email='chipiga86@gmail.com',
    packages=['queue_consumer'],
    install_requires=['bounded-pool'],
    dependency_links=['https://github.com/schipiga/bounded-pool/tarball/master#egg=bounded-pool-0.1'],
)
