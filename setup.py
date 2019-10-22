from setuptools import setup


setup(
    name='queue_consumer',
    version='0.1',
    description='Abstract Concurrent Queue Consumer',
    url='https://github.com/schipiga/queue-consumer/',
    author='Sergei Chipiga <chipiga86@gmail.com>',
    author_email='chipiga86@gmail.com',
    packages=['queue_consumer'],
    install_requires=['bounded-pool @ git+https://github.com/schipiga/bounded-pool@v0.3'],
)
