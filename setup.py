from setuptools import setup

setup(name='dataheap2_http',
      version='0.1',
      author='TU Dresden',
      python_requires=">=3.5",
      packages=['dataheap2_http'],
      scripts=[],
      entry_points='''
      [console_scripts]
      dataheap2-http=dataheap2_http:runserver_cmd
      ''',
      install_requires=['aio-pika', 'aiohttp', 'aiohttp-cors', 'click', 'click-completion', 'click_log', 'colorama', 'protobuf'])
