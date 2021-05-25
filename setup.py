from setuptools import setup

setup(
    name="metricq_grafana",
    version="0.1",
    author="TU Dresden",
    python_requires=">=3.5",
    packages=["metricq_grafana"],
    scripts=[],
    entry_points="""
      [console_scripts]
      metricq-grafana=metricq_grafana:runserver_cmd
      """,
    install_requires=[
        "aio-pika",
        "aiohttp",
        "aiohttp-cors",
        "click",
        "click-completion",
        "click_log",
        "colorama",
        "metricq ~= 3.0",
        "aiocache",
    ],
    extras_require={"journallogger": ["systemd"]},
    setup_requires=["setuptools_scm"],
    use_scm_version=True,
)
