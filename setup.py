# coding=utf-8
from setuptools import setup, find_packages

from factor_table import __version__, __author__

setup(
    name="factor_table",
    version=__version__,
    keywords=("factor", "table",'data','dataset'),
    description="data collection ",
    long_description="data collection ",


    url="http://www.github.com/sn0wfree",
    author=__author__,
    author_email="snowfreedom0815@gmail.com",

    packages=find_packages(),
    include_package_data=True,


)
