from setuptools import setup, find_packages  # noqa: H301

# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools
NAME = "sqlgraph"
VERSION = "0.0.2"
PYTHON_REQUIRES = ">=3.7"
REQUIRES = [
    "sqlglot",
    "networkx",
]

setup(
    name=NAME,
    version=VERSION,
    description="SQL Graph",
    author="Brad Arndt",
    author_email="brad.arndt@gmail.com",
    url="",
    keywords=["SQL Graph"],
    install_requires=REQUIRES,
    packages=find_packages(exclude=["test", "tests"]),
    include_package_data=True,
    long_description_content_type='text/markdown',
    long_description="""\
    """,
    package_data={},
)
