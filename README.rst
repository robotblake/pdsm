========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - docs
      - |
    * - tests
      - |
        |
    * - package
      - | |version| |wheel| |supported-versions| |supported-implementations|
        | |commits-since|

.. |version| image:: https://img.shields.io/pypi/v/pdsm.svg
    :alt: PyPI Package latest release
    :target: https://pypi.python.org/pypi/pdsm

.. |wheel| image:: https://img.shields.io/pypi/wheel/pdsm.svg
    :alt: PyPI Wheel
    :target: https://pypi.python.org/pypi/pdsm

.. |supported-versions| image:: https://img.shields.io/pypi/pyversions/pdsm.svg
    :alt: Supported versions
    :target: https://pypi.python.org/pypi/pdsm

.. |supported-implementations| image:: https://img.shields.io/pypi/implementation/pdsm.svg
    :alt: Supported implementations
    :target: https://pypi.python.org/pypi/pdsm

.. |commits-since| image:: https://img.shields.io/github/commits-since/robotblake/pdsm/v0.1.0.svg
    :alt: Commits since latest release
    :target: https://github.com/robotblake/pdsm/compare/v0.1.0...master

.. end-badges

A tool to manage parquet datasets and their definitions in Hive, Presto, and Athena

* Free software: MIT license

Installation
============

::

    pip install pdsm

Development
===========

To run the all tests run::

    py.test -vv tests
