========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - docs
      - |docs|
    * - tests
      - |github-actions| |codecov|
    * - package
      - |version| |wheel| |supported-versions| |supported-implementations| |commits-since|
.. |docs| image:: https://readthedocs.org/projects/keepice-lakehouse-library/badge/?style=flat
    :target: https://readthedocs.org/projects/keepice-lakehouse-library/
    :alt: Documentation Status

.. |github-actions| image:: https://github.com/migueldlfuentem/keepice-lakehouse-library/actions/workflows/github-actions.yml/badge.svg
    :alt: GitHub Actions Build Status
    :target: https://github.com/migueldlfuentem/keepice-lakehouse-library/actions

.. |codecov| image:: https://codecov.io/gh/migueldlfuentem/keepice-lakehouse-library/branch/main/graphs/badge.svg?branch=main
    :alt: Coverage Status
    :target: https://app.codecov.io/github/migueldlfuentem/keepice-lakehouse-library

.. |version| image:: https://img.shields.io/pypi/v/keepice-lakehouse-library.svg
    :alt: PyPI Package latest release
    :target: https://pypi.org/project/keepice-lakehouse-library

.. |wheel| image:: https://img.shields.io/pypi/wheel/keepice-lakehouse-library.svg
    :alt: PyPI Wheel
    :target: https://pypi.org/project/keepice-lakehouse-library

.. |supported-versions| image:: https://img.shields.io/pypi/pyversions/keepice-lakehouse-library.svg
    :alt: Supported versions
    :target: https://pypi.org/project/keepice-lakehouse-library

.. |supported-implementations| image:: https://img.shields.io/pypi/implementation/keepice-lakehouse-library.svg
    :alt: Supported implementations
    :target: https://pypi.org/project/keepice-lakehouse-library

.. |commits-since| image:: https://img.shields.io/github/commits-since/migueldlfuentem/keepice-lakehouse-library/v0.0.0.svg
    :alt: Commits since latest release
    :target: https://github.com/migueldlfuentem/keepice-lakehouse-library/compare/v0.0.0...main



.. end-badges

A library designed to facilitate interaction with Apache Iceberg, abstracting its complexity for end users.

* Free software: Apache Software License 2.0

Installation
============

::

    pip install keepice-lakehouse-library

You can also install the in-development version with::

    pip install https://github.com/migueldlfuentem/keepice-lakehouse-library/archive/main.zip


Documentation
=============


https://keepice-lakehouse-library.readthedocs.io/


Development
===========

To run all the tests run::

    tox

Note, to combine the coverage data from all the tox environments run:

.. list-table::
    :widths: 10 90
    :stub-columns: 1

    - - Windows
      - ::

            set PYTEST_ADDOPTS=--cov-append
            tox

    - - Other
      - ::

            PYTEST_ADDOPTS=--cov-append tox
