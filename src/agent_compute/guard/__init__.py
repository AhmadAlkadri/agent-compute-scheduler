"""The ACS-internal resource guard (backend ``acs-guard``).

The code is in ``engine.py`` (its docstring describes the guard and has the
one history note).  It is a separate process: run it as
``python3 -m agent_compute.guard ...`` or ``python3 <path>/engine.py ...``;
the ACS runner runs it for a claimed job.  No admission module imports
this package, and admission never opens the guard home's lock.  Importing
the package imports nothing else (the engine loads only when run or
imported explicitly).
"""
