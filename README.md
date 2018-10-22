![dqo](docs/logo.png)

[![Documentation Status](https://readthedocs.org/projects/dqo/badge/?version=latest)](https://dqo.readthedocs.io/en/latest/?badge=latest)

Data Query Objects
==================

A micro-ORM supporting sync and async Python code in one library.


- [Documentation](https://dqo.readthedocs.io)
- [Distribution](https://pypi.org/project/dqo/)


Introduction
------------

There are many regular ORMs in Python, and a few async ORMs, but none that do both.  I needed one, so I built (and unoriginally named) `dqo`.

Why an ORM that handles both?  Why not write all my code in async world?  Because async is [cooperative multitasking](https://en.wikipedia.org/wiki/Cooperative_multitasking), 
and if you coded through the 90s like I did you know cooperative multitasking is the devil.  Yes, it's performant, but *only* if you code 
everything correctly and never hog the main thread.  We abandoned it as a programming paradigm because "code everything correctly" is hard to scale.

So code your core high performance server code as async and leave all your other code as plain old Python.  Without bifurcating your entire codebase.



