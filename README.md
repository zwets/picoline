# picoline 

_Simplistic workflow and job control for on-machine pipelines_

This Python package contains the generic workflow and job control mechanisms used
by the [KCRI CGE Bacterial Analysis Pipeline](https://github.com/zwets/kcri-cge-bap)
and [KCRI Assembly and Quality Analysis Pipeline](https://github.com/zwets/kcri-qap).


## Background

This package does not have the ambition to be generically useful (although
it may).  It was written as the "simplest thing that could possibly work" to
offer:

 * a mechanism to declaratively define a workflow of service invocations
   (including alternative flows, conditional invocation, etc.) for ...
 * an executor that performs workflow logic by invoking service shims that
   dispatch jobs to ...
 * a job scheduler that asynchronously spawns system processes that execute
   the backend (while keeping track of resource constraints), and ...
 * a uniform but loosely coupled mechanism to exchange information between
   the services (the "blackboard")

And all of this in pure Python, that is, embeddable in a Python program,
without further requirements or system dependencies.

The rationale for not building this functionality on top of (the obvious)
workflow engine a/o job control system is that the BAP and QAP are intended
to run on laptops "in the field", rather than on an HPC.

See the [BAP](https://github.com/zwets/kcri-cge-bap) documentation for more
background.


## Documentation

Documentation is in the header comments of the modules:

 * `pico.workflow.logic`: how to define services and their dependencies
 * `pico.workflow.executor`: how to operate the workflow executor
 * `pico.workflow.blackboard`: how to exchange data between services
 * `pico.jobcontrol.job`: how to specify jobs and their requirements
 * `pico.jobcontrol.subproc`: how the default job scheduler works (note it
   has a `main()` function and can be used from the command-line)

Helpful documentation of a complete use case can be found in the code of the
KCRI CGE BAP at <https://github.com/zwets/kcri-cge-bap>.  The `workflow.py`
module in that repository has a CLI tester for workflow definitions.


## Installation

The package can be installed in the usual Pythonic way:

    python3 setup.py install

There is a single dependency (`psutil`) beside the Python3 standard library.


#### Licence

Copyright (c) 2020 Marco van Zwetselaar <io@zwets.it>, KCRI, Tanzania.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

