[![Build status](https://ci.appveyor.com/api/projects/status/koo12o4q2ik8isej/branch/master?svg=true)](https://ci.appveyor.com/project/deveel/deveeldb/branch/master) [![Coverage Status](https://coveralls.io/repos/deveel/deveeldb/badge.svg?branch=master&service=github)](https://coveralls.io/github/deveel/deveeldb?branch=master) [![Coverity Scan Build Status](https://scan.coverity.com/projects/8341/badge.svg)](https://scan.coverity.com/projects/deveel-deveeldb) [![Join the chat! https://gitter.im/deveel/deveeldb](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/deveel/deveeldb?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) [![Slack Status](https://deveeldb-slackin.herokuapp.com/badge.svg)](https://deveeldb-slackin.herokuapp.com/)

DeveelDB 2.0
==========

**NOTE**: *This version of the project is still under development. It is in fact a total rewrite since the original version of the project, that has been discontinued. The poor level of coverage, the architectural model, the limits of the parser, and other requirements made it impossible to maintain it. You can still find it as a branch of the project.*

DeveelDB is a complete embeddable SQL-99 RDBMS for .NET/Mono frameworks. The system is designed around the standards ISO/ANSI and supports the following features:

- Transactions: `COMMIT`, `ROLLBACK` (Isolation Level *Serializable*)
- Data Definition Language (DDL): `CREATE/DROP SCHEMA`, `CREATE/DROP/ALTER TABLE`
- Data Manipulation Language (DML): `SELECT FROM`, `INSERT INTO`, `DELETE FROM`, `UPDATE`
- User Management: `CREATE/DROP/ALTER USER`, `CREATE/DROP ROLE`, `GRANT/REVOKE` statements
- Support for structured variables (eg. `DECLARE var INT(200) NOT NULL`)
- Procedures and functions
- Cursors
- ADO.NET native client
- Direct Access: programmatically execute SQL statements (without ADO.NET client and text commands)

Although the core project is thought to be embedded in applications, the modular architecture allows extensions to other uses, such as providing databases through networks: an application is already included in the solution.

Getting Started
=============

In the wiki pages of the GitHub space dedicated to the project, it is possible to find more information about the project dynamics and usage.

For a quick start, I suggest taking a look at [the getting started guide](https://github.com/deveel/deveeldb/wiki/Getting-Started-Embedded)


Getting It
============

The stage of the version 2.0 is not stable yet, so we are not providing any stable build on nuget.org. Anyway, the binaries of the project daily builds of *DeveelDB* can be obtained through the dedicated NuGet feed on MyGet (http://myget.org): the configuration of the sources depends on the environment used to build your project (referer to NuGet Documentation for further information: http://docs.nuget.org/consume/package-restore).

**NuGet v3**

https://www.myget.org/F/deveeldb/api/v3/index.json

**NuGet v2**

https://www.myget.org/F/deveeldb/api/v2


The feed contains these versions of the library:
- *deveeldb-anycpu*: this is the generic build, that is suited to be used by any CPU architeture
- *deveeldb-x86*: a version of the library built specificaly for the x86 (32-bit) architecture
- *deveeldb-x64*: the build specificaly oriented to the x64 (64-bit) architecture


License
============

*DeveelDB* is released under the [Apache 2.0][http://www.apache.org/licenses/LICENSE-2.0] license. This is a very permissive licensing, that allows anyone to use the core library into commercial and non-commercial project. Other libraries (such as he GIS extension) are released under different licensing, due to commercial reasons or to dependencies from external tools.


Status and Issues
============

You can verify the current status of the application code by  [checking the project](https://ci.appveyor.com/project/tsutomi/deveeldb-3f7ew) at [AppVeyor Continuous Integration](http://ci.appveyor.com) (access as "guest" user: you will find the direct link below the login form).

Please, report any issue or feature request to our [Issue Tracker](http://github.com/deveel/deveeldb/issues)

Contributing
============

The project was started as a proof of concept long time ago (in 2003!), to implement the first SQL engine for .NET: for all this time the project has been developed and managed almost like an hobby by me (Antonello Provenzano), going in parallel with my regular jobs and studies, never gained much attention by the industry, but also not very well managed.

The new version of the project aims to restart everything from scratch, making it right (code coverage, regressions, management, etc.), with the goal to finally deliver something great to .NET developers.
Unfortunately, as you can also see exploring the source code, the amound of work is quite important, and not always I can manage alone to make everything (architectural design, implementation, testing, commenting, etc.): I feel a bit lonely.

If you wish to contribute to the development of the code, but also to other areas of the project (eg. making a website, documenting the code, documenting the project, etc.), please get in touch with me, dropping an email to `antonello at deveel dot org` or joining the chat on [Gitter](https://gitter.im/deveel/deveeldb)!
