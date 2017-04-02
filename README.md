[![Build status][appveyor-image]][appveyor-url] [![Coverage Status][coveralls-image]][coveralls-url] [![Coverity Scan Build Status][coverity-image]][coverity-url] [![MyGet][myget-image]][myget-url] [![NuGet][nuget-image]][nuget-url]
 [![Join the chat!][gitter-image]][gitter-url] [![Slack Status][slack-image]][slack-url]

_**NOTE**: The development of the library has been moved to the repository [https://github.com/deveel/deveeldb.core](https://github.com/deveel/deveeldb.core) targetting the .NET Standard frameworks: the project has been restarted from scratch and the idea is that in the future months this repository and the other will be merged_

DeveelDB 2.0
==========

DeveelDB is a complete embeddable SQL-99 RDBMS for .NET/Mono frameworks. The system is designed around the standards ISO/ANSI and supports the following features:

- Transactions: `COMMIT`, `ROLLBACK` (Isolation Level *Serializable*)
- Data Definition Language (DDL): `CREATE/DROP SCHEMA`, `CREATE/DROP/ALTER TABLE`
- Data Manipulation Language (DML): `SELECT FROM`, `SELECT INTO`, `INSERT INTO`, `DELETE FROM`, `UPDATE`
- User Management: `CREATE/DROP/ALTER USER`, `CREATE/DROP ROLE`, `GRANT/REVOKE` statements
- Support for structured variables (eg. `DECLARE var INT(200) NOT NULL`)
- Procedures and functions
- User-Defined Types and Sequences
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

NuGet Packages
=============
It is possible to restore nightly build versions of the library through the stage NuGet source of *DeveelDB* at MyGet.org (http://myget.org): the configuration of the sources depends on the environment used to build your project (referer to NuGet Documentation for further information: http://docs.nuget.org/consume/package-restore).

**NuGet v3**

https://www.myget.org/F/deveeldb/api/v3/index.json

**NuGet v2**

https://www.myget.org/F/deveeldb/api/v2


The feed contains these versions of the library:
- *deveeldb*: this is the generic build, that is suited to be used by any CPU architeture
- *deveeldb.x86*: a version of the library built specificaly for the x86 (32-bit) architecture
- *deveeldb.x64*: the build specificaly oriented to the x64 (64-bit) architecture

Alpha versions of *DeveelDB* can also be found at the official NuGet saource (http://nuget.org): only the more stable builds are promoted there.

- *deveeldb*: https://www.nuget.org/packages/deveeldb/
- *deveeldb.x86*: https://www.nuget.org/packages/deveeldb.x86/
- *deveeldb.x64*: https://www.nuget.org/packages/deveeldb.x64/

Since the versions of DeveelDB 2.0 deployed on NuGet.org are still in pre-release, you must specify the  *-Pre* suffix

```
PM> Install-Package deveeldb -Pre
```


Binary Packages
===============

We also produce a set of binary packages that we release on GitHub: you can download the latest one for your platform checking [the latest release](https://github.com/deveel/deveeldb/releases/latest)


License
============

*DeveelDB* is released under the [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0) license. This is a very permissive licensing, that allows anyone to use the core library into commercial and non-commercial project. Other libraries (such as he GIS extension) are released under different licensing, due to commercial reasons or to dependencies from external tools.


Status and Issues
============

You can verify the current status of the application code by  [checking the project](https://ci.appveyor.com/project/tsutomi/deveeldb-3f7ew) at [AppVeyor Continuous Integration](http://ci.appveyor.com) (access as "guest" user: you will find the direct link below the login form).

Please, report any issue or feature request to our [Issue Tracker](http://github.com/deveel/deveeldb/issues)

Contributing
============

The project was started as a proof of concept long time ago (in 2003!), to implement the first SQL engine for .NET: for all this time the project has been developed and managed almost like an hobby by me (Antonello Provenzano), going in parallel with my regular jobs and studies, never gained much attention by the industry, but also not very well managed.

The new version of the project aims to restart everything from scratch, making it right (code coverage, regressions, management, etc.), with the goal to finally deliver something great to .NET developers.
Unfortunately, as you can also see exploring the source code, the amound of work is quite important, and not always I can manage alone to make everything (architectural design, implementation, testing, commenting, etc.): I feel a bit lonely.

The following kind of help is very welcome:

* **Development**: implemention of new features, according to the scheduled planning of the project
* **Testing**: implementation of tests to assess given features of the projects are behaving as expected; since DeveelDB is not a project that is following the TDD (*Test-Driven Development*) model, unit tests are crucial to assess the coverage of the code and the stability of the system
* **Beta Testing**: use the final product in given contexts, to provide example cases of its usage and potentially discover bugs not covered by other tests
* **Documentation**: document the code and the project in a way that other users or developers can be helped using DeveelDB or collaborating

If you wish to contribute, please feel free to get in touch with me, dropping an email to `antonello at deveel dot org` or joining the chat on [Gitter](https://gitter.im/deveel/deveeldb)!

[appveyor-image]:https://ci.appveyor.com/api/projects/status/koo12o4q2ik8isej?svg=true
[appveyor-url]:https://ci.appveyor.com/project/deveel/deveeldb
[coveralls-url]:https://coveralls.io/r/deveel/deveeldb
[coveralls-image]:https://coveralls.io/repos/deveel/deveeldb/badge.png
[coverity-image]:https://scan.coverity.com/projects/8341/badge.svg
[coverity-url]:https://scan.coverity.com/projects/deveel-deveeldb
[gitter-image]:https://badges.gitter.im/Join%20Chat.svg
[gitter-url]:https://gitter.im/deveel/deveeldb
[slack-image]:https://deveeldb-slackin.herokuapp.com/badge.svg
[slack-url]:https://deveeldb-slackin.herokuapp.com/
[myget-image]:https://img.shields.io/myget/deveeldb/v/deveeldb.svg?label=MyGet
[myget-url]:https://www.myget.org/feed/deveeldb/package/nuget/deveeldb
[nuget-image]:https://img.shields.io/nuget/vpre/deveeldb.svg?label=NuGet
[nuget-url]:https://www.nuget.org/packages/deveeldb
