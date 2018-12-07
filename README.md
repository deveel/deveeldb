[![Build status][appveyor-image]][appveyor-url] [![Coverage Status][coveralls-image]][coveralls-url] [![Coverity Scan Build Status][coverity-image]][coverity-url] [![MyGet][myget-image]][myget-url] [![NuGet][nuget-image]][nuget-url]
 [![Join the chat!][gitter-image]][gitter-url] [![NuGet][nugetdt-image]][nugetdt-url] 

DeveelDB 3.0
==========

DeveelDB is a complete embeddable SQL-99 RDBMS for .NET/Mono frameworks. The system is designed around the standards ISO/ANSI

Although the core project is thought to be embedded in applications, the modular architecture allows extensions to other uses, such as providing databases through networks: an application is already included in the solution.


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
Unfortunately, as you can also see exploring the source code, the amount of work is quite important, and not always I can manage alone to make everything (architectural design, implementation, testing, commenting, etc.): I feel a bit lonely.

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
[nugetdt-image]:https://img.shields.io/nuget/dt/deveeldb.svg
[nugetdt-url]:https://www.nuget.org/packages/deveeldb
