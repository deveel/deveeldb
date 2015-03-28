[![Build status](https://ci.appveyor.com/api/projects/status/x0v8j8gbt2tb7hj4?svg=true)](https://ci.appveyor.com/project/tsutomi/deveeldb-3f7ew)
[![Stories in Ready](https://badge.waffle.io/deveel/deveeldb.png?label=ready&title=Ready)](https://waffle.io/deveel/deveeldb)
[![Stories in Progress](https://badge.waffle.io/deveel/deveeldb.png?label=in+progress&title=In+Progress)](https://waffle.io/deveel/deveeldb)

DeveelDB
==========

DeveelDB is a complete embeddable SQL-99 RDBMS for .NET/Mono frameworks. The system is designed around the standards ISO/ANSI and supports the following features:

- ACID Transactions: `BEGIN`, `COMMIT`, `ROLLBACK` (Isolation Level *Serializable*)
- Data Definition Language (DDL): `CREATE/DROP SCHEMA`, `CREATE/DROP/ALTER TABLE`
- Data Manipulation Language (DML): `SELECT FROM`, `INSERT INTO`, `DELETE FROM`, `UPDATE`
- User Management: `GRANT/REVOKE` statements
- Support for structured variables (eg. `DECLARE var INT(200) NOT NULL`)
- Procedures and functions: with the (current) limitation of the body defined into .NET classes
- Cursors
- ADO.NET native client
- Direct Access: programmatically execute SQL statements (without ADO.NET client and text commands)

Although the core project is thought to be embedded in applications, the modular architecture allows extensions to other uses, such as providing databases through networks: an application is already included in the solution.

Where to Download
============

The kernel library (_deveeldb.dll_)  is present on [NuGet](http://nuget.org), and can be installed by executing the following command on the Package Manager Console

```
PM> Install-Package deveeldb

OR

PM> Install-Package deveeldb.x86
```

Another version optimized for x64 architecture can be installed using the command

```
PM> Install-Package deveeldb.x64
```

Alternatively, use the NuGet package explorer and search for _deveeldb_ (or _deveeldb.x64_ for x64 optimized version) and install it..

The kernel library has no external dependencies and requires .NET v3.5 (or higher)


How to Use it
============

Please reference to [the wiki pages on GitHub](https://github.com/deveel/deveeldb/wiki) to better understand how to use DeveelDB in your applications


Status and Issues
============

You can verify the current status of the application code by  [checking the project](http://ci.deveel.org/project.html?projectId=DeveelDB&tab=projectOverview) at [Deveel Continuous Integration Server](http://ci.deveel.org) (access as "guest" user: you will find the direct link below the login form).

Please, report any issue or feature request to our [Issue Tracker](http://github.com/deveel/deveeldb/issues)
