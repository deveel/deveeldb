// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;

namespace Deveel.Data.Sql.Statements {
	/// <summary>
	/// An enumeration of supported <see cref="SqlStatement"/>
	/// objects by the system.
	/// </summary>
	/// <seealso cref="SqlStatement"/>
	public enum StatementType {
		// Schema
		/// <summary>
		/// Creates a new <c>SCHEMA</c> object with the specified
		/// configurations,
		/// </summary>
		CreateSchema,

		/// <summary>
		/// Drops an existing <c>SCHEMA</c> from the database.
		/// </summary>
		DropSchema,

		// Tables
		/// <summary>
		/// Creates a database <c>TABLE</c> into an existing schema,
		/// with the specifications given.
		/// </summary>
		CreateTable,

		/// <summary>
		/// Alters the information and dynamics of an existing
		/// database <c>TABLE</c>.
		/// </summary>
		AlterTable,

		/// <summary>
		/// Drops an existing <c>TABLE</c> from a database.
		/// </summary>
		DropTable,

		/// <summary>
		/// Creates a <c>VIEW</c> object, that is the result
		/// of specified query from one or more database tables.
		/// </summary>
		CreateView,

		/// <summary>
		/// Drops an existing <c>VIEW</c> object from the database.
		/// </summary>
		DropView,

		// Security
		/// <summary>
		/// Creates a security group for users having a set of
		/// privileges that will be granted to members of the group.
		/// </summary>
		CreateGroup,

		/// <summary>
		/// Deletes an existing security group from the database.
		/// </summary>
		DropGroup,

		/// <summary>
		/// Creates a single <c>USER</c> identified by a given unique name
		/// and authenticated by a given mechanism. 
		/// </summary>
		CreateUser,

		/// <summary>
		/// Removes an existing user, identified by a unique name, from the
		/// database or a security group.
		/// </summary>
		DropUser,

		/// <summary>
		/// Grants a given set of privileges to an existing user.
		/// </summary>
		Grant,

		/// <summary>
		/// Revokes a set of privileges from an existing user.
		/// </summary>
		Revoke,

		// CRUD
		/// <summary>
		/// Executes a query from one or more data sources,
		/// optionally sorting results.
		/// </summary>
		Select,

		/// <summary>
		/// Inserts data coming from a query into a table or
		/// a variable specified.
		/// </summary>
		SelectInto,

		/// <summary>
		/// Inserts a set of data into a database table.
		/// </summary>
		Insert,

		/// <summary>
		/// Updates data contained into a row of a table identified by the
		/// given search expression.
		/// </summary>
		Update,

		/// <summary>
		/// Deletes a given set of data from a table
		/// </summary>
		Delete
	}
}
