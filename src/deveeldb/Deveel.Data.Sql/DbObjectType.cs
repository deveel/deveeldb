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

using Deveel.Data;

namespace Deveel.Data.Sql {
	/// <summary>
	/// The kind of objects that can be handled by
	/// a database system and its managers
	/// </summary>
	/// <seealso cref="IObjectManager"/>
	public enum DbObjectType {
		/// <summary>
		/// A <c>TABLE</c> object in a database.
		/// </summary>
		Table = 1,

		/// <summary>
		/// A <c>VIEW</c> object obtained by a source query.
		/// </summary>
		View = 2,

		/// <summary>
		/// A user-defined <c>TYPE</c> that holds complex objects
		/// in a database column.
		/// </summary>
		Type = 4,

		/// <summary>
		/// A single variable within a query context or in the system
		/// global context.
		/// </summary>
		Variable = 8,

		/// <summary>
		/// A single <c>ROW</c> in a database table, that holds tabular
		/// data as configured by the table specifications.
		/// </summary>
		Row = 10,

		/// <summary>
		/// The single <c>COLUMN</c> of a table in a database, handling
		/// the form of data that can be stored in a cell.
		/// </summary>
		Column = 11,

		/// <summary>
		/// A <c>TRIGGER</c> fired at provided write events (<c>INSERT</c>, <c>UPDATE</c> or
		/// <c>DELETE</c>) over a table at a given moments (<c>BEFORE</c> or <c>AFTER</c>).
		/// </summary>
		Trigger = 17,

		/// <summary>
		/// A <c>SEQUENCE</c> of numeric values that can be <c>native</c> or
		/// user-defined with given configuration.
		/// </summary>
		Sequence = 18,

		/// <summary>
		/// A routine (<c>PROCEDURE</c> or <c>FUNCTION</c>) defined in a database, that is
		/// a program with defined input parameters and optional output values or
		/// return value (in <c>FUNCTION</c>).
		/// </summary>
		Routine = 20,

		/// <summary>
		/// A cursor is a named, precomputed, query, that accepts optional parameters and
		/// handles a state of the iteration over the query.
		/// </summary>
		Cursor = 25,

		/// <summary>
		/// A <c>SCHEMA</c> object, that is a named container of multiple types
		/// of objects (eg. <c>TABLE</c>, <c>PROCEDURE</c>, <c>VIEW</c>, etc.).
		/// </summary>
		Schema = 51
	}
}