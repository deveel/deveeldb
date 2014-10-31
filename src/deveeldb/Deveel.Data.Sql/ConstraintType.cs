// 
//  Copyright 2010-2014 Deveel
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

using System;

namespace Deveel.Data.Sql {
	/// <summary>
	/// An enumeration of all the supported kinds
	/// of constraints within a table or a schema.
	/// </summary>
	public enum ConstraintType {
		///<summary>
		/// A <c>PRIMARY KEY</c> constraint.
		///</summary>
		/// <remarks>
		/// With this constraint, the 'column_list' list contains the names 
		/// of the columns in this table that are defined as the primary key.  
		/// There may only be one primary key constraint per table.
		/// </remarks>
		PrimaryKey = 1,

		///<summary>
		/// A UNIQUE constraint.
		///</summary>
		/// <remarks>
		/// With this constraint, the 'column_list' list contains the names of the 
		/// columns in this table that must be unique.
		/// </remarks>
		Unique = 2,

		///<summary>
		/// A FOREIGN_KEY constraint.
		///</summary>
		/// <remarks>
		/// With this constraint, the 'table_name' string contains the name of the 
		/// table that this is a foreign key for, the 'column_list' list contains 
		/// the list of foreign key columns, and 'column_list2' optionally contains 
		/// the referenced columns.
		/// </remarks>
		ForeignKey = 3,

		///<summary>
		/// A CHECK constraint.
		///</summary>
		/// <remarks>
		/// With this constraint, the 'expression' object contains the expression 
		/// that must evaluate to true when adding a column to the table.
		/// </remarks>
		Check = 4
	}
}