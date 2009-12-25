//  
//  ConstraintType.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;

namespace Deveel.Data {
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