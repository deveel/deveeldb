//  
//  AlterTableActionType.cs
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

namespace Deveel.Data.Sql {
	/// <summary>
	/// The possible types of actions in a <see cref="AlterTableAction"/>
	/// expression.
	/// </summary>
	public enum AlterTableActionType {
		/// <summary>
		/// Adds a defined column to a table.
		/// </summary>
		AddColumn = 1,

		/// <summary>
		/// Modifies a table by removing a given column.
		/// </summary>
		DropColumn = 2,

		/// <summary>
		/// Adds a new constraint to the table.
		/// </summary>
		AddConstraint = 3,

		/// <summary>
		/// Drops a named constraint from a table.
		/// </summary>
		DropConstraint = 4,

		/// <summary>
		/// Drops a <c>PRIMARY KEY</c> constraint from a table.
		/// </summary>
		DropPrimaryKey = 5,

		/// <summary>
		/// Alters a table column setting the <c>DEFAULT</c> expression.
		/// </summary>
		SetDefault = 6,

		/// <summary>
		/// Drops the <c>DEFAULT</c> expression from a given column.
		/// </summary>
		DropDefault = 7,
	}
}
