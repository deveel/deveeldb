//  
//  ColumnConstraints.cs
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
	/// The types of constraints that can be applied to a
	/// column at definition.
	/// </summary>
	[Flags]
	public enum ColumnConstraints {
		/// <summary>
		/// None constraint for the column was set.
		/// </summary>
		None = 0x00,

		/// <summary>
		/// The column belongs to a <c>PRIMARY KEY</c> constraint
		/// within the containing table.
		/// </summary>
		PrimaryKey = 0x01,

		/// <summary>
		/// Indiciates that the value of a column must be <c>UNIQUE</c>
		/// for each row.
		/// </summary>
		Unique = 0x02,

		/// <summary>
		/// Constraints a column to contain only values that are
		/// <c>NOT NULL</c>.
		/// </summary>
		NotNull = 0x04,

		/// <summary>
		/// All the column constraints possible.
		/// </summary>
		All = PrimaryKey | Unique | NotNull
	}
}