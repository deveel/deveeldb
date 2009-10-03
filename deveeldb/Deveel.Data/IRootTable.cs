// 
//  IRootTable.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;

namespace Deveel.Data {
	/// <summary>
	/// Interface that is implemented by all root tables.
	/// </summary>
	/// <remarks>
	/// A root table is a non-virtual table that represents table data 
	/// in its lowest form.  When the <see cref="Table.ResolveToRawTable"/>
	/// method is called, if it encounters a table that implements 
	/// <see cref="IRootTable"/> then it does not attempt to decend further 
	/// to extract the underlying tables.
	/// <para>
	/// This interface is used for unions.
	/// </para>
	/// </remarks>
	public interface IRootTable {
		/// <summary>
		/// This is function is used to check that two root tables are identical.
		/// </summary>
		/// <param name="table"></param>
		/// <remarks>
		/// This is used if we need to chect that the form of the table is the same.
		/// Such as in a union operation, when we can only union two tables with
		/// the identical columns.
		/// </remarks>
		/// <returns></returns>
		bool TypeEquals(IRootTable table);
	}
}