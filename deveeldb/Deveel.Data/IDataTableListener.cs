// 
//  IDataTableListener.cs
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
	/// A <see cref="IDataTableListener"/> is notified of all 
	/// modifications to the raw entries of the data table.
	/// </summary>
	/// <remarks>
	/// This listener can be used for detecting changes in VIEWs, 
	/// for triggers or for caching of common queries.
	/// </remarks>
	interface IDataTableListener {
		/// <summary>
		/// Called before a row entry in the table is deleted.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="row_index"></param>
		void OnRowDeleted(DataTable table, int row_index);

		/// <summary>
		/// Called after a row entry in the table is added.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="row_index"></param>
		void OnRowAdded(DataTable table, int row_index);
	}
}