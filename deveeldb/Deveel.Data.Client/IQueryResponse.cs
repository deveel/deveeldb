//  
//  IQueryResponse.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
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

namespace Deveel.Data.Client {
	///<summary>
	/// The response to a query executed via the <see cref="IDatabaseInterface.ExecuteQuery"/>
	/// method in the <see cref="IDatabaseInterface"/> interface.
	///</summary>
	/// <remarks>
	/// This contains general information about the result of the query.
	/// </remarks>
	public interface IQueryResponse {
	    ///<summary>
	    /// Returns a number that identifies this query within the set of queries
	    /// executed on the connection.
	    ///</summary>
	    /// <remarks>
	    /// This is used for identifying this query in subsequent operations.
	    /// </remarks>
	    int ResultId { get; }

	    ///<summary>
        /// The time, in milliseconds, that the query took to execute.
	    ///</summary>
	    int QueryTimeMillis { get; }

	    ///<summary>
        /// The total number of rows in the query result.
	    ///</summary>
	    /// <remarks>
	    /// This is known ahead of time, even if no data in the query has been accessed.
	    /// </remarks>
	    int RowCount { get; }

        /// <summary>
        /// The number of columns in the query result.
        /// </summary>
	    int ColumnCount { get; }

		///<summary>
        /// Gets the <see cref="ColumnDescription"/> object that describes column 
        /// at the zero-based index in the result.
		///</summary>
		///<param name="column"></param>
		///<returns></returns>
		ColumnDescription GetColumnDescription(int column);

	    ///<summary>
	    /// Returns any warnings about the query or <b>null</b> if there were no 
	    /// warnings.
	    ///</summary>
	    string Warnings { get; }
	}
}