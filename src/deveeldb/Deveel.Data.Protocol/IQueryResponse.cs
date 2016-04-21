// 
//  Copyright 2010-2016 Deveel
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

namespace Deveel.Data.Protocol {
	///<summary>
	/// The response to a command executed to a remote database that
	/// holds the result of the execution for transferring to the client side.
	///</summary>
	/// <remarks>
	/// This contains general information about the result of the command.
	/// </remarks>
	public interface IQueryResponse {
	    ///<summary>
	    /// Returns a number that identifies this command within the set of queries
	    /// executed on the connection.
	    ///</summary>
	    /// <remarks>
	    /// This is used for identifying this command in subsequent operations.
	    /// </remarks>
	    int ResultId { get; }

	    ///<summary>
        /// The time, in milliseconds, that the command took to execute.
	    ///</summary>
	    int QueryTimeMillis { get; }

	    ///<summary>
        /// The total number of rows in the command result.
	    ///</summary>
	    /// <remarks>
	    /// This is known ahead of time, even if no data in the command has been accessed.
	    /// </remarks>
	    int RowCount { get; }

        /// <summary>
        /// The number of columns in the command result.
        /// </summary>
	    int ColumnCount { get; }

		///<summary>
        /// Gets the <see cref="QueryResultColumn"/> object that describes column 
        /// at the zero-based index in the result.
		///</summary>
		///<param name="column"></param>
		///<returns></returns>
		QueryResultColumn GetColumn(int column);

	    ///<summary>
	    /// Returns any warnings about the command or <b>null</b> if there were no 
	    /// warnings.
	    ///</summary>
	    string Warnings { get; }
	}
}