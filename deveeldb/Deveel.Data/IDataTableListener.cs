// 
//  Copyright 2010  Deveel
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