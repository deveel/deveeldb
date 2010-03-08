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