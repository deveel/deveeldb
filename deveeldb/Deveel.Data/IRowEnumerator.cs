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
using System.Collections;

namespace Deveel.Data {
	/// <summary>
	/// Allows for access to a tables rows.
	/// </summary>
	/// <remarks>
	/// Each call to <see cref="RowIndex"/> returns an <see cref="Int32"/> that 
	/// can be used in the <see cref="Table.GetCellContents(Int32, Int32)"/>.
	/// </remarks>
	public interface IRowEnumerator : IEnumerator {
		/// <summary>
		/// Gets the current row index of the enumeration.
		/// </summary>
		int RowIndex { get; }
	}
}