// 
//  Copyright 2010-2018 Deveel
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
using System.Collections.Generic;
using System.Text;

namespace Deveel.Data.Sql.Tables {
	/// <summary>
	/// The kind of events that can happen on a table row during 
	/// the life-time of a transaction.
	/// </summary>
	public enum TableRowEventType {
		/// <summary>
		/// A new row was added to the table.
		/// </summary>
		Add = 1,

		/// <summary>
		/// A row was removed from a table.
		/// </summary>
		Remove = 2,

		/// <summary>
		/// During an update of values of a row, this was
		/// added again to a table.
		/// </summary>
		UpdateAdd = 3,

		/// <summary>
		/// During an update of values of a row, this
		/// was removed before the value are update.
		/// </summary>
		UpdateRemove = 4,
	}
}