// 
//  Copyright 2010-2015 Deveel
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

namespace Deveel.Data.Sql.Triggers {
	public sealed class OldNewTableState {
		internal OldNewTableState(ObjectName tableSource, int oldRowIndex, Row newDataRow, bool newMutable) {
			TableSource = tableSource;
			OldRowIndex = oldRowIndex;
			NewDataRow = newDataRow;
			IsNewMutable = newMutable;
		}

		internal OldNewTableState() {
			OldRowIndex = -1;
		}

		public ObjectName TableSource { get; private set; }

		public int OldRowIndex { get; private set; }

		public Row NewDataRow { get; private set; }

		public bool IsNewMutable { get; private set; }

		/// <summary>
		/// The tUserContextTable object that represents the OLD table, if set.
		/// </summary>
		public ITable OldDataTable { get; internal set; }

		/// <summary>
		/// The tUserContextTable object that represents the NEW table, if set.
		/// </summary>
		public ITable NewDataTable { get; internal set; }
	}
}