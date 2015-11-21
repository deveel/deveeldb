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
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	public class SimpleRowEnumerator : IEnumerator<Row> {
		private readonly ITable source;
		private int index = -1;
		private int rowCount;

		public SimpleRowEnumerator(ITable source) {
			if (source == null)
				throw new ArgumentNullException("source");

			this.source = source;
			rowCount = source.RowCount;
		}

		public void Dispose() {
		}

		public bool MoveNext() {
			return ++index < rowCount;
		}

		public void Reset() {
			rowCount = source.RowCount;
			index = -1;
		}

		public Row Current {
			get { return new Row(source, new RowId(source.TableInfo.Id, index)); }
		}

		object IEnumerator.Current {
			get { return Current; }
		}
	}
}