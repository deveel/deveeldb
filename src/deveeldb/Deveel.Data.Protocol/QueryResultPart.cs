﻿// 
//  Copyright 2010-2014 Deveel
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
using System.Collections.Generic;

namespace Deveel.Data.Protocol {
	[Serializable]
	public sealed class QueryResultPart {
		private readonly List<object[]> rows;

		public QueryResultPart(int columnCount) {
			ColumnCount = columnCount;
			rows = new List<object[]>();
		}

		public int ColumnCount { get; private set; }

		public void AddRow(object[] row) {
			rows.Add(row);
		}

		public object[] GetRow(int index) {
			return rows[index];
		}
	}
}