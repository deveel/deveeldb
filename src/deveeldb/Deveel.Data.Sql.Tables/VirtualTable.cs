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
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql.Tables {
	class VirtualTable : JoinedTable {
		private IList<int>[] rowList;
		private int rowCount;

		public VirtualTable(IEnumerable<ITable> tables) 
			: this(tables, null) {
		}

		public VirtualTable(IEnumerable<ITable> tables, IList<IList<int>> rows) 
			: base(tables) {
			SetRows(rows);
		}

		public VirtualTable(ITable table) 
			: this(table, null) {
		}

		public VirtualTable(ITable table, IList<int> rows) 
			: base(table) {
			SetRows(new []{rows});
		}

		private void SetRows(IList<IList<int>> rows) {
			var tableList = ReferenceTables.ToList();

			for (int i = 0; i < tableList.Count; ++i) {
				rowList[i] = new List<int>(rows[i]);
			}
			if (rows.Count > 0) {
				rowCount = rows[0].Count();
			}
		}

		protected override void Init(IEnumerable<ITable> tables) {
			var tableList = tables.ToList();
			base.Init(tableList);

			int tableCount = tableList.Count;
			rowList = new IList<int>[tableCount];
			for (int i = 0; i < tableCount; ++i) {
				rowList[i] = new List<int>();
			}
		}

		public override int RowCount {
			get { return rowCount; }
		}

		protected IList<int>[] ReferenceRows {
			get { return rowList; }
		}

		protected override IEnumerable<int> ResolveRowsForTable(IEnumerable<int> rows, int tableNum) {
			var rowSet = rows.ToList();
			IList<int> curRowList = rowList[tableNum];
			for (int n = rowSet.Count - 1; n >= 0; --n) {
				int aa = rowSet[n];
				int bb = curRowList[aa];
				rowSet[n] = bb;
			}

			return rowSet.ToArray();
		}
	}
}
