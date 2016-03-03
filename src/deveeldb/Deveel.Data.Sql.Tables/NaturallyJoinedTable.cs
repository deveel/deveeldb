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
	class NaturallyJoinedTable : JoinedTable {
		// The row counts of the left and right tables.
		private readonly int leftRowCount, rightRowCount;

		// The lookup row set for the left and right tables.  Basically, these point
		// to each row in either the left or right tables.
		private readonly IList<int> leftSet, rightSet;
		private readonly bool leftIsSimpleEnum, rightIsSimpleEnum;

		public NaturallyJoinedTable(ITable left, ITable right)
			: base(new[] {left, right}) {
			leftRowCount = left.RowCount;
			rightRowCount = right.RowCount;

			// Build lookup tables for the rows in the parent tables if necessary
			// (usually it's not necessary).

			// If the left or right tables are simple enumerations, we can optimize
			// our access procedure,
			leftIsSimpleEnum = (left.GetEnumerator() is SimpleRowEnumerator);
			rightIsSimpleEnum = (right.GetEnumerator() is SimpleRowEnumerator);

			leftSet = !leftIsSimpleEnum ? left.Select(x => x.RowId.RowNumber).ToList() : null;
			rightSet = !rightIsSimpleEnum ? right.Select(x => x.RowId.RowNumber).ToList() : null;
		}

		public override int RowCount {
			get {
				// Natural join row count is (left table row count * right table row count)
				return leftRowCount * rightRowCount;
			}
		}

		private int GetLeftRowIndex(int rowIndex) {
			if (leftIsSimpleEnum)
				return rowIndex;

			return leftSet[rowIndex];
		}

		private int GetRightRowIndex(int rowIndex) {
			if (rightIsSimpleEnum)
				return rowIndex;

			return rightSet[rowIndex];
		}

		protected override IEnumerable<int> ResolveRowsForTable(IEnumerable<int> rowSet, int tableNum) {
			var rowList = rowSet.ToList();
			bool pickRightTable = (tableNum == 1);
			for (int n = rowList.Count - 1; n >= 0; --n) {
				int aa = rowList[n];
				// Reverse map row index to parent domain
				int parentRow;
				if (pickRightTable) {
					parentRow = GetRightRowIndex(aa % rightRowCount);
				} else {
					parentRow = GetLeftRowIndex(aa / rightRowCount);
				}
				rowList[n] = parentRow;
			}

			return rowList.ToArray();
		}
	}
}
