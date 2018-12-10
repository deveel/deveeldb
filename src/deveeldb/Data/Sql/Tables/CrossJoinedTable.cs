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
using System.Linq;

namespace Deveel.Data.Sql.Tables {
	public class CrossJoinedTable : JoinedTable {
		private readonly long leftRowCount;
		private readonly long rightRowCount;
 
		private readonly IEnumerable<long> leftRows;
		private readonly IEnumerable<long> rightRows;

		private readonly bool leftIsSimpleEnum;
		private readonly bool rightIsSimpleEnum;

		public CrossJoinedTable(ITable left, ITable right)
			: base(new[] {left, right}) {
			leftRowCount = left.RowCount;
			rightRowCount = right.RowCount;

			leftIsSimpleEnum = left.GetEnumerator() is SimpleRowEnumerator;
			rightIsSimpleEnum = right.GetEnumerator() is SimpleRowEnumerator;

			leftRows = !leftIsSimpleEnum ? left.Select(x => x.Number) : null;
			rightRows = !rightIsSimpleEnum ? right.Select(x => x.Number) : null;

		}

		public override long RowCount => leftRowCount * rightRowCount;

		private long GetRightRowIndex(long rowIndex) {
			if (rightIsSimpleEnum)
				return rowIndex;

			return rightRows.ElementAt(rowIndex);
		}

		private long GetLeftRowIndex(long rowIndex) {
			if (leftIsSimpleEnum)
				return rowIndex;

			return leftRows.ElementAt(rowIndex);
		}

		protected override IEnumerable<long> ResolveTableRows(IEnumerable<long> rowSet, int tableNum) {
			var rowList = rowSet.ToBigArray();
			bool pickRightTable = (tableNum == 1);
			for (long n = rowList.Length - 1; n >= 0; --n) {
				var row = rowList[n];

				// Reverse map row index to parent domain
				long parentRow;
				if (pickRightTable) {
					parentRow = GetRightRowIndex(row % rightRowCount);
				} else {
					parentRow = GetLeftRowIndex(row / rightRowCount);
				}
				rowList[n] = parentRow;
			}

			return rowList;
		}
	}
}