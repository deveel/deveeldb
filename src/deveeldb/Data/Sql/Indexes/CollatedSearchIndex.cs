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

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Indexes {
	public abstract class CollatedSearchIndex : TableIndex {
		protected CollatedSearchIndex(IndexInfo indexInfo, ITable table)
			: base(indexInfo, table) {
		}

		protected virtual long RowCount => Table.RowCount;

		protected virtual IndexKey First => GetKey(0);

		protected virtual IndexKey Last => GetKey(RowCount - 1);

		protected void ThrowIfReadOnly() {
			if (IsReadOnly)
				throw new InvalidOperationException("The index is read-only");
		}

		public override void Insert(long row) {
			ThrowIfReadOnly();
		}

		public override void Remove(long row) {
			ThrowIfReadOnly();
		}

		protected abstract long SearchFirst(IndexKey value);

		protected abstract long SearchLast(IndexKey value);

		protected virtual IEnumerable<long> AddRange(long start, long end, IEnumerable<long> input) {
			var list = new BigList<long>((end - start) + 2);
			if (input != null)
				list.AddRange(input);

			for (var i = start; i <= end; ++i) {
				list.Add(i);
			}

			return list;
		}

		private long PositionOfRangePoint(RangeFieldOffset position, IndexKey val) {
			long p;
			IndexKey cell;

			switch (position) {

				case RangeFieldOffset.FirstValue:

					if (val.Equals(IndexRange.FirstInSet)) {
						return 0;
					}

					if (val.Equals(IndexRange.LastInSet)) {
						// Get the last value and search for the first instance of it.
						cell = Last;
					}
					else {
						cell = val;
					}

					p = SearchFirst(cell);

					// (If value not found)
					if (p < 0)
						return -(p + 1);

					return p;

				case RangeFieldOffset.LastValue:

					if (val.Equals(IndexRange.LastInSet))
						return RowCount - 1;

					if (val.Equals(IndexRange.FirstInSet)) {
						// Get the first value.
						cell = First;
					}
					else {
						cell = val;
					}

					p = SearchLast(cell);

					// (If value not found)
					if (p < 0) {
						return -(p + 1) - 1;
					}

					return p;

				case RangeFieldOffset.BeforeFirstValue:

					if (val.Equals(IndexRange.FirstInSet))
						return -1;

					if (val.Equals(IndexRange.LastInSet)) {
						// Get the last value and search for the first instance of it.
						cell = Last;
					}
					else {
						cell = val;
					}

					p = SearchFirst(cell);

					// (If value not found)
					if (p < 0) {
						return -(p + 1) - 1;
					}

					return p - 1;

				case RangeFieldOffset.AfterLastValue:

					if (val.Equals(IndexRange.LastInSet)) {
						return RowCount;
					}

					if (val.Equals(IndexRange.FirstInSet)) {
						// Get the first value.
						cell = First;
					}
					else {
						cell = val;
					}

					p = SearchLast(cell);

					// (If value not found)
					if (p < 0) {
						return -(p + 1);
					}

					return p + 1;

				default:

					throw new InvalidOperationException("Unrecognised position.");
			}

		}

		private IEnumerable<long> AddRange(IndexRange range, IEnumerable<long> list) {
			// Select the range specified.
			var startFlag = range.StartOffset;
			var start = range.StartValue;
			var endFlag = range.EndOffset;
			var end = range.EndValue;

			long r1 = PositionOfRangePoint(startFlag, start);
			long r2 = PositionOfRangePoint(endFlag, end);

			if (r2 < r1)
				return list;

			// Add the range to the set
			return AddRange(r1, r2, list);

		}

		public override IEnumerable<long> SelectRange(IndexRange[] ranges) {
			// If no items in the set return an empty set
			if (RowCount == 0)
				return new long[0];

			if (ranges.Length == 1 &&
			    ranges[0] == IndexRange.FullRange) {
				return AddRange(0, RowCount - 1, null);
			}

			IEnumerable<long> list = null;

			foreach (var range in ranges) {
				list = AddRange(range, list);
			}

			if (list == null)
				return new long[0];

			return list;
		}
	}
}