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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Index {
	public abstract class CollatedSearchIndex : ColumnIndex {
		protected CollatedSearchIndex(ITable table, int columnOffset) 
			: base(table, columnOffset) {
		}

		protected virtual int Count {
			get { return Table.RowCount; }
		}

		protected virtual DataObject First {
			get { return GetValue(0); }
		}

		protected virtual DataObject Last {
			get { return GetValue(Count - 1); }
		}

		private void AssertNotReadOnly() {
			if (IsReadOnly)
				throw new InvalidOperationException("The index is read-only");
		}

		public override void Insert(int rowNumber) {
			AssertNotReadOnly();
		}

		public override void Remove(int rowNumber) {
			AssertNotReadOnly();
		}

		protected override void Dispose(bool disposing) {
		}

		protected abstract int SearchFirst(DataObject value);

		protected abstract int SearchLast(DataObject value);

		protected virtual IEnumerable<int> AddRange(int start, int end, IEnumerable<int> input) {
			var list = new List<int>((end - start) + 2);
			if (input != null)
				list.AddRange(input);

			for (int i = start; i <= end; ++i) {
				list.Add(i);
			}

			return list;
		}

		private IEnumerable<int> AddRange(IndexRange range, IEnumerable<int> list) {
			// Select the range specified.
			var startFlag = range.StartOffset;
			var start = range.StartValue;
			var endFlag = range.EndOffset;
			var end = range.EndValue;

			int r1 = PositionOfRangePoint(startFlag, start);
			int r2 = PositionOfRangePoint(endFlag, end);

			if (r2 < r1)
				return list;

			// Add the range to the set
			return AddRange(r1, r2, list);

		}

		public override IEnumerable<int> SelectAll() {
			return AddRange(0, Count - 1, null);
		}

		public override IEnumerable<int> SelectRange(IndexRange[] ranges) {
			// If no items in the set return an empty set
			if (Count == 0)
				return new List<int>(0);

			IEnumerable<int> list = null;
			foreach (var range in ranges) {
				list = AddRange(range, list);
			}

			if (list == null)
				return new List<int>(0);

			return list;
		}

		private int PositionOfRangePoint(RangeFieldOffset position, DataObject val) {
			int p;
			DataObject cell;

			switch (position) {

				case RangeFieldOffset.FirstValue:
					if (val.Equals(IndexRange.FirstInSet)) {
						return 0;
					}
					if (val.Equals(IndexRange.LastInSet)) {
						// Get the last value and search for the first instance of it.
						cell = Last;
					} else {
						cell = val;
					}

					p = SearchFirst(cell);

					// (If value not found)
					if (p < 0)
						return -(p + 1);

					return p;

				case RangeFieldOffset.LastValue:
					if (val.Equals(IndexRange.LastInSet))
						return Count - 1;

					if (val.Equals(IndexRange.FirstInSet)) {
						// Get the first value.
						cell = First;
					} else {
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
					} else {
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
						return Count;
					}
					if (val.Equals(IndexRange.FirstInSet)) {
						// Get the first value.
						cell = First;
					} else {
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
	}
}
