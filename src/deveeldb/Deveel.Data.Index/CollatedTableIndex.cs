// 
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

using Deveel.Data.Sql;

namespace Deveel.Data.Index {
	public abstract class CollatedTableIndex : TableIndex {
		protected CollatedTableIndex(ITable table, int columnOffset)
			: base(table, columnOffset) {
		}

		protected virtual int Length {
			get { return Table.RowCount; }
		}

		protected virtual DataObject FirstInOrder {
			get { return GetValue(0); }
		}

		protected virtual DataObject LastInOrder {
			get { return GetValue(Length - 1); }
		}

		private void AssertNotReadOnly() {
			if (IsReadOnly)
				throw new ArgumentException("The index is read-only and cannot be muted.");
		}

		public override void Insert(int rowNumber) {
			AssertNotReadOnly();
		}

		public override void Remove(int rowNumber) {
			AssertNotReadOnly();
		}

		protected abstract int SearchFirst(DataObject value);

		protected abstract int SearchLast(DataObject value);

		protected virtual IEnumerable<int> AddRangeToSet(int start, int end, IEnumerable<int> list) {
			if (list == null)
				list = new List<int>(((end - start) + 2));

			var result = new List<int>(list);
			for (var i = start; i <= end; ++i) {
				result.Add(i);
			}

			return result;
		}

		public override IEnumerable<int> SelectAll() {
			return AddRangeToSet(0, Length - 1, null);
		}

		private int PositionOfRangeField(RangeFieldOffset position, DataObject val) {
			int p;
			DataObject cell;

			switch (position) {
				case RangeFieldOffset.FirstValue:
					if (val == IndexRange.FirstInSet) {
						return 0;
					}
					if (val == IndexRange.LastInSet) {
						// Get the last value and search for the first instance of it.
						cell = LastInOrder;
					} else {
						cell = val;
					}
					p = SearchFirst(cell);
					// (If value not found)
					if (p < 0) {
						return -(p + 1);
					}
					return p;

				case RangeFieldOffset.LastValue:
					if (val == IndexRange.LastInSet) {
						return Length - 1;
					}
					if (val == IndexRange.FirstInSet) {
						// Get the first value.
						cell = FirstInOrder;
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
					if (val == IndexRange.FirstInSet)
						return -1;

					if (val == IndexRange.LastInSet) {
						// Get the last value and search for the first instance of it.
						cell = LastInOrder;
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
					if (val == IndexRange.LastInSet) {
						return Length;
					}
					if (val == IndexRange.FirstInSet) {
						// Get the first value.
						cell = FirstInOrder;
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
					throw new ApplicationException("Unrecognised position.");
			}
		}

		private IEnumerable<int> AddRange(IndexRange range, IEnumerable<int> list) {
			// Select the range specified.
			var startFlag = range.StartOffset;
			var start = range.StartValue;
			var endFlag = range.EndOffset;
			var end = range.EndValue;

			var r1 = PositionOfRangeField(startFlag, start);
			var r2 = PositionOfRangeField(endFlag, end);

			if (r2 < r1)
				return list;

			// Add the range to the set
			return AddRangeToSet(r1, r2, list);
		}

		/// <inheritdoc/>
		public override IEnumerable<int> SelectRange(IndexRange[] ranges) {
			// If no items in the set return an empty set
			if (Length == 0)
				return new List<int>(0);

			IEnumerable<int> list = null;
			foreach (var range in ranges) {
				list = AddRange(range, list);
			}

			if (list == null)
				return new List<int>(0);

			return list;
		}
	}
}