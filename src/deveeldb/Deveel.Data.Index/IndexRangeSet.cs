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

using Deveel.Data.Deveel.Data.Sql;

namespace Deveel.Data.Index {
	public sealed class IndexRangeSet {
		private readonly List<IndexRange> ranges;

		public IndexRangeSet()
			: this(new[] {IndexRange.FullRange}) {
		}

		private IndexRangeSet(IEnumerable<IndexRange> ranges) {
			this.ranges = new List<IndexRange>(ranges);
		}

		private static IndexRange IntersectOn(IndexRange range, BinaryOperator op, DataObject value, bool nullCheck) {
			var start = range.StartValue;
			var startPosition = range.StartOffset;
			var end = range.EndValue;
			var endPosition = range.EndOffset;

			bool inclusive = op.IsOfType(BinaryOperatorType.Is) ||
			                 op.IsOfType(BinaryOperatorType.Equal) ||
			                 op.IsOfType(BinaryOperatorType.GreaterOrEqualThan) ||
			                 op.IsOfType(BinaryOperatorType.SmallerOrEqualThan);

			if (op.IsOfType(BinaryOperatorType.Is) ||
			    op.IsOfType(BinaryOperatorType.Equal) ||
			    op.IsOfType(BinaryOperatorType.GreaterThan) ||
			    op.IsOfType(BinaryOperatorType.GreaterOrEqualThan)) {
				// With this operator, NULL values must return null.
				if (nullCheck && value.IsNull) {
					return IndexRange.Null;
				}

				if (start.Equals(IndexRange.FirstInSet)) {
					start = value;
					startPosition = inclusive ? RangeFieldOffset.FirstValue : RangeFieldOffset.AfterLastValue;
				} else {
					int c = value.CompareTo(start);
					if ((c == 0 && startPosition == RangeFieldOffset.FirstValue) || c > 0) {
						start = value;
						startPosition = inclusive ? RangeFieldOffset.FirstValue : RangeFieldOffset.AfterLastValue;
					}
				}
			}

			if (op.IsOfType(BinaryOperatorType.Is) ||
			    op.IsOfType(BinaryOperatorType.Equal) ||
			    op.IsOfType(BinaryOperatorType.SmallerThan) ||
			    op.IsOfType(BinaryOperatorType.SmallerOrEqualThan)) {
				// With this operator, NULL values must return null.
				if (nullCheck && value.IsNull) {
					return IndexRange.Null;
				}

				// If start is first in set, then we have to change it to after NULL
				if (nullCheck && start.Equals(IndexRange.FirstInSet)) {
					start = DataObject.Null();
					startPosition = RangeFieldOffset.AfterLastValue;
				}

				if (end.Equals(IndexRange.LastInSet)) {
					end = value;
					endPosition = inclusive ? RangeFieldOffset.LastValue : RangeFieldOffset.BeforeFirstValue;
				} else {
					int c = value.CompareTo(end);
					if ((c == 0 && endPosition == RangeFieldOffset.LastValue) || c < 0) {
						end = value;
						endPosition = inclusive ? RangeFieldOffset.LastValue : RangeFieldOffset.BeforeFirstValue;
					}
				}
			}

			// If start and end are not null types (if either are, then it means it
			// is a placeholder value meaning start or end of set).
			if (!start.Equals(IndexRange.FirstInSet) && 
				!end.Equals(IndexRange.LastInSet)) {
				// If start is higher than end, return null
				int c = start.CompareTo(end);
				if ((c == 0 && (startPosition == RangeFieldOffset.AfterLastValue ||
				                endPosition == RangeFieldOffset.BeforeFirstValue)) ||
				    c > 0) {
					return IndexRange.Null;
				}
			}

			// The new intersected range
			return new IndexRange(startPosition, start, endPosition, end);
		}

		/// <summary>
		/// Returns true if the two SelectableRange ranges intersect.
		/// </summary>
		private static bool IntersectedBy(IndexRange range1, IndexRange range2) {
			var startFlag1 = range1.StartOffset;
			var start1 = range1.StartValue;
			var endFlag1 = range1.EndOffset;
			var end1 = range1.EndValue;

			var startFlag2 = range2.StartOffset;
			var start2 = range2.StartValue;
			var endFlag2 = range2.EndOffset;
			var end2 = range2.EndValue;

			var startCell1 = start1.Equals(IndexRange.FirstInSet) ? null : start1;
			var endCell1 = end1.Equals(IndexRange.LastInSet) ? null : end1;
			var startCell2 = start2.Equals(IndexRange.FirstInSet) ? null : start2;
			var endCell2 = end2.Equals(IndexRange.LastInSet) ? null : end2;

			bool intersect1 = false;
			if (startCell1 != null && endCell2 != null) {
				int c = startCell1.CompareTo(endCell2);
				if (c < 0 ||
				    (c == 0 && (startFlag1 == RangeFieldOffset.FirstValue ||
				                endFlag2 == RangeFieldOffset.LastValue))) {
					intersect1 = true;
				}
			} else {
				intersect1 = true;
			}

			bool intersect2 = false;
			if (startCell2 != null && endCell1 != null) {
				int c = startCell2.CompareTo(endCell1);
				if (c < 0 ||
				    (c == 0 && (startFlag2 == RangeFieldOffset.FirstValue ||
				                endFlag1 == RangeFieldOffset.LastValue))) {
					intersect2 = true;
				}
			} else {
				intersect2 = true;
			}

			return (intersect1 && intersect2);
		}

		/// <summary>
		/// Alters the first range so it encompasses the second range.
		/// </summary>
		/// <remarks>
		/// This assumes that range1 intersects range2.
		/// </remarks>
		private static IndexRange ChangeRangeSizeToEncompass(IndexRange range1, IndexRange range2) {
			var startPosition1 = range1.StartOffset;
			var start1 = range1.StartValue;
			var endPosition1 = range1.EndOffset;
			var end1 = range1.EndValue;

			var startPosition2 = range2.StartOffset;
			var start2 = range2.StartValue;
			var endPosition2 = range2.EndOffset;
			var end2 = range2.EndValue;

			if (!start1.Equals(IndexRange.FirstInSet)) {
				if (!start2.Equals(IndexRange.FirstInSet)) {
					var cell = start1;
					int c = cell.CompareTo(start2);
					if (c > 0 ||
					    c == 0 && startPosition1 == RangeFieldOffset.AfterLastValue &&
					    startPosition2 == RangeFieldOffset.FirstValue) {
						start1 = start2;
						startPosition1 = startPosition2;
					}
				} else {
					start1 = start2;
					startPosition1 = startPosition2;
				}
			}

			if (!end1.Equals(IndexRange.LastInSet)) {
				if (!end2.Equals(IndexRange.LastInSet)) {
					var cell = end1;
					int c = cell.CompareTo(end2);
					if (c < 0 ||
					    c == 0 && endPosition1 == RangeFieldOffset.BeforeFirstValue &&
					    endPosition2 == RangeFieldOffset.LastValue) {
						end1 = end2;
						endPosition1 = endPosition2;
					}
				} else {
					end1 = end2;
					endPosition1 = endPosition2;
				}
			}

			return new IndexRange(startPosition1, start1, endPosition1, end1);
		}

		public IndexRangeSet Intersect(BinaryOperator op, DataObject value) {
			lock (this) {
				int sz = ranges.Count;
				var list = ranges.GetRange(0, sz);

				if (op.IsOfType(BinaryOperatorType.NotEqual) ||
				    op.IsOfType(BinaryOperatorType.IsNot)) {
					bool nullCheck = op.IsOfType(BinaryOperatorType.NotEqual);
					int j = 0;
					while (j < sz) {
						var range = list[j];
						var leftRange = IntersectOn(range,  BinaryOperator.SmallerThan, value, nullCheck);
						var rightRange = IntersectOn(range, BinaryOperator.GreaterThan, value, nullCheck);
						list.RemoveAt(j);
						if (leftRange != IndexRange.Null) {
							list.Add(leftRange);
						}
						if (rightRange != IndexRange.Null) {
							list.Add(rightRange);
						}
						j++;
					}

					return new IndexRangeSet(list);
				} else {
					bool nullCheck = !op.IsOfType(BinaryOperatorType.Is);
					int j = 0;
					while (j < sz) {
						var range = list[j];
						range = IntersectOn(range, op, value, nullCheck);
						if (range == IndexRange.Null) {
							list.RemoveAt(j);
						} else {
							list[j] = range;
						}
						j++;
					}

					return new IndexRangeSet(list);
				}
			}
		}

		/// <summary>
		/// Unions the current range set with the given range set.
		/// </summary>
		public IndexRangeSet Union(IndexRangeSet unionTo) {
			lock (this) {
				var rangeSet = new List<IndexRange>(ranges);
				var inputSet = unionTo.ranges;

				int inSz = inputSet.Count;
				int n = 0;
				while (n < inSz) {
					var inRange = inputSet[n];
					int sz = rangeSet.Count;
					var i = rangeSet.GetRange(0, sz);
					int j = 0;
					while (j < i.Count) {
						var range = i[j];
						if (IntersectedBy(inRange, range)) {
							i.RemoveAt(j);
							inRange = ChangeRangeSizeToEncompass(inRange, range);
						}
						j++;
					}

					// Insert into sorted position
					var startPoint = inRange.StartOffset;
					var start = inRange.StartValue;
					var endPoint = inRange.EndOffset;
					var end = inRange.EndValue;

					if (start == IndexRange.FirstInSet) {
						rangeSet.Insert(0, inRange);
					} else {
						var startCell = start;
						i = rangeSet.GetRange(0, rangeSet.Count);
						j = 0;
						while (j < i.Count) {
							var range = i[j];
							var curStart = range.StartValue;
							if (!curStart.Equals(IndexRange.FirstInSet)) {
								if (curStart.CompareTo(startCell) > 0) {
									i[j] = i[j - 1];
									break;
								}
							}
							j++;
						}
						i.Add(inRange);
					}
					n++;
				}

				return new IndexRangeSet(rangeSet);
			}
		}

		public IndexRange[] ToArray() {
			lock (this) {
				return ranges.ToArray();
			}
		}
	}
}