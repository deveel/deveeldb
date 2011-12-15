// 
//  Copyright 2010  Deveel
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
using System.Text;

namespace Deveel.Data {
	/// <summary>
	/// Represents a complex normalized range of a list.
	/// </summary>
	/// <remarks>
	/// This is essentially a set of <see cref="SelectableRange"/> objects 
	/// that make up a complex view of a range.
	/// </remarks>
	/// <example>
	/// For example, say we had a query:
	/// <code>
	/// (a &gt; 10 and a &lt; 20 and a &lt;&gt; 15) or a &gt;= 50
	/// </code>
	/// we could represent this range by the following range set:
	/// <code>
	/// RANGE: AfterLastValue 10, BeforeFirstValue 15
	/// RANGE: AfterLastValue 15, BeforeFirstValue 20
	/// RANGE: FirstValue 50, LastValue LastInSet
	/// </code>
	/// </example>
	public sealed class SelectableRangeSet {
		/// <summary>
		/// The list of ranges.
		/// </summary>
		private List<SelectableRange> rangeSet;

		///<summary>
		/// Constructs the <see cref="SelectableRangeSet"/> to a full range 
		/// (a range that encompases all values).
		///</summary>
		public SelectableRangeSet() {
			rangeSet = new List<SelectableRange>();
			rangeSet.Add(SelectableRange.FullRange);
		}

		/// <summary>
		/// Intersects the given SelectableRange object with the given operator and
		/// value constraint.
		/// </summary>
		/// <remarks>
		/// <b>Note</b>: This does not work with the <c>&lt;&gt;</c> operator 
		/// which must be handled another way.
		/// </remarks>
		private static SelectableRange IntersectRange(SelectableRange range, Operator op, TObject val, bool nullCheck) {
			TObject start = range.Start;
			RangePosition startPosition = range.StartPosition;
			TObject end = range.End;
			RangePosition endPosition = range.EndPosition;

			bool inclusive = op.IsEquivalent("is") || op.IsEquivalent("=") ||
								op.IsEquivalent(">=") || op.IsEquivalent("<=");

			if (op.IsEquivalent("is") || op.IsEquivalent("=") || op.IsEquivalent(">") || op.IsEquivalent(">=")) {
				// With this operator, NULL values must return null.
				if (nullCheck && val.IsNull) {
					return null;
				}

				if (start == SelectableRange.FirstInSet) {
					start = val;
					startPosition = inclusive
					                	? RangePosition.FirstValue
					                	: RangePosition.AfterLastValue;
				} else {
					int c = val.CompareTo(start);
					if ((c == 0 && startPosition == RangePosition.FirstValue) || c > 0) {
						start = val;
						startPosition = inclusive
						                	? RangePosition.FirstValue
						                	: RangePosition.AfterLastValue;
					}
				}
			}
			if (op.IsEquivalent("is") || op.IsEquivalent("=") || op.IsEquivalent("<") || op.IsEquivalent("<=")) {
				// With this operator, NULL values must return null.
				if (nullCheck && val.IsNull) {
					return null;
				}

				// If start is first in set, then we have to change it to after NULL
				if (nullCheck && start == SelectableRange.FirstInSet) {
					start = TObject.Null;
					startPosition = RangePosition.AfterLastValue;
				}

				if (end == SelectableRange.LastInSet) {
					end = val;
					endPosition = inclusive ? RangePosition.LastValue :
										   RangePosition.BeforeFirstValue;
				} else {
					int c = val.CompareTo(end);
					if ((c == 0 && endPosition == RangePosition.LastValue) || c < 0) {
						end = val;
						endPosition = inclusive ? RangePosition.LastValue :
											   RangePosition.BeforeFirstValue;
					}
				}
			}

			// If start and end are not null types (if either are, then it means it
			// is a placeholder value meaning start or end of set).
			if (start != SelectableRange.FirstInSet &&
				end != SelectableRange.LastInSet) {
				// If start is higher than end, return null
				int c = start.CompareTo(end);
				if ((c == 0 && (startPosition == RangePosition.AfterLastValue ||
								endPosition == RangePosition.BeforeFirstValue)) ||
					c > 0) {
					return null;
				}
			}

			// The new intersected range
			return new SelectableRange(startPosition, start, endPosition, end);
		}

		/// <summary>
		/// Returns true if the two SelectableRange ranges intersect.
		/// </summary>
		private static bool RangeIntersectedBy(SelectableRange range1, SelectableRange range2) {
			RangePosition startFlag1 = range1.StartPosition;
			TObject start1 = range1.Start;
			RangePosition endFlag1 = range1.EndPosition;
			TObject end1 = range1.End;

			RangePosition startFlag2 = range2.StartPosition;
			TObject start2 = range2.Start;
			RangePosition endFlag2 = range2.EndPosition;
			TObject end2 = range2.End;

			TObject startCell1 = start1 == SelectableRange.FirstInSet ? null : start1;
			TObject endCell1 = end1 == SelectableRange.LastInSet ? null : end1;
			TObject startCell2 = start2 == SelectableRange.FirstInSet ? null : start2;
			TObject endCell2 = end2 == SelectableRange.LastInSet ? null : end2;

			bool intersect1 = false;
			if (startCell1 != null && endCell2 != null) {
				int c = startCell1.CompareTo(endCell2);
				if (c < 0 ||
					(c == 0 && (startFlag1 == RangePosition.FirstValue ||
								endFlag2 == RangePosition.LastValue))) {
					intersect1 = true;
				}
			} else {
				intersect1 = true;
			}

			bool intersect2 = false;
			if (startCell2 != null && endCell1 != null) {
				int c = startCell2.CompareTo(endCell1);
				if (c < 0 ||
					(c == 0 && (startFlag2 == RangePosition.FirstValue ||
								endFlag1 == RangePosition.LastValue))) {
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
		private static SelectableRange ChangeRangeSizeToEncompass(SelectableRange range1, SelectableRange range2) {
			RangePosition startPosition1 = range1.StartPosition;
			TObject start1 = range1.Start;
			RangePosition endPosition1 = range1.EndPosition;
			TObject end1 = range1.End;

			RangePosition startPosition2 = range2.StartPosition;
			TObject start2 = range2.Start;
			RangePosition endPosition2 = range2.EndPosition;
			TObject end2 = range2.End;

			if (start1 != SelectableRange.FirstInSet) {
				if (start2 != SelectableRange.FirstInSet) {
					TObject cell = start1;
					int c = cell.CompareTo(start2);
					if (c > 0 ||
						c == 0 && startPosition1 == RangePosition.AfterLastValue &&
								  startPosition2 == RangePosition.FirstValue) {
						start1 = start2;
						startPosition1 = startPosition2;
					}
				} else {
					start1 = start2;
					startPosition1 = startPosition2;
				}
			}

			if (end1 != SelectableRange.LastInSet) {
				if (end2 != SelectableRange.LastInSet) {
					TObject cell = end1;
					int c = cell.CompareTo(end2);
					if (c < 0 ||
						c == 0 && endPosition1 == RangePosition.BeforeFirstValue &&
								  endPosition2 == RangePosition.LastValue) {
						end1 = end2;
						endPosition1 = endPosition2;
					}
				} else {
					end1 = end2;
					endPosition1 = endPosition2;
				}
			}

			return new SelectableRange(startPosition1, start1, endPosition1, end1);
		}

		/// <summary>
		/// Intersects this range with the given Operator and value constant.
		/// </summary>
		/// <example>
		/// For example, if a range is <c>'a' -&gt; [END]</c> and the given 
		/// operator is '&lt;=' and the value is 'z' the result range is 'a' -&gt; 'z'.
		/// </example>
		public void Intersect(Operator op, TObject val) {
			lock (this) {
				int sz = rangeSet.Count;
				List<SelectableRange> i = rangeSet.GetRange(0, sz);

				if (op.IsEquivalent("<>") || op.IsEquivalent("is not")) {
					bool nullCheck = op.IsEquivalent("<>");
					int j = 0;
					while (j < sz) {
						SelectableRange range = i[j];
						SelectableRange leftRange = IntersectRange(range, Operator.Get("<"), val, nullCheck);
						SelectableRange rightRange = IntersectRange(range, Operator.Get(">"), val, nullCheck);
						i.RemoveAt(j);
						if (leftRange != null) {
							i.Add(leftRange);
						}
						if (rightRange != null) {
							i.Add(rightRange);
						}
						j++;
					}

					rangeSet = new List<SelectableRange>(i);
				} else {
					bool nullCheck = !op.IsEquivalent("is");
					int j = 0;
					while (j < sz) {
						SelectableRange range = i[j];
						range = IntersectRange(range, op, val, nullCheck);
						if (range == null) {
							i.RemoveAt(j);
						} else {
							i[j] = range;
						}
						j++;
					}

					rangeSet = new List<SelectableRange>(i);
				}
			}
		}

		/// <summary>
		/// Unions this range with the given Operator and value constant.
		/// </summary>
		public void Union(Operator op, TObject val) {
			throw new NotImplementedException();
		}

		/// <summary>
		/// Unions the current range set with the given range set.
		/// </summary>
		public void Union(SelectableRangeSet unionTo) {
			List<SelectableRange> inputSet = unionTo.rangeSet;

			int inSz = inputSet.Count;
			int n = 0;
			while (n < inSz) {
				SelectableRange inRange = (SelectableRange)inputSet[n];
				int sz = rangeSet.Count;
				List<SelectableRange> i = rangeSet.GetRange(0, rangeSet.Count);
				int j = 0;
				while (j < i.Count) {
					SelectableRange range = i[j];
					if (RangeIntersectedBy(inRange, range)) {
						i.RemoveAt(j);
						inRange = ChangeRangeSizeToEncompass(inRange, range);
					}
					j++;
				}

				// Insert into sorted position
				RangePosition startPoint = inRange.StartPosition;
				TObject start = inRange.Start;
				RangePosition endPoint = inRange.EndPosition;
				TObject end = inRange.End;

				if (start == SelectableRange.FirstInSet) {
					rangeSet.Insert(0, inRange);
				} else {
					TObject startCell = start;
					i = rangeSet.GetRange(0, rangeSet.Count);
					j = 0;
					while (j < i.Count) {
						SelectableRange range = i[j];
						TObject curStart = range.Start;
						if (curStart != SelectableRange.FirstInSet) {
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

		}

		/// <summary>
		/// Returns the range as an array of SelectableRange or an empty array 
		/// if there is no range.
		/// </summary>
		/// <returns></returns>
		public SelectableRange[] ToArray() {
			int sz = rangeSet.Count;
			SelectableRange[] ranges = new SelectableRange[sz];
			for (int i = 0; i < sz; ++i) {
				ranges[i] = rangeSet[i];
			}
			return ranges;
		}
	}
}