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
using System.Collections;
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
		private readonly ArrayList range_set;

		///<summary>
		/// Constructs the <see cref="SelectableRangeSet"/> to a full range 
		/// (a range that encompases all values).
		///</summary>
		public SelectableRangeSet() {
			range_set = new ArrayList();
			range_set.Add(SelectableRange.FULL_RANGE);
		}

		/// <summary>
		/// Intersects the given SelectableRange object with the given operator and
		/// value constraint.
		/// </summary>
		/// <remarks>
		/// <b>Note</b>: This does not work with the <c>&lt;&gt;</c> operator 
		/// which must be handled another way.
		/// </remarks>
		private static SelectableRange IntersectRange(SelectableRange range, Operator op, TObject val, bool null_check) {
			TObject start = range.Start;
			byte start_flag = range.StartFlag;
			TObject end = range.End;
			byte end_flag = range.EndFlag;

			bool inclusive = op.Is("is") || op.Is("=") ||
								op.Is(">=") || op.Is("<=");

			if (op.Is("is") || op.Is("=") || op.Is(">") || op.Is(">=")) {
				// With this operator, NULL values must return null.
				if (null_check && val.IsNull) {
					return null;
				}

				if (start == SelectableRange.FIRST_IN_SET) {
					start = val;
					start_flag = inclusive ? SelectableRange.FIRST_VALUE :
											 SelectableRange.AFTER_LAST_VALUE;
				} else {
					int c = val.CompareTo(start);
					if ((c == 0 && start_flag == SelectableRange.FIRST_VALUE) || c > 0) {
						start = val;
						start_flag = inclusive ? SelectableRange.FIRST_VALUE :
												 SelectableRange.AFTER_LAST_VALUE;
					}
				}
			}
			if (op.Is("is") || op.Is("=") || op.Is("<") || op.Is("<=")) {
				// With this operator, NULL values must return null.
				if (null_check && val.IsNull) {
					return null;
				}

				// If start is first in set, then we have to change it to after NULL
				if (null_check && start == SelectableRange.FIRST_IN_SET) {
					start = TObject.Null;
					start_flag = SelectableRange.AFTER_LAST_VALUE;
				}

				if (end == SelectableRange.LAST_IN_SET) {
					end = val;
					end_flag = inclusive ? SelectableRange.LAST_VALUE :
										   SelectableRange.BEFORE_FIRST_VALUE;
				} else {
					int c = val.CompareTo(end);
					if ((c == 0 && end_flag == SelectableRange.LAST_VALUE) || c < 0) {
						end = val;
						end_flag = inclusive ? SelectableRange.LAST_VALUE :
											   SelectableRange.BEFORE_FIRST_VALUE;
					}
				}
			}

			// If start and end are not null types (if either are, then it means it
			// is a placeholder value meaning start or end of set).
			if (start != SelectableRange.FIRST_IN_SET &&
				end != SelectableRange.LAST_IN_SET) {
				// If start is higher than end, return null
				int c = start.CompareTo(end);
				if ((c == 0 && (start_flag == SelectableRange.AFTER_LAST_VALUE ||
								end_flag == SelectableRange.BEFORE_FIRST_VALUE)) ||
					c > 0) {
					return null;
				}
			}

			// The new intersected range
			return new SelectableRange(start_flag, start, end_flag, end);
		}

		/// <summary>
		/// Returns true if the two SelectableRange ranges intersect.
		/// </summary>
		private static bool RangeIntersectedBy(SelectableRange range1,
												  SelectableRange range2) {
			byte start_flag_1 = range1.StartFlag;
			TObject start_1 = range1.Start;
			byte end_flag_1 = range1.EndFlag;
			TObject end_1 = range1.End;

			byte start_flag_2 = range2.StartFlag;
			TObject start_2 = range2.Start;
			byte end_flag_2 = range2.EndFlag;
			TObject end_2 = range2.End;

			TObject start_cell_1, end_cell_1;
			TObject start_cell_2, end_cell_2;

			start_cell_1 = start_1 == SelectableRange.FIRST_IN_SET ? null : start_1;
			end_cell_1 = end_1 == SelectableRange.LAST_IN_SET ? null : end_1;
			start_cell_2 = start_2 == SelectableRange.FIRST_IN_SET ? null : start_2;
			end_cell_2 = end_2 == SelectableRange.LAST_IN_SET ? null : end_2;

			bool intersect_1 = false;
			if (start_cell_1 != null && end_cell_2 != null) {
				int c = start_cell_1.CompareTo(end_cell_2);
				if (c < 0 ||
					(c == 0 && (start_flag_1 == SelectableRange.FIRST_VALUE ||
								end_flag_2 == SelectableRange.LAST_VALUE))) {
					intersect_1 = true;
				}
			} else {
				intersect_1 = true;
			}

			bool intersect_2 = false;
			if (start_cell_2 != null && end_cell_1 != null) {
				int c = start_cell_2.CompareTo(end_cell_1);
				if (c < 0 ||
					(c == 0 && (start_flag_2 == SelectableRange.FIRST_VALUE ||
								end_flag_1 == SelectableRange.LAST_VALUE))) {
					intersect_2 = true;
				}
			} else {
				intersect_2 = true;
			}

			return (intersect_1 && intersect_2);
		}

		/// <summary>
		/// Alters the first range so it encompasses the second range.
		/// </summary>
		/// <remarks>
		/// This assumes that range1 intersects range2.
		/// </remarks>
		private static SelectableRange ChangeRangeSizeToEncompass(SelectableRange range1, SelectableRange range2) {

			byte start_flag_1 = range1.StartFlag;
			TObject start_1 = range1.Start;
			byte end_flag_1 = range1.EndFlag;
			TObject end_1 = range1.End;

			byte start_flag_2 = range2.StartFlag;
			TObject start_2 = range2.Start;
			byte end_flag_2 = range2.EndFlag;
			TObject end_2 = range2.End;

			if (start_1 != SelectableRange.FIRST_IN_SET) {
				if (start_2 != SelectableRange.FIRST_IN_SET) {
					TObject cell = start_1;
					int c = cell.CompareTo(start_2);
					if (c > 0 ||
						c == 0 && start_flag_1 == SelectableRange.AFTER_LAST_VALUE &&
								  start_flag_2 == SelectableRange.FIRST_VALUE) {
						start_1 = start_2;
						start_flag_1 = start_flag_2;
					}
				} else {
					start_1 = start_2;
					start_flag_1 = start_flag_2;
				}
			}

			if (end_1 != SelectableRange.LAST_IN_SET) {
				if (end_2 != SelectableRange.LAST_IN_SET) {
					TObject cell = (TObject)end_1;
					int c = cell.CompareTo(end_2);
					if (c < 0 ||
						c == 0 && end_flag_1 == SelectableRange.BEFORE_FIRST_VALUE &&
								  end_flag_2 == SelectableRange.LAST_VALUE) {
						end_1 = end_2;
						end_flag_1 = end_flag_2;
					}
				} else {
					end_1 = end_2;
					end_flag_1 = end_flag_2;
				}
			}

			return new SelectableRange(start_flag_1, start_1, end_flag_1, end_1);
		}

		/// <summary>
		/// Intersects this range with the given Operator and value constant.
		/// </summary>
		/// <example>
		/// For example, if a range is <c>'a' -&gt; [END]</c> and the given 
		/// operator is '&lt;=' and the value is 'z' the result range is 'a' -&gt; 'z'.
		/// </example>
		public void Intersect(Operator op, TObject val) {
			int sz = range_set.Count;
			ArrayList i = range_set.GetRange(0, sz);
			Queue queue = new Queue(i);

			if (op.Is("<>") || op.Is("is not")) {
				bool nullCheck = op.Is("<>");
				int j = 0;
				while (j < queue.Count) {
					object obj = queue.Peek();
					SelectableRange range = (SelectableRange)obj;
					SelectableRange leftRange = IntersectRange(range, Operator.Get("<"), val, nullCheck);
					SelectableRange rightRange = IntersectRange(range, Operator.Get(">"), val, nullCheck);
					queue.Dequeue();
					if (leftRange != null)
						queue.Enqueue(leftRange);
					if (rightRange != null)
						queue.Enqueue(rightRange);
					j++;
				}
			} else {
				bool nullCheck = !op.Is("is");
				int j = 0;
				while (j < sz) {
					object obj = i[j];
					SelectableRange range = (SelectableRange)obj;
					range = IntersectRange(range, op, val, nullCheck);
					if (range == null) {
						i.RemoveAt(j);
					} else {
						i[j] = range;
					}
					j++;
				}
			}
		}

		/// <summary>
		/// Unions this range with the given Operator and value constant.
		/// </summary>
		public void Union(Operator op, TObject val) {
			throw new NotImplementedException("PENDING");
		}

		/// <summary>
		/// Unions the current range set with the given range set.
		/// </summary>
		public void Union(SelectableRangeSet union_to) {
			ArrayList inputSet = union_to.range_set;

			int inSz = inputSet.Count;
			int n = 0;
			while (n < inSz) {
				SelectableRange inRange = (SelectableRange)inputSet[n];
				int sz = range_set.Count;
				ArrayList i = range_set.GetRange(0, range_set.Count);
				int j = 0;
				while (j < i.Count) {
					object obj = i[j];
					SelectableRange range = (SelectableRange)obj;
					if (RangeIntersectedBy(inRange, range)) {
						i.RemoveAt(j);
						inRange = ChangeRangeSizeToEncompass(inRange, range);
					}
					j++;
				}

				// Insert into sorted position
				int startPoint = inRange.StartFlag;
				TObject start = inRange.Start;
				int endPoint = inRange.EndFlag;
				TObject end = inRange.End;

				if (start == SelectableRange.FIRST_IN_SET) {
					range_set.Insert(0, inRange);
				} else {
					TObject startCell = start;
					i = range_set.GetRange(0, range_set.Count);
					j = 0;
					while (j < i.Count) {
						SelectableRange range = (SelectableRange)i[j];
						TObject curStart = range.Start;
						if (curStart != SelectableRange.FIRST_IN_SET) {
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
			int sz = range_set.Count;
			SelectableRange[] ranges = new SelectableRange[sz];
			for (int i = 0; i < sz; ++i) {
				ranges[i] = (SelectableRange)range_set[i];
			}
			return ranges;
		}


		/// <inheritdoc/>
		public override String ToString() {
			StringBuilder buf = new StringBuilder();
			if (range_set.Count == 0) {
				return "(NO RANGE)";
			}
			for (int i = 0; i < range_set.Count; ++i) {
				buf.Append(range_set[i]);
				buf.Append(", ");
			}
			return buf.ToString();
		}
	}
}