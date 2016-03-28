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
using System.Text;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Index {
	/// <summary>
	/// Describes the range of values to select from an index.
	/// </summary>
	/// <remarks>
	/// A range has a start value, an end value, and whether we should pick 
	/// inclusive or exclusive of the end value. The start value may be a 
	/// concrete value from the set or it may be a flag that represents the 
	/// start or end of the list.
	/// <para>
	/// Note that the start value may not compare less than the end value.
	/// For example, start can not be <see cref="RangeFieldOffset.LastValue"/> 
	/// and end can not be  <see cref="RangeFieldOffset.FirstValue"/>.
	/// </para>
	/// </remarks>
	public struct IndexRange {
		/// <summary>
		/// 
		/// </summary>
		public static readonly Field FirstInSet = new Field(PrimitiveTypes.Null(), new SqlString("FirstInSet"));

		/// <summary>
		/// 
		/// </summary>
		public static readonly Field LastInSet = new Field(PrimitiveTypes.Null(), new SqlString("LastInSet"));

		/// <summary>
		/// Constructs the range given a start and an end location
		/// </summary>
		/// <param name="startOffset">The offset of the first value of the range.</param>
		/// <param name="firstValue">The first value of the range</param>
		/// <param name="lastOffset">The offset within the range of the last value.</param>
		/// <param name="endValue">The last value of the range.</param>
		public IndexRange(RangeFieldOffset startOffset, Field firstValue, RangeFieldOffset lastOffset, Field endValue)
			: this(false) {
			StartOffset = startOffset;
			StartValue = firstValue;
			EndOffset = lastOffset;
			EndValue = endValue;
		}

		private IndexRange(bool isNull)
			: this() {
			IsNull = isNull;
		}

		public bool IsNull { get; private set; }

		/// <summary>
		/// The entire range of values in an index (including <c>NULL</c>)
		/// </summary>
		public static readonly IndexRange FullRange = new IndexRange(RangeFieldOffset.FirstValue, FirstInSet,
			RangeFieldOffset.LastValue, LastInSet);

		/// <summary>
		/// The entire range of values in an index (not including <c>NULL</c>)
		/// </summary>
		public static readonly IndexRange FullRangeNotNull = new IndexRange(RangeFieldOffset.AfterLastValue, Field.Null(),
			RangeFieldOffset.LastValue, LastInSet);

		public static readonly IndexRange Null = new IndexRange(true);

		/// <summary>
		/// Gets the offset of the first value of the range.
		/// </summary>
		public RangeFieldOffset StartOffset { get; private set; }

		/// <summary>
		/// Gets the first value of the range.
		/// </summary>
		public Field StartValue { get; private set; }

		/// <summary>
		/// Gets the offset of the last value of the range.
		/// </summary>
		public RangeFieldOffset EndOffset { get; private set; }

		/// <summary>
		/// Gets the last value of the range.
		/// </summary>
		public Field EndValue { get; private set; }

		/// <inheritdoc/>
		public override bool Equals(object obj) {
			var destRange = (IndexRange)obj;
			if (IsNull && destRange.IsNull)
				return true;
			if (IsNull && !destRange.IsNull)
				return false;
			if (!IsNull && destRange.IsNull)
				return false;

			return (StartValue.Value.Equals(destRange.StartValue.Value) &&
			        EndValue.Value.Equals(destRange.EndValue.Value) &&
			        StartOffset == destRange.StartOffset &&
			        EndOffset == destRange.EndOffset);
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return base.GetHashCode();
		}

		/// <inheritdoc/>
		public override string ToString() {
			var sb = new StringBuilder();
			if (StartOffset == RangeFieldOffset.FirstValue) {
				sb.Append("FIRST_VALUE ");
			} else if (StartOffset == RangeFieldOffset.AfterLastValue) {
				sb.Append("AFTER_LAST_VALUE ");
			}

			sb.Append(StartValue.Value);
			sb.Append(" -> ");
			if (EndOffset == RangeFieldOffset.LastValue) {
				sb.Append("LAST_VALUE ");
			} else if (EndOffset == RangeFieldOffset.BeforeFirstValue) {
				sb.Append("BEFORE_FIRST_VALUE ");
			}
			sb.Append(EndValue.Value);
			return sb.ToString();
		}

		public static bool operator ==(IndexRange a, IndexRange b) {
			return a.Equals(b);
		}

		public static bool operator !=(IndexRange a, IndexRange b) {
			return !(a == b);
		}
	}
}