// 
//  Copyright 2010-2011  Deveel
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
using System.Text;

using Deveel.Data.Types;

namespace Deveel.Data {
	/// <summary>
	/// An object that represents a range of values to select from a list.
	/// </summary>
	/// <remarks>
	/// A range has a start value, an end value, and whether we should pick 
	/// inclusive or exclusive of the end value. The start value may be a 
	/// concrete value from the set or it may be a flag that represents the 
	/// start or end of the list.
	/// <para>
	/// Note that the the start value may not compare less than the end value.
	/// For example, start can not be <see cref="RangePosition.LastValue"/> 
	/// and end can not be  <see cref="RangePosition.FirstValue"/>.
	/// </para>
	/// </remarks>
	/// <example>
	/// For example, to select the first item from a set the range would be:
	/// <code>
	/// RANGE:
	///		start = FirstValue, first
	///		end   = LastValue, first
	/// </code>
	/// To select the last item from a set the range would be:
	/// <code>
	/// RANGE:
	///		start = FirstValue, last
	///		end   = LastValue, last
	/// </code>
	/// To select the range of values between '10' and '15' then range would be:
	/// <code>
	/// RANGE:
	///		start = FirstValue, '10'
	///		end   = LastValue, '15'
	/// </code>
	/// </example>
	public sealed class SelectableRange {
		/// <summary>
		/// An object that represents the first value in the set.
		/// </summary>
		/// <remarks>
		/// Note that these objects have no (NULL) type.
		/// </remarks>
		public static readonly TObject FirstInSet = new TObject(TType.NullType, "[FIRST_IN_SET]");

		/// <summary>
		/// An object that represents the last value in the set.
		/// </summary>
		/// <remarks>
		/// Note that these objects have no (NULL) type.
		/// </remarks>
		public static readonly TObject LastInSet = new TObject(TType.NullType, "[LAST_IN_SET]");

		/// <summary>
		/// The range that represents the entire range (including null).
		/// </summary>
		public static readonly SelectableRange FullRange = new SelectableRange(RangePosition.FirstValue, FirstInSet,
		                                                                       RangePosition.LastValue, LastInSet);

		/// <summary>
		/// The range that represents the entire range (not including null).
		/// </summary>
		public static readonly SelectableRange FullRangeNoNulls = new SelectableRange(RangePosition.AfterLastValue, TObject.Null,
		                                                                              RangePosition.LastValue, LastInSet);

		/// <summary>
		/// The end of the range to select from the set.
		/// </summary>
		private readonly TObject end;

		/// <summary>
		/// Denotes the place for the range to end with respect to the end value.
		/// </summary>
		/// <remarks>
		/// Either <see cref="RangePosition.BeforeFirstValue"/> or 
		/// <see cref="RangePosition.LastValue"/>.
		/// </remarks>
		private readonly RangePosition endPosition;

		/// <summary>
		/// Denotes the place for the range to start with respect to the start value.
		/// </summary>
		/// <remarks>
		/// Either <see cref="RangePosition.FirstValue"/> or 
		/// <see cref="RangePosition.AfterLastValue"/>.
		/// </remarks>
		private readonly RangePosition startPosition;

		/// <summary>
		/// The start of the range to select from the set.
		/// </summary>
		private readonly TObject start;

		///<summary>
		///</summary>
		///<param name="startPosition"></param>
		///<param name="start"></param>
		///<param name="endPosition"></param>
		///<param name="end"></param>
		public SelectableRange(RangePosition startPosition, TObject start,
		                       RangePosition endPosition, TObject end) {
			this.start = start;
			this.end = end;
			this.startPosition = startPosition;
			this.endPosition = endPosition;
		}

		/// <summary>
		/// Gets the start of the range.
		/// </summary>
		/// <remarks>
		/// This may return <see cref="FirstInSet"/> or  <see cref="LastInSet"/>.
		/// </remarks>
		public TObject Start {
			get { return start; }
		}

		/// <summary>
		/// Gets the end of the range.
		/// </summary>
		/// <remarks>
		/// This may return <see cref="FirstInSet"/> or  <see cref="LastInSet"/>.
		/// </remarks>
		public TObject End {
			get { return end; }
		}

		/// <summary>
		/// Gets the point for the range to start.
		/// </summary>
		/// <remarks>
		/// This must be either <see cref="RangePosition.FirstValue"/> or 
		/// <see cref="RangePosition.AfterLastValue"/>.
		/// </remarks>
		public RangePosition StartPosition {
			get { return startPosition; }
		}

		/// <summary>
		/// Gets the point for the range to end.
		/// </summary>
		/// <remarks>
		/// This must be either <see cref="RangePosition.BeforeFirstValue"/> or 
		/// <see cref="RangePosition.LastValue"/>.
		/// </remarks>
		public RangePosition EndPosition {
			get { return endPosition; }
		}

		/// <inheritdoc/>
		public override String ToString() {
			StringBuilder buf = new StringBuilder();
			if (StartPosition == RangePosition.FirstValue) {
				buf.Append("FIRST_VALUE ");
			} else if (StartPosition == RangePosition.AfterLastValue) {
				buf.Append("AFTER_LAST_VALUE ");
			}
			buf.Append(Start.ToString());
			buf.Append(" -> ");
			if (EndPosition == RangePosition.LastValue) {
				buf.Append("LAST_VALUE ");
			} else if (EndPosition == RangePosition.BeforeFirstValue) {
				buf.Append("BEFORE_FIRST_VALUE ");
			}
			buf.Append(End.ToString());
			return buf.ToString();
		}

		/// <inheritdoc/>
		public override bool Equals(Object ob) {
			SelectableRange destRange = (SelectableRange)ob;
			return (Start.ValuesEqual(destRange.Start) &&
			        End.ValuesEqual(destRange.End) &&
			        StartPosition == destRange.StartPosition &&
			        EndPosition == destRange.EndPosition);
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return base.GetHashCode();
		}
	}
}