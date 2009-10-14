//  
//  SelectableRange.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Text;

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
	/// For example, start can not be <see cref="LAST_VALUE"/> 
	/// and end can not be  <see cref="FIRST_VALUE"/>.
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
		// ---------- Statics ----------

		// Represents the various points in the set on the value to represent the
		// set range.
		public const byte AFTER_LAST_VALUE = 4;
		public const byte BEFORE_FIRST_VALUE = 3;

		public const byte FIRST_VALUE = 1,
		                  LAST_VALUE = 2;

		/// <summary>
		/// An object that represents the first value in the set.
		/// </summary>
		/// <remarks>
		/// Note that these objects have no (NULL) type.
		/// </remarks>
		public static readonly TObject FIRST_IN_SET = new TObject(TType.NullType, "[FIRST_IN_SET]");

		/// <summary>
		/// An object that represents the last value in the set.
		/// </summary>
		/// <remarks>
		/// Note that these objects have no (NULL) type.
		/// </remarks>
		public static readonly TObject LAST_IN_SET = new TObject(TType.NullType, "[LAST_IN_SET]");

		/// <summary>
		/// The range that represents the entire range (including null).
		/// </summary>
		public static readonly SelectableRange FULL_RANGE =
			new SelectableRange(FIRST_VALUE, FIRST_IN_SET, LAST_VALUE, LAST_IN_SET);

		/// <summary>
		/// The range that represents the entire range (not including null).
		/// </summary>
		public static readonly SelectableRange FULL_RANGE_NO_NULLS = new SelectableRange(AFTER_LAST_VALUE, TObject.Null,
		                                                                                 LAST_VALUE, LAST_IN_SET);

		/// <summary>
		/// The end of the range to select from the set.
		/// </summary>
		private readonly TObject end;

		/// <summary>
		/// Denotes the place for the range to end with respect to the end value.
		/// </summary>
		/// <remarks>
		/// Either <see cref="BEFORE_FIRST_VALUE"/> or <see cref="LAST_VALUE"/>.
		/// </remarks>
		private readonly byte set_end_flag;

		/// <summary>
		/// Denotes the place for the range to start with respect to the start value.
		/// </summary>
		/// <remarks>
		/// Either <see cref="FIRST_VALUE"/> or <see cref="AFTER_LAST_VALUE"/>.
		/// </remarks>
		private readonly byte set_start_flag;

		/// <summary>
		/// The start of the range to select from the set.
		/// </summary>
		private readonly TObject start;

		///<summary>
		///</summary>
		///<param name="set_start_flag"></param>
		///<param name="start"></param>
		///<param name="set_end_flag"></param>
		///<param name="end"></param>
		public SelectableRange(byte set_start_flag, TObject start,
		                       byte set_end_flag, TObject end) {
			this.start = start;
			this.end = end;
			this.set_start_flag = set_start_flag;
			this.set_end_flag = set_end_flag;
		}

		/// <summary>
		/// Gets the start of the range.
		/// </summary>
		/// <remarks>
		/// This may return <see cref="FIRST_IN_SET"/> or  <see cref="LAST_IN_SET"/>.
		/// </remarks>
		public TObject Start {
			get { return start; }
		}

		/// <summary>
		/// Gets the end of the range.
		/// </summary>
		/// <remarks>
		/// This may return <see cref="FIRST_IN_SET"/> or  <see cref="LAST_IN_SET"/>.
		/// </remarks>
		public TObject End {
			get { return end; }
		}

		/// <summary>
		/// Gets the point for the range to start.
		/// </summary>
		/// <remarks>
		/// This must be either <see cref="FIRST_VALUE"/> or <see cref="AFTER_LAST_VALUE"/>.
		/// </remarks>
		public byte StartFlag {
			get { return set_start_flag; }
		}

		/// <summary>
		/// Gets the point for the range to end.
		/// </summary>
		/// <remarks>
		/// This must be either <see cref="BEFORE_FIRST_VALUE"/> or 
		/// <see cref="LAST_VALUE"/>.
		/// </remarks>
		public byte EndFlag {
			get { return set_end_flag; }
		}

		/// <inheritdoc/>
		public override String ToString() {
			StringBuilder buf = new StringBuilder();
			if (StartFlag == FIRST_VALUE) {
				buf.Append("FIRST_VALUE ");
			} else if (StartFlag == AFTER_LAST_VALUE) {
				buf.Append("AFTER_LAST_VALUE ");
			}
			buf.Append(Start.ToString());
			buf.Append(" -> ");
			if (EndFlag == LAST_VALUE) {
				buf.Append("LAST_VALUE ");
			} else if (EndFlag == BEFORE_FIRST_VALUE) {
				buf.Append("BEFORE_FIRST_VALUE ");
			}
			buf.Append(End.ToString());
			return buf.ToString();
		}

		/// <inheritdoc/>
		public override bool Equals(Object ob) {
			if (base.Equals(ob)) {
				return true;
			}

			SelectableRange dest_range = (SelectableRange)ob;
			return (Start.ValuesEqual(dest_range.Start) &&
			        End.ValuesEqual(dest_range.End) &&
			        StartFlag == dest_range.StartFlag &&
			        EndFlag == dest_range.EndFlag);
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return base.GetHashCode();
		}
	}
}