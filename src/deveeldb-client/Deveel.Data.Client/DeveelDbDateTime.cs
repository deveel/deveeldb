// 
//  DeveelDbDateTime.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Data.SqlTypes;
using System.Globalization;

namespace Deveel.Data.Client {
	public struct DeveelDbDateTime : IComparable, INullable {
		private DeveelDbDateTime(DateTime value, bool isNull) {
			this.value = value;
			this.isNull = isNull;
		}

		public DeveelDbDateTime(DateTime dt)
			: this(dt, false) {
		}

		public DeveelDbDateTime(long ticks)
			: this(new DateTime(ticks)) {
		}

		public DeveelDbDateTime(DeveelDbDateTime from)
			: this(from.Value) {
		}

		public DeveelDbDateTime(int year, int month, int day)
			: this(new DateTime(year, month, day)) {
		}

		public DeveelDbDateTime(int year, int month, int day, Calendar calendar)
			: this(new DateTime(year, month, day, calendar)) {
		}

		public DeveelDbDateTime(int year, int month, int day, int hour, int minute, int second)
			: this(new DateTime(year, month, day, hour, minute, second)) {
		}

		public DeveelDbDateTime(int year, int month, int day, int hour, int minute, int second, Calendar calendar)
			: this(new DateTime(year, month, day, hour, minute, second, calendar)) {
		}

		public DeveelDbDateTime(int year, int month, int day, int hour, int minute, int second, int millisecond)
			: this(new DateTime(year, month, day, hour, minute, second, millisecond)) {
		}

		public DeveelDbDateTime(int year, int month, int day, int hour, int minute, int second, int millisecond, Calendar calendar)
			: this(new DateTime(year, month, day, hour, minute, second, millisecond, calendar)) {
		}

		private readonly DateTime value;
		private readonly bool isNull;

		public static readonly DeveelDbDateTime Null = new DeveelDbDateTime(DateTime.MinValue, true);
		public static readonly DeveelDbDateTime MaxValue = new DeveelDbDateTime(DateTime.MaxValue);
		public static readonly DeveelDbDateTime MinValue = new DeveelDbDateTime(DateTime.MinValue);

		public DateTime Value {
			get { return value; }
		}

		#region Implementation of IComparable

		public int CompareTo(object obj) {
			if (obj == null || obj == DBNull.Value)
				return isNull ? 0 : -1;
			if (obj is DateTime) {
				if (isNull)
					throw new ArgumentException("Cannot compare NULL TIME values");
				return value.CompareTo(obj);
			}
			if (obj is DeveelDbDateTime) {
				DeveelDbDateTime d = (DeveelDbDateTime) obj;
				return isNull && d.isNull ? 0 : value.CompareTo(d.value);
			}

			throw new ArgumentException("Cannot compare DeveelDB times to " + obj.GetType());
		}

		#endregion

		public override bool Equals(object obj) {
			if (obj == null || obj == DBNull.Value)
				return isNull;

			if (obj is DateTime)
				return isNull ? false : value.Equals((DateTime) obj);
			if (obj is DeveelDbDateTime) {
				DeveelDbDateTime d = (DeveelDbDateTime) obj;
				return isNull && d.isNull || value.Equals(d.value);
			}

			throw new ArgumentException("Cannot test equality between DeveelDB times to " + obj.GetType());
		}

		public override int GetHashCode() {
			return isNull ? 0 : value.GetHashCode();
		}

		public override string ToString() {
			return isNull ? "NULL" : value.ToString(CultureInfo.InvariantCulture);
		}

		#region Implementation of INullable

		public bool IsNull {
			get { return isNull; }
		}

		#endregion

		public static DeveelDbDateTime Parse(string s) {
			return s == null ? Null : new DeveelDbDateTime(DateTime.Parse(s, CultureInfo.InvariantCulture));
		}

		#region Operators

		public static DeveelDbBoolean operator ==(DeveelDbDateTime a, DeveelDbDateTime b) {
			return a.IsNull || b.IsNull ? DeveelDbBoolean.Null : a.value == b.value;
		}

		public static DeveelDbBoolean operator !=(DeveelDbDateTime a, DeveelDbDateTime b) {
			return !(a == b);
		}

		public static DeveelDbBoolean operator >(DeveelDbDateTime a, DeveelDbDateTime b) {
			return a.IsNull || b.IsNull ? DeveelDbBoolean.Null : a.value > b.value;
		}

		public static DeveelDbBoolean operator <(DeveelDbDateTime a, DeveelDbDateTime b) {
			return a.IsNull || b.IsNull ? DeveelDbBoolean.Null : new DeveelDbBoolean(a.value < b.value);
		}

		// implicit operators ...

		public static implicit operator DeveelDbDateTime(DateTime d) {
			return new DeveelDbDateTime(d);
		}

		public static implicit operator DeveelDbDateTime(string s) {
			return Parse(s);
		}

		// explicit operators ...

		public static explicit operator DateTime(DeveelDbDateTime d) {
			return d.Value;
		}

		public static explicit operator string (DeveelDbDateTime d) {
			return d.ToString();
		}

		#endregion
	}
}