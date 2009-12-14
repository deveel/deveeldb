// 
//  DeveelDbNumber.cs
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

using Deveel.Math;

namespace Deveel.Data.Client {
	public struct DeveelDbNumber : IComparable, INullable {
		private DeveelDbNumber(BigDecimal value, NumberState state, bool isNull) {
			if (state == NumberState.None) {
				this.value = value;
			} else {
				this.value = null;
			}
			this.state = state;
			this.isNull = isNull;
			intForm = false;
			longForm = false;
		}

		internal DeveelDbNumber(BigDecimal value, NumberState state)
			: this(value, state, false) {
		}

		public DeveelDbNumber(BigDecimal value)
			: this(value, NumberState.None, false) {
		}

		public DeveelDbNumber(int value)
			: this(new BigDecimal(value)) {
			intForm = true;
		}

		public DeveelDbNumber(long value)
			: this(new BigDecimal(value)) {
			longForm = true;
		}

		public DeveelDbNumber(double value)
			: this(new BigDecimal(value), GetNumberState(value)) {
		}

		private readonly BigDecimal value;
		private readonly NumberState state;
		private readonly bool isNull;
		private readonly bool intForm;
		private readonly bool longForm;

		public static readonly DeveelDbNumber Null = new DeveelDbNumber(null, NumberState.None, true);
		public static readonly DeveelDbNumber PositiveInfinity = new DeveelDbNumber(null, NumberState.PositiveInfinity, false);
		public static readonly DeveelDbNumber NegativeInfinity = new DeveelDbNumber(null, NumberState.NegativeInfinity, false);
		public static readonly DeveelDbNumber NaN = new DeveelDbNumber(null, NumberState.NotANumber, false);

		public bool IsPositiveInfinity {
			get { return state == NumberState.PositiveInfinity; }
		}

		public bool IsNegativeInfinity {
			get { return state == NumberState.NegativeInfinity; }
		}

		public bool IsNaN {
			get { return state == NumberState.NotANumber; }
		}

		internal bool IsFromInt32 {
			get { return intForm; }
		}

		internal bool IsFromInt64 {
			get { return longForm; }
		}

		internal NumberState State {
			get { return state; }
		}

		public int Scale {
			get { return (state == NumberState.None ? value.Scale : -1); }
		}

		#region Implementation of IComparable

		public int CompareTo(object obj) {
			if (obj == null || obj == DBNull.Value)
				return isNull ? 0 : -1;

			if (!(obj is DeveelDbNumber))
				throw new ArgumentException();

			DeveelDbNumber other = (DeveelDbNumber) obj;
			if (isNull && other.isNull)
				return 0;

			//TODO: check the number state...

			return value.CompareTo(other.value);
		}

		#endregion

		#region Implementation of INullable

		public bool IsNull {
			get { return isNull; }
		}

		#endregion

		private static NumberState GetNumberState(double value) {
			if (Double.IsNaN(value))
				return NumberState.NotANumber;
			if (Double.IsNegativeInfinity(value))
				return NumberState.NegativeInfinity;
			if (Double.IsPositiveInfinity(value))
				return NumberState.PositiveInfinity;
			return NumberState.None;
		}

		public int ToInt32() {
			if (state != NumberState.None)
				return (int) ToDouble();
			return value.ToInt32();
		}

		public long ToInt64() {
			if (state != NumberState.None)
				return (long) ToDouble();
			return value.ToInt64();
		}

		public double ToDouble() {
			switch(state) {
				case (NumberState.NegativeInfinity):
					return Double.NegativeInfinity;
				case (NumberState.PositiveInfinity):
					return Double.PositiveInfinity;
				case (NumberState.NotANumber):
					return Double.NaN;
				default:
					return value.ToDouble();					
			}
		}

		public byte[] ToByteArray() {
			if (state != NumberState.None)
				return new byte[0];
			return value.MovePointRight(value.Scale).ToBigInteger().ToByteArray();
		}

		//TODO: operators...
	}
}