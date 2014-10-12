// 
//  Copyright 2014  Deveel
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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Types;
using Deveel.Math;

namespace Deveel.Data.Types {
	[Serializable]
	public sealed class NumericType : DataType {
		public NumericType(SqlType sqlType, int size, byte scale) 
			: base("NUMERIC", sqlType) {
			Size = size;
			Scale = scale;
		}

		public NumericType(SqlType sqlType)
			: this(sqlType, -1) {
		}

		public NumericType(SqlType sqlType, int size)
			: this(sqlType, size, 0) {
		}

		public int Size { get; private set; }

		public byte Scale { get; private set; }

		public override bool Equals(object obj) {
			var other = obj as NumericType;
			if (other == null)
				return false;

			return SqlType == other.SqlType &&
			       Size == other.Size &&
			       Scale == other.Scale;
		}

		public override int GetHashCode() {
			return base.GetHashCode();
		}

		private static int GetIntSize(SqlType sqlType) {
			switch (sqlType) {
				case SqlType.TinyInt:
					return 1;
				case SqlType.SmallInt:
					return 2;
				case SqlType.Integer:
					return 4;
				case SqlType.BigInt:
					return 8;
				default:
					return 0;
			}
		}


		private static int GetFloatSize(SqlType sqlType) {
			switch (sqlType) {
				default:
					return 0;
				case SqlType.Real:
					return 4;
				case SqlType.Float:
				case SqlType.Double:
					return 8;
			}
		}

		public override DataType Wider(DataType otherType) {
			SqlType t1SqlType = SqlType;
			SqlType t2SqlType = otherType.SqlType;
			if (t1SqlType == SqlType.Decimal) {
				return this;
			}
			if (t2SqlType == SqlType.Decimal) {
				return otherType;
			}
			if (t1SqlType == SqlType.Numeric) {
				return this;
			}
			if (t2SqlType == SqlType.Numeric) {
				return otherType;
			}

			if (t1SqlType == SqlType.Bit) {
				return otherType; // It can't be any smaller than a Bit
			}
			if (t2SqlType == SqlType.Bit) {
				return this;
			}

			int t1IntSize = GetIntSize(t1SqlType);
			int t2IntSize = GetIntSize(t2SqlType);
			if (t1IntSize > 0 && t2IntSize > 0) {
				// Both are int types, use the largest size
				return (t1IntSize > t2IntSize) ? this : otherType;
			}

			int t1FloatSize = GetFloatSize(t1SqlType);
			int t2FloatSize = GetFloatSize(t2SqlType);
			if (t1FloatSize > 0 && t2FloatSize > 0) {
				// Both are floating types, use the largest size
				return (t1FloatSize > t2FloatSize) ? this : otherType;
			}

			if (t1FloatSize > t2IntSize) {
				return this;
			}
			if (t2FloatSize > t1IntSize) {
				return otherType;
			}
			if (t1IntSize >= t2FloatSize || t2IntSize >= t1FloatSize) {
				// Must be a long (8 bytes) and a real (4 bytes), widen to a double
				return new NumericType(SqlType.Double, 8, 0);
			}

			// NOTREACHED - can't get here, the last three if statements cover
			// all possibilities.
			throw new ApplicationException("Widest type error.");
		}

		public override bool IsComparable(DataType type) {
			return type is NumericType || type is BooleanType;
		}

		public override int Compare(DataObject x, DataObject y) {
			var n1 = (Number)x;
			Number n2;

			if (y is Number) {
				n2 = (Number)y;
			} else {
				n2 = (bool)y ? Number.One : Number.Zero;
			}

			return n1.CompareTo(n2);
		}

		private static DateTime ToDate(long time) {
			return new DateTime(time);
		}

		public override DataObject CastTo(DataObject value, DataType destType) {
			var n = (Number) value;

			var sqlType = destType.SqlType;
			switch (sqlType) {
				case (SqlType.Bit):
				case (SqlType.Boolean):
					return n.ToBoolean();
				case (SqlType.TinyInt):
				case (SqlType.SmallInt):
				case (SqlType.Integer):
					return (Number)n.ToInt32();
				case (SqlType.BigInt):
					return (Number)n.ToInt64();
				case (SqlType.Float):
				case (SqlType.Real):
				case (SqlType.Double):
					double d = n.ToDouble();
					NumberState state = NumberState.None;
					if (Double.IsNaN(d))
						state = NumberState.NotANumber;
					else if (Double.IsPositiveInfinity(d))
						state = NumberState.PositiveInfinity;
					else if (Double.IsNegativeInfinity(d))
						state = NumberState.NegativeInfinity;
					return new Number(state, new BigDecimal(d));
				case (SqlType.Numeric):
				// fall through
				case (SqlType.Decimal):
					return Number.Parse(n.ToString());
				case (SqlType.Char):
					return new StringObject(CastUtil.PaddedString(n.ToString(), ((StringType) destType).MaxSize));
				case (SqlType.VarChar):
				case (SqlType.LongVarChar):
					return new StringObject(n.ToString());
				case (SqlType.Date):
				case (SqlType.Time):
				case (SqlType.TimeStamp):
					return ToDate(n.ToInt64());
				case (SqlType.Blob):
				// fall through
				case (SqlType.Binary):
				// fall through
				case (SqlType.VarBinary):
				// fall through
				case (SqlType.LongVarBinary):
					return new BinaryObject(n.ToByteArray());
				case (SqlType.Null):
					return null;
				default:
					throw new InvalidCastException();
			}
		}

		public override string ToString() {
			var sb = new StringBuilder(Name);
			if (Size != -1) {
				sb.Append('(');
				sb.Append(Size);
				if (Scale > 0) {
					sb.Append(',');
					sb.Append(Scale);
				}

				sb.Append(')');
			}
			return sb.ToString();
		}
	}
}