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

using Deveel.Math;

namespace Deveel.Data.Types {
	[Serializable]
	public sealed class NumericType : DataType {
		public NumericType(SqlTypeCode sqlType, int size, byte scale) 
			: base("NUMERIC", sqlType) {
			Size = size;
			Scale = scale;
		}

		public NumericType(SqlTypeCode sqlType)
			: this(sqlType, -1) {
		}

		public NumericType(SqlTypeCode sqlType, int size)
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
			return (SqlType.GetHashCode() * Scale) + Size.GetHashCode();
		}

		private static int GetIntSize(SqlTypeCode sqlType) {
			switch (sqlType) {
				case SqlTypeCode.TinyInt:
					return 1;
				case SqlTypeCode.SmallInt:
					return 2;
				case SqlTypeCode.Integer:
					return 4;
				case SqlTypeCode.BigInt:
					return 8;
				default:
					return 0;
			}
		}


		private static int GetFloatSize(SqlTypeCode sqlType) {
			switch (sqlType) {
				default:
					return 0;
				case SqlTypeCode.Real:
					return 4;
				case SqlTypeCode.Float:
				case SqlTypeCode.Double:
					return 8;
			}
		}

		public override DataType Wider(DataType otherType) {
			SqlTypeCode t1SqlType = SqlType;
			SqlTypeCode t2SqlType = otherType.SqlType;
			if (t1SqlType == SqlTypeCode.Decimal) {
				return this;
			}
			if (t2SqlType == SqlTypeCode.Decimal) {
				return otherType;
			}
			if (t1SqlType == SqlTypeCode.Numeric) {
				return this;
			}
			if (t2SqlType == SqlTypeCode.Numeric) {
				return otherType;
			}

			if (t1SqlType == SqlTypeCode.Bit) {
				return otherType; // It can't be any smaller than a Bit
			}
			if (t2SqlType == SqlTypeCode.Bit) {
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
				return new NumericType(SqlTypeCode.Double, 8, 0);
			}

			// NOTREACHED - can't get here, the last three if statements cover
			// all possibilities.
			throw new ApplicationException("Widest type error.");
		}

		public override bool IsComparable(DataType type) {
			return type is NumericType || type is BooleanType;
		}

		public override int Compare(DataObject x, DataObject y) {
			var n1 = (NumericObject)x;
			NumericObject n2;

			if (y is NumericObject) {
				n2 = (NumericObject)y;
			} else if (y is BooleanObject) {
				n2 = (BooleanObject) y ? NumericObject.One : NumericObject.Zero;
			} else {
				throw new NotSupportedException();
			}

			return n1.CompareTo(n2);
		}

		private static DateObject ToDate(long time) {
			return new DateTime(time);
		}

		public override DataObject CastTo(DataObject value, DataType destType) {
			var n = (NumericObject) value;

			var sqlType = destType.SqlType;
			switch (sqlType) {
				case (SqlTypeCode.Bit):
				case (SqlTypeCode.Boolean):
					return (BooleanObject) n.ToBoolean();
				case (SqlTypeCode.TinyInt):
				case (SqlTypeCode.SmallInt):
				case (SqlTypeCode.Integer):
					return (NumericObject)n.ToInt32();
				case (SqlTypeCode.BigInt):
					return (NumericObject)n.ToInt64();
				case (SqlTypeCode.Float):
				case (SqlTypeCode.Real):
				case (SqlTypeCode.Double):
					double d = n.ToDouble();
					var state = NumericState.None;
					if (Double.IsNaN(d))
						return NumericObject.NaN;
					if (Double.IsPositiveInfinity(d))
						return NumericObject.PositiveInfinity;
					if (Double.IsNegativeInfinity(d))
						return NumericObject.NegativeInfinity;

					return new NumericObject(PrimitiveTypes.Numeric(sqlType), state, new BigDecimal(d));
				case (SqlTypeCode.Numeric):
				// fall through
				case (SqlTypeCode.Decimal):
					return NumericObject.Parse(n.ToString());
				case (SqlTypeCode.Char):
					return new StringObject(CastUtil.PaddedString(n.ToString(), ((StringType) destType).MaxSize));
				case (SqlTypeCode.VarChar):
				case (SqlTypeCode.LongVarChar):
					return new StringObject(PrimitiveTypes.String(sqlType), n.ToString());
				case (SqlTypeCode.Date):
				case (SqlTypeCode.Time):
				case (SqlTypeCode.TimeStamp):
					return ToDate(n.ToInt64());
				case (SqlTypeCode.Blob):
				// fall through
				case (SqlTypeCode.Binary):
				// fall through
				case (SqlTypeCode.VarBinary):
				// fall through
				case (SqlTypeCode.LongVarBinary):
					return new BinaryObject(PrimitiveTypes.Binary(sqlType), n.ToByteArray());
				case (SqlTypeCode.Null):
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