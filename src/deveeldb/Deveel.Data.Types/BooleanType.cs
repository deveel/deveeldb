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
using System.Globalization;
using System.IO;

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Types {
	[Serializable]
	public sealed class BooleanType : SqlType {
		public BooleanType(SqlTypeCode typeCode) 
			: base("BOOLEAN", typeCode) {
			AssertIsBoolean(typeCode);
		}

		private BooleanType(ObjectData data)
			: base(data) {
		}

		private static void AssertIsBoolean(SqlTypeCode sqlType) {
			if (!IsBooleanType(sqlType))
				throw new ArgumentException(String.Format("The SQL type {0} is not BOOLEAN.", sqlType));
		}

		internal static bool IsBooleanType(SqlTypeCode sqlType) {
			return (sqlType == SqlTypeCode.Bit ||
			        sqlType == SqlTypeCode.Boolean);
		}

		public override bool IsStorable {
			get { return true; }
		}

		public override bool IsCacheable(ISqlObject value) {
			return value is SqlBoolean || value is SqlNull;
		}

		public override Type GetObjectType() {
			return typeof (SqlBoolean);
		}

		public override Type GetRuntimeType() {
			return typeof(bool);
		}

		public override int Compare(ISqlObject x, ISqlObject y) {
			if (!(x is SqlBoolean))
				throw new ArgumentException();

			var a = (SqlBoolean) x;
			SqlBoolean b;

			if (y is SqlNumber) {
				b = ((SqlNumber) y) == SqlNumber.One ? SqlBoolean.True : ((SqlNumber) y) == SqlNumber.Zero ? SqlBoolean.False : SqlBoolean.Null;
			} else if (y is SqlBoolean) {
				b = (SqlBoolean) y;
			} else {
				throw new ArgumentException();
			}

			return a.CompareTo(b);
		}

		public override bool IsComparable(SqlType type) {
			return type is BooleanType || type is NumericType;
		}

		public override bool CanCastTo(SqlType destType) {
			return destType is StringType ||
			       destType is NumericType ||
			       destType is BinaryType ||
				   destType is BooleanType;
		}

		public override DataObject CastTo(DataObject value, SqlType destType) {
			var bValue = ((SqlBoolean) value.Value);
			if (destType is StringType) {
				var s = Convert.ToString(bValue);
				// TODO: Provide a method in StringType to build a string specific to the type
				return new DataObject(destType, new SqlString(s));
			}
			if (destType is NumericType) {
				SqlNumber num;
				if (bValue == SqlBoolean.Null) {
					num = SqlNumber.Null;
				} else if (bValue) {
					num = SqlNumber.One;
				} else {
					num = SqlNumber.Zero;
				}

				return new DataObject(destType, num);
			}
			if (destType is BinaryType) {
				var bytes = (byte[]) Convert.ChangeType(bValue, typeof (byte[]), CultureInfo.InvariantCulture);
				return new DataObject(destType, new SqlBinary(bytes));
			}
			if (destType is BooleanType)
				return value;

			return base.CastTo(value, destType);
		}

		public override ISqlObject Negate(ISqlObject value) {
			var b = (SqlBoolean) value;
			return b.Not();
		}

		public override void SerializeObject(Stream stream, ISqlObject obj) {
			var writer = new BinaryWriter(stream);

			if (obj is SqlNull) {
				writer.Write((byte)0);
			} else if (obj is SqlBoolean) {
				var b = (SqlBoolean)obj;

				if (b.IsNull) {
					writer.Write((byte)0);
				} else {
					var value = (bool)b;
					writer.Write((byte)1);
					writer.Write((byte)(value ? 1 : 0));
				}
			} else {
				throw new ArgumentException("Cannot serialize an object that is not a BOOLEAN using a boolean type.");
			}
		}

		public override ISqlObject DeserializeObject(Stream stream) {
			var reader = new BinaryReader(stream);

			var type = reader.ReadByte();
			if (type == 0)
				return SqlBoolean.Null;

			var value = reader.ReadByte();
			return new SqlBoolean(value);
		}

		internal override int ColumnSizeOf(ISqlObject obj) {
			return obj.IsNull ? 1 : 1 + 1;
		}

		public override ISqlObject Reverse(ISqlObject value) {
			return Negate(value);
		}

		public override ISqlObject And(ISqlObject a, ISqlObject b) {
			if (a.IsNull || b.IsNull)
				return SqlBoolean.Null;

			var b1 = (SqlBoolean) a;
			var b2 = (SqlBoolean) b;

			return b1.And(b2);
		}

		public override ISqlObject Or(ISqlObject a, ISqlObject b) {
			if (a.IsNull || b.IsNull)
				return SqlBoolean.Null;

			var b1 = (SqlBoolean)a;
			var b2 = (SqlBoolean)b;

			return b1.Or(b2);
		}

		public override ISqlObject XOr(ISqlObject a, ISqlObject b) {
			if (a.IsNull || b.IsNull)
				return SqlBoolean.Null;

			var b1 = (SqlBoolean)a;
			var b2 = (SqlBoolean)b;

			return b1.And(b2);
		}
	}
}