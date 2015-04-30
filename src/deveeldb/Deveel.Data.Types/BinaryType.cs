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
using System.IO;
using System.Linq;
using System.Text;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Types {
	[Serializable]
	public sealed class BinaryType : DataType, ISizeableType {
		public BinaryType(SqlTypeCode sqlType) 
			: this(sqlType, -1) {
		}

		public BinaryType(SqlTypeCode sqlType, int maxSize) 
			: base("BINARY", sqlType) {
			MaxSize = maxSize;
			AssertIsBinary(sqlType);
		}

		int ISizeableType.Size {
			get { return MaxSize; }
		}

		public int MaxSize { get; private set; }

		public override bool IsIndexable {
			get { return false; }
		}

		private static void AssertIsBinary(SqlTypeCode sqlType) {
			if (!IsBinaryType(sqlType))
				throw new ArgumentException(String.Format("The SQL type {0} is not a BINARY", sqlType));
		}

		public override string ToString() {
			var sb = new StringBuilder(Name);
			if (MaxSize > 0)
				sb.AppendFormat("({0})", MaxSize);

			return sb.ToString();
		}

		private SqlBoolean ToBoolean(ISqlBinary binary) {
			if (binary.Length != 1)
				throw new InvalidCastException();

			var b = binary.First();
			if (b != 0 && b != 1)
				throw new InvalidCastException();

			return b == 1;
		}

		public override DataObject CastTo(DataObject value, DataType destType) {
			var sqlType = destType.SqlType;
			var binary = ((ISqlBinary) value.Value);

			ISqlObject casted;

			switch (sqlType) {
				case SqlTypeCode.Bit:
					casted = ToBoolean(binary);
					break;
					// TODO: All other casts
				default:
					throw new InvalidCastException();
			}

			return new DataObject(destType, casted);
		}

		public override void SerializeObject(Stream stream, ISqlObject obj, ISystemContext systemContext) {
			var writer = new BinaryWriter(stream);

			if (obj is SqlBinary) {
				var bin = (SqlBinary) obj;
				writer.Write((byte)1);
				writer.Write((int)bin.Length);
				writer.Write(bin.ToByteArray());
			} else if (obj is SqlLongBinary) {
				var lob = (SqlLongBinary) obj;

				writer.Write((byte)2);

				// TODO:
			}

			base.SerializeObject(stream, obj, systemContext);
		}

		public override ISqlObject DeserializeObject(Stream stream, ISystemContext context) {
			var reader = new BinaryReader(stream);

			var type = reader.ReadByte();
			if (type == 1) {
				var length = reader.ReadInt32();
				var bytes = reader.ReadBytes(length);
				return new SqlBinary(bytes);
			} 
			if (type == 2) {
				// TODO:
			}

			throw new FormatException();
		}

		internal static bool IsBinaryType(SqlTypeCode sqlType) {
			return sqlType == SqlTypeCode.Binary ||
			       sqlType == SqlTypeCode.VarBinary ||
			       sqlType == SqlTypeCode.LongVarBinary ||
			       sqlType == SqlTypeCode.Blob;
		}
	}
}