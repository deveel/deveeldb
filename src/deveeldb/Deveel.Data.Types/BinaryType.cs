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
using System.Linq;
using System.Text;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Types {
	[Serializable]
	public sealed class BinaryType : DataType {
		public int MaxSize { get; private set; }

		public BinaryType(SqlTypeCode sqlType) 
			: this(sqlType, -1) {
		}

		public BinaryType(SqlTypeCode sqlType, int maxSize) 
			: base("BINARY", sqlType) {
			MaxSize = maxSize;
			AssertIsBinary(sqlType);
		}

		public override bool IsIndexable {
			get { return false; }
		}

		private static void AssertIsBinary(SqlTypeCode sqlType) {
			if (sqlType != SqlTypeCode.Binary &&
				sqlType != SqlTypeCode.VarBinary &&
				sqlType != SqlTypeCode.LongVarBinary &&
				sqlType != SqlTypeCode.Blob)
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
	}
}