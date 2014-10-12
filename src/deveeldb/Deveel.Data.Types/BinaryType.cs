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

		public override DataObject CastTo(DataObject value, DataType destType) {
			var sqlType = destType.SqlType;

			if (value is BinaryObject) {
				if (sqlType != SqlTypeCode.Blob &&
					 sqlType != SqlTypeCode.Binary &&
					 sqlType != SqlTypeCode.VarBinary &&
					 sqlType != SqlTypeCode.LongVarBinary) {
					// Attempt to deserialize it
						
					object graph = ((BinaryObject)value).Deserialize();
					if (!(graph is DataObject))
						throw new InvalidCastException();

					var dataObject = (DataObject) graph;
					return dataObject.Type.CastTo(dataObject, destType);
				}

				// This is a BinaryObject that is being cast to a binary type so
				// no further processing is necessary.
				return value;
			}

			// IBlobRef can be BINARY, VARBINARY or LONGVARBINARY
			if (value is IBlobRef) {
				if (sqlType == SqlTypeCode.Binary ||
					sqlType == SqlTypeCode.Blob ||
					sqlType == SqlTypeCode.VarBinary ||
					sqlType == SqlTypeCode.LongVarBinary) {
					return value;
				}
			}

			return base.CastTo(value, destType);
		}
	}
}