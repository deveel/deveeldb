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

using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Types {
	/// <summary>
	/// A data type that represents the <c>NULL</c> value of a given
	/// SQL data type.
	/// </summary>
	public sealed class NullType : SqlType {
		/// <summary>
		/// Constructs the type with the given <see cref="SqlTypeCode"/>.
		/// </summary>
		/// <param name="typeCode"></param>
		public NullType(SqlTypeCode typeCode) 
			: base("NULL", typeCode) {
		}

		public override void SerializeObject(Stream stream, ISqlObject obj) {
			var writer = new BinaryWriter(stream);

			if (obj is SqlNull) {
				writer.Write((byte)1);
			} else if (obj == null || obj.IsNull) {
				writer.Write((byte)2);
			}

			throw new FormatException();
		}

		public override ISqlObject DeserializeObject(Stream stream) {
			var reader = new BinaryReader(stream);
			var type = reader.ReadByte();

			if (type == 1)
				return SqlNull.Value;
			if (type == 2) {
				// TODO: check the SQL Type Code of the type and construct the
				//       NULL value specific for the type.
				throw new NotImplementedException();
			}

			throw new FormatException();
		}

		public override object ConvertTo(ISqlObject obj, Type destType) {
			if (obj == null || obj.IsNull)
				return null;

			throw new InvalidCastException();
		}
	}
}
