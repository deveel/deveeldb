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
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql.Types {
	public sealed class TypeResolveContext {
		private Dictionary<string, DataTypeMeta> meta;

		public TypeResolveContext(SqlTypeCode typeCode) 
			: this(typeCode, typeCode.ToString().ToUpperInvariant()) {
		}

		public TypeResolveContext(SqlTypeCode typeCode, string typeName) 
			: this(typeCode, typeName, new DataTypeMeta[0]) {
		}

		public TypeResolveContext(SqlTypeCode typeCode, string typeName, DataTypeMeta[] meta) {
			TypeCode = typeCode;
			TypeName = typeName;

			this.meta = new Dictionary<string, DataTypeMeta>(StringComparer.OrdinalIgnoreCase);

			if (meta != null) {
				foreach (var typeMeta in meta) {
					this.meta[typeMeta.Name] = typeMeta;
				}
			}
		}

		public SqlTypeCode TypeCode { get; private set; }

		public string TypeName { get; private set; }

		public bool IsPrimitive {
			get { return PrimitiveTypes.IsPrimitive(TypeCode); }
		}

		public bool HasAnyMeta {
			get { return meta.Count > 0; }
		}

		public bool HasMeta(string name) {
			return meta.ContainsKey(name);
		}

		public DataTypeMeta GetMeta(string name) {
			DataTypeMeta typeMeta;
			if (!meta.TryGetValue(name, out typeMeta))
				return null;

			return typeMeta;
		}

		public DataTypeMeta[] GetMeta() {
			return meta.Values.ToArray();
		}
	}
}
