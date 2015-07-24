using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Types {
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
