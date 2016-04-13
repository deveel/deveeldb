using System;

namespace Deveel.Data.Sql.Types {
	public sealed class DataTypeInfo {
		public DataTypeInfo(string typeName) 
			: this(typeName, new DataTypeMeta[0]) {
		}

		public DataTypeInfo(string typeName, DataTypeMeta[] metadata) {
			if (String.IsNullOrEmpty(typeName))
				throw new ArgumentNullException("typeName");

			TypeName = typeName;
			Metadata = metadata;
		}

		public string TypeName { get; private set; }

		public DataTypeMeta[] Metadata { get; private set; }

		public bool IsPrimitive {
			get { return PrimitiveTypes.IsPrimitive(TypeName); }
		}
	}
}
