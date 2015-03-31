using System;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class Schema : IDbObject {
		public Schema(SchemaInfo schemaInfo) {
			if (schemaInfo == null)
				throw new ArgumentNullException("schemaInfo");

			SchemaInfo = schemaInfo;
		}

		public SchemaInfo SchemaInfo { get; private set; }

		ObjectName IDbObject.FullName {
			get { return new ObjectName(SchemaInfo.Name); }
		}

		DbObjectType IDbObject.ObjectType {
			get { return DbObjectType.Schema; }
		}
	}
}
