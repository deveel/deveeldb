using System;
using System.IO;

namespace Deveel.Data.Sql.Types {
	public static class SqlTypeSerializer {
		public static void Serialize(SqlType type, BinaryWriter writer) {
			throw new NotImplementedException();
		}

		public static SqlType Deserialize(BinaryReader reader, ISqlTypeResolver resolver) {
			throw new NotImplementedException();
		}
	}
}