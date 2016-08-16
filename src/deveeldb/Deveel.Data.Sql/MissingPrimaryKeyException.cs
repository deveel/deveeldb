using System;

namespace Deveel.Data.Sql {
	public sealed class MissingPrimaryKeyException : SqlErrorException {
		internal MissingPrimaryKeyException(ObjectName tableName)
			: base(SystemErrorCodes.MissingPrimaryKey, FormatMessage(tableName)) {
		}

		private static string FormatMessage(ObjectName tableName) {
			return String.Format("The table '{0}' has no PRIMARY key defined.", tableName);
		}
	}
}
