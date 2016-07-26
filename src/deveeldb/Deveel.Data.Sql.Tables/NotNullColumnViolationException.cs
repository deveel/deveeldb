using System;

namespace Deveel.Data.Sql.Tables {
	public sealed class NotNullColumnViolationException : ConstraintViolationException {
		internal NotNullColumnViolationException(ObjectName tableName, string columnName)
			: base(SystemErrorCodes.NotNullColumnViolation, FormatMessage(tableName, columnName)) {
			TableName = tableName;
			ColumnName = columnName;
		}

		public ObjectName TableName { get; private set; }

		public string ColumnName { get; private set; }

		private static string FormatMessage(ObjectName tableName, string columnName) {
			return String.Format("Attempt to set NULL to the column '{0}' of table '{1}'.", columnName, tableName);
		}
	}
}
