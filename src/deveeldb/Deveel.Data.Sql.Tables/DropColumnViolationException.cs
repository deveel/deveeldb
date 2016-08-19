using System;

namespace Deveel.Data.Sql.Tables {
	public sealed class DropColumnViolationException : ConstraintViolationException {
		internal DropColumnViolationException(ObjectName tableName, string columnName, string constraintName,
			ObjectName linkedTableName)
			: base(SystemErrorCodes.ColumnDropViolation, FormatMessage(tableName, columnName, constraintName, linkedTableName)) {
			TableName = tableName;
			ColumnName = columnName;
			ConstraintName = constraintName;
			LinkedTableName = linkedTableName;
		}

		public ObjectName TableName { get; private set; }

		public string ColumnName { get; private set; }

		public string ConstraintName { get; private set; }

		public ObjectName LinkedTableName { get; private set; }

		private static string FormatMessage(ObjectName tableName, string columnName, string constraintName, ObjectName linkedTableName) {
			return String.Format("Attempt to DROP the column '{0}' in table '{1}' that is linked by constraint '{2}' to table '{3}'",
					columnName, tableName, constraintName, linkedTableName);
		}
	}
}
