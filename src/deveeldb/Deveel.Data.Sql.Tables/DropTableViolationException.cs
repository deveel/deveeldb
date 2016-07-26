using System;

namespace Deveel.Data.Sql.Tables {
	public sealed class DropTableViolationException : ConstraintViolationException {
		internal DropTableViolationException(ObjectName tableName, string constraintName, ObjectName linkedTableName)
			: base(SystemErrorCodes.TableDropViolation, FormatMessage(tableName, constraintName, linkedTableName)) {
			TableName = tableName;
			ConstraintName = constraintName;
			LinkedTableName = linkedTableName;
		}

		public ObjectName TableName { get; private set; }

		public string ConstraintName { get; private set; }

		public ObjectName LinkedTableName { get; private set; }

		private static string FormatMessage(ObjectName tableName, string constraintName, ObjectName linkedTableName) {
			return String.Format("Attempt to DROP the table '{0}' that is linked by the constraint '{1}' to table '{2}'.", tableName, constraintName, linkedTableName);
		}
	}
}
