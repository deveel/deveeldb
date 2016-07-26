using System;

namespace Deveel.Data.Sql.Tables {
	public sealed class PrimaryKeyViolationException : ConstraintViolationException {
		internal PrimaryKeyViolationException(ObjectName tableName, string constraintName, string[] columnNames, ConstraintDeferrability deferrability)
			: base(SystemErrorCodes.PrimaryKeyViolation, FormatMessage(tableName, constraintName, columnNames, deferrability)) {
			TableName = tableName;
			ConstraintName = constraintName;
			ColumnNames = columnNames;
			Deferrability = deferrability;
		}

		public ObjectName TableName { get; private set; }

		public string[] ColumnNames { get; private set; }

		public string ConstraintName { get; private set; }

		public ConstraintDeferrability Deferrability { get; private set; }

		private static string FormatMessage(ObjectName tableName, string constraintName, string[] columnNames, ConstraintDeferrability deferrability) {
			return String.Format("{0} PRIMARY KEY violation for constraint '{1}({2})' on table '{3}'.",
				deferrability.AsDebugString(), constraintName, String.Join(", ", columnNames), tableName);
		}
	}
}
