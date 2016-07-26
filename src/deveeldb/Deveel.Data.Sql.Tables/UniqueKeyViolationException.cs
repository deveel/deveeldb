using System;

namespace Deveel.Data.Sql.Tables {
	public sealed class UniqueKeyViolationException : ConstraintViolationException {

		internal UniqueKeyViolationException(ObjectName tableName, string constraintName, string[] columnNames,
			ConstraintDeferrability deferrability)
			: base(SystemErrorCodes.UniqueKeyViolation, FormatMessage(tableName, constraintName, columnNames, deferrability)) {
			TableName = tableName;
			ConstraintName = constraintName;
			ColumnNames = columnNames;
			Deferrability = deferrability;
		}

		private static string FormatMessage(ObjectName tableName, string constraintName, string[] columnNames, ConstraintDeferrability deferrability) {
			return String.Format("{0} UNIQUE KEY violation for constraint '{1}({2})' on table '{3}'.",
	deferrability.AsDebugString(), constraintName, String.Join(", ", columnNames), tableName);

		}

		public ObjectName TableName { get; private set; }

		public string ConstraintName { get; private set; }

		public string[] ColumnNames { get; private set; }

		public ConstraintDeferrability Deferrability { get; private set; }
	}
}
