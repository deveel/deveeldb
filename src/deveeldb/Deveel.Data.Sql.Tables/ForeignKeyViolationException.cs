using System;

namespace Deveel.Data.Sql.Tables {
	public sealed class ForeignKeyViolationException : ConstraintViolationException {
		internal ForeignKeyViolationException(ObjectName tableName, string constraintName, string[] columnNames,
			ObjectName refTableName, string[] refColumnNames, ConstraintDeferrability deferrability)
			: base(SystemErrorCodes.ForeignKeyViolation, FormatMessage(tableName, constraintName, columnNames, refTableName, refColumnNames, deferrability)) {
			TableName = tableName;
			ConstraintName = constraintName;
			ColumnNames = columnNames;
			LinkedTableName = refTableName;
			LinkedColumnNames = refColumnNames;
			Deferrability = deferrability;
		}

		public ObjectName TableName { get; private set; }

		public string ConstraintName { get; private set; }

		public string[] ColumnNames { get; private set; }

		public ObjectName LinkedTableName { get; private set; }

		public string[] LinkedColumnNames { get; private set; }

		public ConstraintDeferrability Deferrability { get; private set; }

		private static string FormatMessage(ObjectName tableName, string constraintName, string[] columnNames, ObjectName refTableName, string[] refColumnNames, ConstraintDeferrability deferrability) {
			return String.Format("{0} FOREIGN KEY violation for constraint '{1}({2})' referencing '{3}({4})'",
				deferrability.AsDebugString(), constraintName, String.Join(", ", columnNames), refTableName,
				String.Join(", ", refColumnNames));
		}
	}
}
