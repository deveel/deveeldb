using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Tables {
	public sealed class CheckViolationException : ConstraintViolationException {
		internal CheckViolationException(ObjectName tableName, string constraintName, SqlExpression expression,
			ConstraintDeferrability deferrability)
			: base(SystemErrorCodes.CheckViolation, FormatMessage(tableName, constraintName, expression, deferrability)) {
			TableName = tableName;
			ConstraintName = constraintName;
			CheckExpression = expression;
			Deferrability = deferrability;
		}

		public ObjectName TableName { get; private set; }

		public string ConstraintName { get; private set; }

		public SqlExpression CheckExpression { get; private set; }

		public ConstraintDeferrability Deferrability { get; private set; }

		private static string FormatMessage(ObjectName tableName, string constraintName, SqlExpression expression, ConstraintDeferrability deferrability) {
			return String.Format("{0} CHECK violation for constraint '{1}' ({2}) on table '{3}'", deferrability, constraintName, expression, tableName);
		}
	}
}
