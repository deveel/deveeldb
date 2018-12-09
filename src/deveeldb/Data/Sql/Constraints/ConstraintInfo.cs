using System;

namespace Deveel.Data.Sql.Constraints {
	public abstract class ConstraintInfo : IDbObjectInfo, ISqlFormattable {
		protected ConstraintInfo(ObjectName tableName, string constraintName) {
			TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
			ConstraintName = constraintName ?? throw new ArgumentNullException(nameof(constraintName));
			Deferrability = ConstraintDeferrability.InitiallyImmediate;
		}

		DbObjectType IDbObjectInfo.ObjectType => DbObjectType.Constraint;

		public string ConstraintName { get; }

		public ObjectName TableName { get; }

		public ConstraintDeferrability Deferrability { get; set; }

		public ObjectName FullName => new ObjectName(TableName, ConstraintName);

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			AppendTo(builder);
		}

		protected virtual void AppendTo(SqlStringBuilder builder) {

		}
	}
}