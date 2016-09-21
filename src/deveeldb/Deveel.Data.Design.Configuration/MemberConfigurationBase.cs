using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Design.Configuration {
	public class MemberConfiguration<TType> {
		internal MemberConfiguration(MemberModelConfiguration configuration) {
			Configuration = configuration;
		}

		internal MemberModelConfiguration Configuration { get; private set; }

		protected virtual SqlType BuildSqlType() {
			return PrimitiveTypes.FromType(typeof(TType));
		}

		public MemberConfiguration<TType> HasColumnName(string value) {
			Configuration.ColumnName = value;
			return this;
		}

		public MemberConfiguration<TType> Ignore(bool value = true) {
			if (value)
				Configuration.TypeModel.IgnoreMember(Configuration.Member.Name);

			return this;
		}

		public MemberConfiguration<TType> NotNull(bool value = true) {
			Configuration.NotNull = value;
			return this;
		}

		private void AddToConstraint(ConstraintType constraintType) {
			var constraint = Configuration.TypeModel.GetConstraint(constraintType);
			constraint.AddMember(Configuration.Member.Name);
		}

		public MemberConfiguration<TType> PrimaryKey(bool value = true) {
			if (value)
				AddToConstraint(ConstraintType.PrimaryKey);

			return this;
		}

		public MemberConfiguration<TType> Unique(bool value = true) {
			if (value)
				AddToConstraint(ConstraintType.Unique);

			return this;
		}

		public MemberConfiguration<TType> HasDefault(SqlExpression expression) {
			Configuration.DefaultExpression = expression;
			return this;
		}
	}
}
