using System;
using System.Reflection;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Design.Configuration {
	public sealed class MemberModelConfiguration {
		internal MemberModelConfiguration(TypeModelConfiguration typeModel, MemberInfo member) {
			TypeModel = typeModel;
			Member = member;
		}

		public MemberInfo Member { get; private set; }

		public Type MemberType {
			get {
				if (Member is PropertyInfo)
					return ((PropertyInfo) Member).PropertyType;
				if (Member is FieldInfo)
					return ((FieldInfo) Member).FieldType;

				throw new InvalidOperationException();
			}
		}

		public TypeModelConfiguration TypeModel { get; private set; }

		public string ColumnName { get; set; }

		public SqlType ColumnType { get; set; }

		public bool NotNull { get; set; }

		public SqlExpression DefaultExpression { get; set; }

		internal MemberModelConfiguration Clone(TypeModelConfiguration type) {
			return new MemberModelConfiguration(type, Member) {
				ColumnName = ColumnName,
				ColumnType = ColumnType,
				NotNull = NotNull,
				DefaultExpression = DefaultExpression
			};
		}
	}
}
