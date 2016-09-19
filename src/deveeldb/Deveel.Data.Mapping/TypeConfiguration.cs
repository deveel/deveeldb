using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Mapping {
	public class TypeConfiguration<TType> : ITypeConfigurationProvider<TType> {
		private string tableName;
		private IDictionary<string, IMemberConfigurationProvider> members;

		string ITypeConfigurationProvider<TType>.TableName {
			get { return tableName; }
		}

		IEnumerable<IMemberConfigurationProvider> ITypeConfigurationProvider<TType>.MemberConfigurations {
			get { return members == null ? new IMemberConfigurationProvider[0] : members.Values.AsEnumerable(); }
		}

		ITypeConfiguration<TType> ITypeConfiguration<TType>.HasTableName(string value) {
			return HasTableName(value);
		}

		IMemberConfiguration ITypeConfiguration<TType>.Member<TMember>(Expression<Func<TType, TMember>> selector) {
			throw new NotImplementedException();
		}

		public TypeConfiguration<TType> HasTableName(string value) {
			tableName = value;
			return this;
		}

		private MemberInfo FindMember<TMember>(Expression<Func<TType, TMember>> selector) {
			var type = typeof(TType);

			MemberExpression member = selector.Body as MemberExpression;
			if (member == null)
				throw new ArgumentException(string.Format(
					"Expression '{0}' refers to a method, not a property.",
					selector.ToString()));

			var memberInfo = member.Member;

			if (type != memberInfo.ReflectedType &&
			    !type.IsSubclassOf(memberInfo.ReflectedType))
				throw new ArgumentException(string.Format(
					"Expresion '{0}' refers to a property that is not from type {1}.",
					selector.ToString(),
					type));

			return memberInfo;
		}

		public IMemberConfiguration Member<TMember>(Expression<Func<TType, TMember>> selector) {
			var memberInfo = FindMember(selector);

			if (members == null)
				members = new Dictionary<string, IMemberConfigurationProvider>();

			var config = new MemberConfiguration(memberInfo);
			members[memberInfo.Name] = config;
			return config;
		}

		#region MemberConfiguration

		class MemberConfiguration : IMemberConfigurationProvider {
			public MemberConfiguration(MemberInfo member) {
				Member = member;
				DiscoverAttributes();
			}

			public MemberInfo Member { get; private set; }

			private void AssertIsNotIgnored() {
				if (IsIgnored)
					throw new InvalidOperationException(String.Format("Member '{0}' in type '{1}' is ignored.", Member.Name,
						Member.ReflectedType));
			}

			private void DiscoverAttributes() {
				var attrs = Member.GetCustomAttributes(true);
				foreach (var attribute in attrs) {
					if (attribute is IgnoreAttribute) {
						IsIgnored = true;
					} else if (attribute is ColumnAttribute) {
						AssertIsNotIgnored();

						var column = (ColumnAttribute) attribute;
						if (!String.IsNullOrEmpty(column.Name))
							ColumnName = column.Name;

						if (!String.IsNullOrEmpty(column.TypeName))
							ColumnType = SqlType.Parse(column.TypeName);

						if (column.Default != null) {
							SqlExpression defaultExpr;
							if (column.DefaultIsExpression) {
								if (!(column.Default is string))
									throw new InvalidOperationException();

								defaultExpr = SqlExpression.Parse((string) column.Default);
							} else {
								var value = Field.Create(column.Default);
								defaultExpr = SqlExpression.Constant(value);
							}

							DefaultExpression = defaultExpr;
						}

						IsNotNull = !column.Null;
					} else if (attribute is ConstraintAttribute) {
						var constraint = (ConstraintAttribute) attribute;

						if (constraint.Type == ConstraintType.PrimaryKey) {
							IsPrimaryKey = true;
						} else if (constraint.Type == ConstraintType.Unique) {
							IsUnique = true;
						} else {
							throw new NotImplementedException();
						}
					} else if (attribute is IdentityAttribute) {
						// TODO:
						throw new NotImplementedException();
					}
				}
			}

			public IMemberConfiguration HasColumnName(string value) {
				if (String.IsNullOrEmpty(value))
					throw new ArgumentNullException("value");

				ColumnName = value;
				return this;
			}

			public IMemberConfiguration HasColumnType(SqlType value) {
				throw new NotImplementedException();
			}

			public IMemberConfiguration NotNull(bool value = true) {
				throw new NotImplementedException();
			}

			public IMemberConfiguration PrimaryKey(bool value = true) {
				throw new NotImplementedException();
			}

			public IMemberConfiguration Unique(bool value = true) {
				throw new NotImplementedException();
			}

			public IMemberConfiguration HasDefault(SqlExpression defaultExpression) {
				throw new NotImplementedException();
			}

			public IMemberConfiguration Ignore(bool value = true) {
				throw new NotImplementedException();
			}

			public string ColumnName { get; private set; }

			public SqlType ColumnType { get; private set; }

			public bool IsNotNull { get; private set; }

			public bool IsUnique { get; private set; }

			public bool IsPrimaryKey { get; private set; }

			public SqlExpression DefaultExpression { get; private set; }

			public bool IsIgnored { get; private set; }
		}

		#endregion
	}
}
