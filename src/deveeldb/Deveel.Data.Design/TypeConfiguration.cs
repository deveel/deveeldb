using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Design {
	public class TypeConfiguration<TType> : ITypeConfigurationProvider where TType : class {
		private string tableName;
		private IDictionary<string, IMemberConfigurationProvider> members;

		string ITypeConfigurationProvider.TableName {
			get { return tableName; }
		}

		Type ITypeConfigurationProvider.Type {
			get { return typeof(TType); }
		}

		IEnumerable<IMemberConfigurationProvider> ITypeConfigurationProvider.MemberConfigurations {
			get { return members == null ? new IMemberConfigurationProvider[0] : members.Values.AsEnumerable(); }
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
					"Expression '{0}' refers to a property that is not from type {1}.", selector, type));

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

						if (!String.IsNullOrEmpty(column.Type)) {
							ColumnType = SqlType.Parse(column.Type);
						} else if (!String.IsNullOrEmpty(column.TypeName)) {
							ColumnType = PrimitiveTypes.Resolve(column.TypeName, new [] {
								new DataTypeMeta("Precision", column.Precision.ToString()),
								new DataTypeMeta("Scale", column.Scale.ToString()),
								new DataTypeMeta("MaxSize", column.Size.ToString()),   
							});
						}
					} else if (attribute is ColumnConstraintAttribute) {
						var constraint = (ColumnConstraintAttribute) attribute;

						if (constraint.ConstraintType == ColumnConstraintType.PrimaryKey) {
							IsPrimaryKey = true;
						} else if (constraint.ConstraintType == ColumnConstraintType.Unique) {
							IsUnique = true;
						} else if (constraint.ConstraintType == ColumnConstraintType.NotNull) {
							IsNotNull = true;
						}
					} else if (attribute is IdentityAttribute) {
						// TODO:
						throw new NotImplementedException();
					} else if (attribute is DefaultAttribute) {
						var columnDefault = (DefaultAttribute) attribute;
						if (columnDefault.DefaultType == ColumnDefaultType.Constant) {
							var defaultValue = Field.Create(columnDefault.Value);
							DefaultExpression = SqlExpression.Constant(defaultValue);
						} else {
							DefaultExpression = SqlExpression.Parse((string) columnDefault.Value);
						}
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
				if (value == null)
					throw new ArgumentNullException("value");

				ColumnType = value;
				return this;
			}

			public IMemberConfiguration NotNull(bool value = true) {
				IsNotNull = value;
				return this;
			}

			public IMemberConfiguration PrimaryKey(bool value = true) {
				IsPrimaryKey = value;
				return this;
			}

			public IMemberConfiguration Unique(bool value = true) {
				IsUnique = value;
				return this;
			}

			public IMemberConfiguration HasDefault(SqlExpression defaultExpression) {
				DefaultExpression = defaultExpression;
				return this;
			}

			public IMemberConfiguration Ignore(bool value = true) {
				IsIgnored = value;
				return this;
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
