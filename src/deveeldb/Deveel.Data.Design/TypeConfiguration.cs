using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Design {
	public class TypeConfiguration<TType> : ITypeConfigurationProvider where TType : class {
		private string tableName;
		private IDictionary<string, IMemberConfigurationProvider> members;
		private IList<IAssociationConfigurationProvider> associations;
		private SqlExpression checkExpression;

		public TypeConfiguration() {
			DiscoverAttributes();
			DiscoverMembers();
		}

		string ITypeConfigurationProvider.TableName {
			get { return tableName; }
		}

		Type ITypeConfigurationProvider.Type {
			get { return typeof(TType); }
		}

		SqlExpression ITypeConfigurationProvider.Check {
			get { return checkExpression; }
		}

		IEnumerable<IMemberConfigurationProvider> ITypeConfigurationProvider.MemberConfigurations {
			get { return members == null ? new IMemberConfigurationProvider[0] : members.Values.AsEnumerable(); }
		}

		private void DiscoverAttributes() {
			var attributes = typeof(TType).GetCustomAttributes(false);
			foreach (var attribute in attributes) {
				if (attribute is TableNameAttribute) {
					var tableNameAttr = (TableNameAttribute) attribute;

					var sb = new StringBuilder();
					if (!String.IsNullOrEmpty(tableNameAttr.Schema))
						sb.Append(tableNameAttr.Schema).Append(".");

					sb.Append(tableNameAttr.Name);

					tableName = sb.ToString();
				} else if (attribute is CheckAttribute) {
					var checkAttr = (CheckAttribute) attribute;
					checkExpression = SqlExpression.Parse(checkAttr.Expression);
				}
			}
		}

		public TypeConfiguration<TType> HasTableName(string value) {
			tableName = value;
			return this;
		}

		public TypeConfiguration<TType> Check(SqlExpression expression) {
			checkExpression = expression;
			return this;
		}

		private static MemberInfo FindMember<TEntity, TMember>(Expression<Func<TEntity, TMember>> selector) {
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

		public IRequiredAssociationConfiguration<TType, TTarget> Require<TTarget>(Expression<Func<TType, TTarget>> selector) where TTarget : class {
			var member = FindMember(selector);
			var association = new RequiredAssociationConfiguration<TTarget>(member);
			if (associations == null)
				associations = new List<IAssociationConfigurationProvider>();

			associations.Add(association);

			return association;
		}

		private void DiscoverMembers() {
			var typeMembers = typeof(TType).GetMembers(BindingFlags.Instance | BindingFlags.Public)
				.Where(x => x is FieldInfo || x is PropertyInfo);

			foreach (var typeMember in typeMembers) {
				if (typeMember is PropertyInfo &&
					((PropertyInfo)typeMember).IsSpecialName)
					continue;

				if (members == null)
					members = new Dictionary<string, IMemberConfigurationProvider>();

				members[typeMember.Name] = new MemberConfiguration(typeMember);
			}
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

		#region AssociationConfiguration<TType, TTarget>

		abstract class AssociationConfiguration<TTarget> : IAssociationConfiguration<TType, TTarget>, IAssociationConfigurationProvider where TTarget : class {
			protected AssociationConfiguration(MemberInfo sourceMember) {
				SourceMember = sourceMember;
			}

			public abstract AssociationType AssociationType { get; }

			public abstract AssociationCardinality Cardinality { get; }

			public Type SourceType {
				get { return typeof(TType); }
			}

			public Type OtherType {
				get { return typeof(TTarget); }
			}

			public MemberInfo SourceMember { get; private set; }

			public MemberInfo OtherMember { get; protected set; }

			public MemberInfo KeyMember { get; protected set; }

			public bool Cascade { get; protected set; }
		}

		#endregion

		#region RequiredAssociationConfiguration

		class RequiredAssociationConfiguration<TTarget> : AssociationConfiguration<TTarget>,
			IRequiredAssociationConfiguration<TType, TTarget> where TTarget : class {
			public RequiredAssociationConfiguration(MemberInfo sourceMember) 
				: base(sourceMember) {
			}

			public override AssociationType AssociationType {
				get { return AssociationType.Destination; }
			}

			public override AssociationCardinality Cardinality {
				get { return AssociationCardinality.ManyToOne; }
			}

			public void SetKeyMember(MemberInfo keyMember) {
				KeyMember = keyMember;
			}

			public void SetCascaseOnDelete(bool value) {
				Cascade = value;
			}

			public IDependantAssociationConfiguration<TType, TTarget> WithMany(Expression<Func<TTarget, ICollection<TType>>> selector) {
				var otherMember = FindMember(selector);
				if (otherMember == null)
					throw new InvalidOperationException();

				OtherMember = otherMember;
				return new DependantAssociationConfiguration<TTarget>(this);
			}

			public IForeignKeyAssociationConfiguration<TType, TTarget> WithOptional(Expression<Func<TTarget, TType>> selector) {
				throw new NotImplementedException();
			}
		}

		#endregion

		#region DependantAssociationConfiguration

		class DependantAssociationConfiguration<TTarget> : IDependantAssociationConfiguration<TType, TTarget>, ICascableAssociationConfiguration where TTarget : class {
			public DependantAssociationConfiguration(RequiredAssociationConfiguration<TTarget> parent) {
				Association = parent;
			}

			public RequiredAssociationConfiguration<TTarget> Association { get; private set; }

			public ICascableAssociationConfiguration HasForeignKey<TKey>(Expression<Func<TType, TKey>> selector) {
				var member = FindMember(selector);
				Association.SetKeyMember(member);
				return this;
			}

			public void CascadeOnDelete(bool value = true) {
				Association.SetCascaseOnDelete(value);
			}
		}

		#endregion
	}
}
