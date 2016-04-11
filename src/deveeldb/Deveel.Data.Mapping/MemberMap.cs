using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Mapping {
	public sealed class MemberMap<TType> : IMemberMap where TType : class {
		internal MemberMap(MemberInfo member) {
			Member = member;

			if (member is PropertyInfo) {
				MemberType = ((PropertyInfo)member).PropertyType;
			} else if (member is FieldInfo) {
				MemberType = ((FieldInfo)member).FieldType;
			} else {
				throw new ArgumentException(String.Format("Member of type '{0}' is not permitted.", member.GetType()));
			}


			Constraints = new List<ConstraintType>();
		}

		private TypeMap<TType> TypeMap { get; set; }

		private MemberInfo Member { get; set; }

		private Type MemberType { get; set; }

		string IMemberMap.MemberName {
			get { return Member.Name; }
		}

		private string ColumnName { get; set; }

		private SqlType ColumnType { get; set; }

		private bool IsNotNull { get; set; }

		private bool IsIdentity { get; set; }

		private bool IsUnique { get; set; }

		private IList<ConstraintType> Constraints { get; set; }

		private object DefaultExpression { get; set; }

		private bool DefaultIsExpression { get; set; }

		public MemberMap<TType> Name(string columnName) {
			ColumnName = columnName;
			return this;
		}

		public MemberMap<TType> Type(SqlType type) {
			ColumnType = type;
			return this;
		}

		public MemberMap<TType> NotNull(bool value = true) {
			IsNotNull = value;
			return this;
		}

		public MemberMap<TType> Identity(bool value = true) {
			IsIdentity = value;
			return this;
		}

		public MemberMap<TType> Unique(bool value = true) {
			IsUnique = value;
			return this;
		}

		public MemberMap<TType> Default(SqlExpression expression) {
			DefaultExpression = expression;
			DefaultIsExpression = true;
			return this;
		}

		public MemberMap<TType> Default(string value, bool asSql = true) {
			if (asSql) {
				Default(SqlExpression.Parse(value));
			} else {
				DefaultExpression = value;
				DefaultIsExpression = false;
			}

			return this;
		}

		public MemberMap<TType> Default(object value) {
			if (value is SqlExpression) {
				Default((SqlExpression) value);
			} else {
				DefaultExpression = value;
				DefaultIsExpression = false;
			}

			return this;
		}

		MemberMapInfo IMemberMap.GetMapInfo(TypeMapInfo typeInfo) {
			ConstraintMapInfo[] constraints = null;
			object defaultExp = DefaultExpression;
			bool defaultIsExp = DefaultIsExpression;

			if (IsIdentity) {
				constraints = new[] {
					new ConstraintMapInfo(Member, ColumnName, ConstraintType.PrimaryKey, null) 
				};

				defaultExp = SqlExpression.FunctionCall("UNIQUEKEY", new SqlExpression[] {SqlExpression.Constant(typeInfo.TableName.FullName)});
				defaultIsExp = true;
			} else if (Constraints.Count > 0) {
				constraints = Constraints.Select(x => new ConstraintMapInfo(Member, ColumnName, x, null)).ToArray();
			}

			var mapInfo = new MemberMapInfo(Member, ColumnName, ColumnType, !IsNotNull, constraints);
			mapInfo.SetDefault(defaultExp, defaultIsExp);

			return mapInfo;
		}
	}
}
