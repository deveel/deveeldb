using System;
using System.Reflection;

using Deveel.Data.Design.Configuration;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Design {
	public sealed class DbMemberInfo {
		internal DbMemberInfo(MemberModelConfiguration configuration) {
			Configuration = configuration;
		}

		private MemberModelConfiguration Configuration { get; set; }

		public MemberInfo Member {
			get { return Configuration.Member; }
		}

		public Type MemberType {
			get { return Configuration.MemberType; }
		}

		public DbTypeInfo TypeInfo {
			get { return new DbTypeInfo(Configuration.TypeModel); }
		}

		public string ColumnName {
			get { return Configuration.ColumnName; }
		}

		public string FullColumnName {
			get { return String.Format("{0}.{1}", TypeInfo.TableName, ColumnName); }
		}

		public SqlType ColumnType {
			get { return Configuration.ColumnType; }
		}

		public bool NotNull {
			get { return Configuration.NotNull; }
		}

		public bool IsKey {
			get { return Configuration.TypeModel.IsMemberOfConstraint(null, ConstraintType.PrimaryKey, Member.Name); }
		}

		public bool IsUnique {
			get { return Configuration.TypeModel.IsMemberOfAnyConstraint(ConstraintType.Unique, Member.Name); }
		}

		internal void ApplyFromRow(object obj, Row row) {
			if (row == null)
				throw new ArgumentNullException("row");

			if (String.IsNullOrEmpty(ColumnName))
				throw new InvalidOperationException(String.Format("No column name was set for the member {0} in type '{1}'.",
					Member.Name, TypeInfo.Type));

			var columnOffset = row.Table.TableInfo.IndexOfColumn(ColumnName);
			if (columnOffset == -1)
				throw new InvalidOperationException(
					String.Format(
						"The member '{0}' of type '{1}' is mapped to the column '{2}' of table '{3}' not found in selected row structure.",
						Member.Name, TypeInfo.Type, ColumnName, TypeInfo.TableName));

			var columnValue = row.GetValue(columnOffset);

			if (Field.IsNullField(columnValue)) {
				if (IsKey)
					throw new InvalidOperationException(
						String.Format("The member '{0}' of type '{1}' is the PRIMARY KEY of the table but the selected field is NULL",
							Member.Name,
							TypeInfo.Type));

				if (NotNull)
					throw new InvalidOperationException(
						String.Format("The member '{0}' of type '{1}' is marked as NOT NULL but the selected field is NULL", Member.Name,
							TypeInfo.Type));
			}

			var finalValue = columnValue.CastTo(ColumnType);
			var value = finalValue.ConvertTo(MemberType);

			Apply(obj, value);
		}

		internal void Apply(object obj, object value) {
			if (obj == null)
				throw new ArgumentNullException("obj");
			if (value == null && NotNull)
				throw new InvalidOperationException(
					String.Format("The member '{0}' of type '{1}' is marked as NOT NULL but the selected field is NULL", Member.Name,
						TypeInfo.Type));

			if (Member is PropertyInfo) {
				((PropertyInfo) Member).SetValue(obj, value, null);
			} else if (Member is FieldInfo) {
				((FieldInfo) Member).SetValue(obj, value);
			}
		}
	}
}