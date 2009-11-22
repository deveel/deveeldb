using System;
using System.Collections;

namespace Deveel.Data.DbModel {
	public sealed class DbTable : DbObject, IDbGrantableObject {
		public DbTable(string schema, string name, string type) 
			: base(schema, name, DbObjectType.Table) {
			this.type = type;
			objects = new ArrayList();
			privileges = new ArrayList();
		}

		private readonly string type;
		private readonly ArrayList objects;
		private readonly ArrayList privileges;

		public string Type {
			get { return type; }
		}

		public DbObject this[string name] {
			get {
				int index = FindObjectIndex(name);
				return (index == -1 ? null : this[index]);
			}
		}

		public DbObject this[int index] {
			get { return objects[index] as DbObject; }
		}

		public IList Objects {
			get { return (IList) objects.Clone(); }
		}

		public IList Columns {
			get {
				ArrayList columns = new ArrayList();
				IList objList = Objects;
				for (int i = 0; i < objList.Count; i++) {
					DbColumn column = objList[i] as DbColumn;
					if (column != null)
						columns.Add(column);
				}

				return columns;
			}
		}

		public IList Constraints {
			get {
				ArrayList constraints = new ArrayList();
				IList objList = Objects;
				for (int i = 0; i < objList.Count; i++) {
					DbConstraint constraint = objList[i] as DbConstraint;
					if (constraint != null)
						constraints.Add(constraint);
				}

				return constraints;
			}
		}

		public DbColumn IdentityColumn {
			get {
				IList columns = Columns;
				for (int i = 0; i < columns.Count; i++) {
					DbColumn column = (DbColumn) columns[i];
					if (column.Identity)
						return column;
				}

				return null;
			}
		}

		private static int FindObjectIndex(IList list, string name) {
			for (int i = 0; i < list.Count; i++) {
				DbObject obj = (DbObject) list[i];
				if (obj.Name == null)
					continue;

				if (obj.Name == name)
					return i;
			}

			return -1;
		}

		private int FindObjectIndex(string name) {
			return FindObjectIndex(objects, name);
		}

		public bool ContainsObject(string name) {
			return FindObjectIndex(name) != -1;
		}

		public DbPrivilege AddPrivilege(string privilege, string grantor, string grantee, bool grantable) {
			DbPrivilege priv = new DbPrivilege(Schema, Name, privilege, grantor, grantee, grantable);
			privileges.Add(priv);
			return priv;
		}

		public void AddPrivilege(DbPrivilege privilege) {
			privileges.Add(privilege);
		}

		public bool HasColumn(string name) {
			return FindObjectIndex(Columns, name) != -1;
		}

		public bool HasConstraint(string name) {
			return FindObjectIndex(Constraints, name) != -1;
		}

		public DbConstraint GetNamedConstraint(string name) {
			IList constraints = Constraints;
			int index = FindObjectIndex(constraints, name);
			return (index == -1 ? null : constraints[index] as DbConstraint);
		}

		public DbConstraint[] GetConstraints(DbConstraintType constraintType) {
			IList constraints = Constraints;
			ArrayList result = new ArrayList();
			for (int i = 0; i < constraints.Count; i++) {
				DbConstraint constraint = (DbConstraint) constraints[i];
				if (constraint.ConstraintType == constraintType)
					result.Add(constraint);
			}

			return (DbConstraint[]) result.ToArray(typeof (DbConstraint));
		}

		public void AddColumn(DbColumn column) {
			if (column == null)
				throw new ArgumentNullException("column");

			string tableName = column.TableName;
			string schemaName = column.Schema;

			if (schemaName != null && schemaName != Schema)
				throw new ArgumentException();

			if (tableName != null && tableName != Name)
				throw new ArgumentException();

			if (tableName == null || schemaName == null) {
				DbColumn copyColumn = new DbColumn(Schema, Name, column.Name, column.DataType, column.Size);
				copyColumn.Scale = column.Scale;
				copyColumn.Default = column.Default;
				foreach (DbPrivilege privilege in column.Privileges)
					copyColumn.AddPrivilege(privilege);
				column = copyColumn;
			}

			if (ContainsObject(column.Name))
				throw new ArgumentException();

			objects.Add(column);
		}

		public DbColumn AddColumn(string name, DbDataType dataType) {
			if (name == null)
				throw new ArgumentNullException("name");

			DbColumn column = new DbColumn(Schema, Name, name, dataType);
			objects.Add(column);
			return column;
		}

		public void AddConstraint(DbConstraint constraint) {
			if (constraint == null)
				throw new ArgumentNullException("constraint");

			string tableName = constraint.Table;
			string schemaName = constraint.Schema;

			if (schemaName != null && schemaName != Schema)
				throw new ArgumentException();

			if (tableName != null && tableName != Name)
				throw new ArgumentException();

			if (tableName == null || schemaName == null) {
				//TODO:
			}

			objects.Add(constraint);
		}

		public DbConstraint AddConstraint(string name, DbConstraintType constraintType) {
			if (name != null && name.Length > 0 && HasConstraint(name))
				throw new ArgumentException();

			DbConstraint constraint;
			switch (constraintType) {
				case DbConstraintType.PrimaryKey:
					constraint = new DbPrimaryKey(Schema, Name, name);
					break;
				case DbConstraintType.ForeignKey:
					constraint = new DbForeignKey(Schema, Name, name);
					break;
				case DbConstraintType.Unique:
					constraint = new DbUniqueConstraint(Schema, Name, name);
					break;
				case DbConstraintType.Check:
					constraint = new DbCheckConstraint(Schema, Name);
					break;
				default:
					throw new ArgumentException();
			}

			objects.Add(constraint);
			return constraint;
		}

		public DbColumn GetColumn(string columnName) {
			int index = FindObjectIndex(columnName);
			return (index == -1 ? null : (DbColumn) objects[index]);
		}
	}
}