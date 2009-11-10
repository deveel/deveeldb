using System;
using System.Collections;

namespace Deveel.Data.DbModel {
	public sealed class DbSchema : DbObject, IEnumerable {
		public DbSchema(string name) 
			: base(name, name, DbObjectType.Schema) {
			objects = new ArrayList();
		}

		private readonly ArrayList objects;

		public IList Objects {
			get { return (IList) objects.Clone(); }
		}

		public override string FullName {
			get { return Name; }
		}

		public DbObject this[string name] {
			get {
				int index = FindObjectIndex(name);
				return (index == -1 ? null : (DbObject) objects[index]);
			}
		}

		private int FindObjectIndex(string name) {
			for (int i = 0; i < objects.Count; i++) {
				DbObject obj = (DbObject) objects[i];
				if (obj.Name == name)
					return i;
			}

			return -1;
		}

		public void AddTable(DbTable table) {
			if (table == null)
				throw new ArgumentNullException("table");

			if (table.Schema != null && table.Schema != Name)
				throw new ArgumentException();

			if (Contains(table.Name))
				throw new ArgumentException();

			objects.Add(table);
		}

		public DbTable AddTable(string name, string type) {
			if (name == null)
				throw new ArgumentNullException("name");

			if (Contains(name))
				throw new ArgumentException();

			DbTable table = new DbTable(Schema, name, type);
			objects.Add(table);
			return table;
		}

		public bool Contains(string name) {
			return FindObjectIndex(name) != -1;
		}

		public IEnumerator GetEnumerator() {
			return objects.GetEnumerator();
		}
	}
}