using System;
using System.Collections;

namespace Deveel.Data.DbModel {
	public sealed class DbDatabase : DbObject {
		public DbDatabase(string name)
			: base(null, name, DbObjectType.Database) {
			schemata = new ArrayList();
		}

		private readonly ArrayList schemata;

		public DbSchema this[int index] {
			get { return schemata[index] as DbSchema; }
		}

		public DbSchema this[string schemaName] {
			get {
				int index = FindSchemaIndex(schemaName);
				return (index == -1 ? null : this[index]);
			}
		}

		private int FindSchemaIndex(string schemaName) {
			for (int i = 0; i < schemata.Count; i++) {
				DbSchema schema = (DbSchema) schemata[i];
				if (schema.Name == schemaName)
					return i;
			}

			return -1;
		}

		public bool ContainsSchema(string schemaName) {
			return FindSchemaIndex(schemaName) != -1;
		}

		public void AddSchema(DbSchema schema) {
			if (schema == null)
				throw new ArgumentNullException("schema");

			if (ContainsSchema(schema.Name))
				throw new ArgumentException();

			schemata.Add(schema);
		}

		public DbSchema AddSchema(string name) {
			if (name == null || name.Length == 0)
				throw new ArgumentNullException("name");

			if (ContainsSchema(name))
				throw new ArgumentException();

			DbSchema schema = new DbSchema(name);
			schemata.Add(schema);
			return schema;
		}
	}
}