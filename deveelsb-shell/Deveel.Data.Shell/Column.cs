using System;
using System.Collections;

namespace Deveel.Data.Shell {
	public sealed class Column : IComparable {
		private readonly Table table;
		private readonly String name;
		private readonly int position; // starting at 1
		private readonly string type;
		private readonly int size;
		private readonly int scale;
		private readonly bool nullable;
		private string defaultValue;
		private readonly ArrayList privileges;

		internal Column(Table table, string name, int position, string type, int size, int scale, bool nullable) {
			this.table = table;
			this.name = name;
			this.position = position;
			this.type = type;
			this.size = size;
			this.scale = scale;
			this.nullable = nullable;
			privileges = new ArrayList();
		}

		public int Scale {
			get { return scale; }
		}

		public string Name {
			get { return name; }
		}

		public string Default {
			get { return defaultValue; }
		}

		public string Type {
			get { return type; }
		}

		internal void SetDefault(String value) {
			defaultValue = value;
		}

		public int Size {
			get { return size; }
		}

		public bool IsNullable {
			get { return nullable; }
		}

		public int Position {
			get { return position; }
		}

		public bool IsPartOfPrimaryKey {
			get {
				PrimaryKey primaryKey = table.PrimaryKey;
				return (primaryKey == null ? false : primaryKey.HasColumn(name));
			}
		}

		public ICollection Privileges {
			get { return privileges;}
		}

		internal void AddPrivilege(Privilege privilege) {
			privileges.Add(privilege);
		}

		/* 
		 * Compares both <code>Column</code>s according to their position.
		 * @see java.lang.Comparable#compareTo(java.lang.Object)
		 */
		public int CompareTo(Object o) {
			int result = 1;
			Column other = (Column)o;
			if (other.Position < position)
				result = -1;
			else if (other.Position == position)
				result = 0;
			return result;
		}

		/**
		 * 
		 * @param o
		 * @param colNameIgnoreCase  specifies if column names shall be compared in a case insensitive way.
		 * @return if the columns are equal
		 */
		public bool Equals(Object o, bool colNameIgnoreCase) {
			if (o is Column) {
				Column other = (Column)o;

				if (size != other.size)
					return false;

				// ignore the position, it's not important
				/*
				if (position != other.position)
					return false;
				*/

				if (nullable != other.nullable)
					return false;

				if ((name == null && other.name != null)
				   || (name != null
						&& (colNameIgnoreCase && String.Compare(name, other.name, true) != 0
						|| !colNameIgnoreCase && !name.Equals(other.name)
							  )
					  )
				   )
					return false;

				if ((type == null && other.type != null)
				   || (type != null && !type.Equals(other.type)))
					return false;

				if ((defaultValue == null && other.defaultValue != null)
				   || (defaultValue != null && !defaultValue.Equals(other.defaultValue)))
					return false;

			}
			return true;
		}

	}
}