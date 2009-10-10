using System;
using System.Collections;

namespace Deveel.Data.Shell {
	public sealed class Table : IComparable {

		private String _name;
		private Hashtable /*<String, Column>*/ _columns;

		// private PrimaryKey _pk;

		// FIXME: add notion of schema.

		public Table(String name) {
			_name = name;
			_columns = new Hashtable();
		}

		public String getName() {
			return _name;
		}

		public void setName(String s) {
			_name = s;
		}

		public void addColumn(Column column) {
			_columns.Add(column.getName(), column);
		}

		public IEnumerator getColumnIterator() {
			IEnumerator result = null;
			if (_columns != null) {
				result = _columns.Values.GetEnumerator();
			}
			return result;
		}

		public Column getColumnByName(String name, bool ignoreCase) {
			Column result = null;
			if (_columns != null) {
				result = (Column)_columns[name];
				if (result == null && ignoreCase) {
					IEnumerator iter = _columns.Keys.GetEnumerator();
					while (iter.MoveNext()) {
						String colName = (String)iter.Current;
						if (String.Compare(colName, name, true) == 0) {
							result = (Column)_columns[colName];
							break;
						}
					}
				}
			}
			return result;
		}

		/**
		 * @return <code>true</code>, if this <code>Table</code> has any foreign key, otherwise <code>false</code>.
		 */
		public bool hasForeignKeys() {
			return getForeignKeys() != null;
		}

		/**
		 * @return A <code>Set</code> of <code>ColumnFkInfo</code> objects or <code>null</code>.
		 */
		public IList/*<ColumnFkInfo>*/ getForeignKeys() {
			ArrayList result = null;

			if (_columns != null) {
				IEnumerator iter = _columns.Values.GetEnumerator();
				while (iter.MoveNext()) {
					Column c = (Column)iter.Current;
					if (c.getFkInfo() != null) {
						if (result == null)
							result = new ArrayList();
						result.Add(c.getFkInfo());
					}
				}
			}

			return result;
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		public override String ToString() {
			return _name;
		}

		public override int GetHashCode() {
			return _name.GetHashCode();
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		public override bool Equals(Object other) {
			bool result = false;

			if (other == this)
				result = true;

			else if (other is Table) {
				Table o = (Table)other;

				if (_name != null && _name.Equals(o.getName()))
					result = true;

				else if (_name == null && o.getName() == null)
					result = true;
			}

			return result;
		}

		/* (non-Javadoc)
		 * @see java.lang.Comparable#compareTo(java.lang.Object)
		 */
		public int CompareTo(Object other) {
			int result = 0;
			if (other is Table) {
				Table o = (Table)other;
				result = _name.CompareTo(o.getName());
			}
			return result;
		}

		/*
		public boolean columnIsPartOfPk(String column) {
			boolean result = false;
			if (_pk != null) {
				result = _pk.columnParticipates(column);
			}
			return result;
		}
		*/

		/**
		 * @return
		 */
		/*
		public PrimaryKey getPk() {
			return _pk;
		}
		*/

		/**
		 * @param key
		 */
		/*
		public void setPk(PrimaryKey key) {
			_pk = key;
		}
		*/

	}
}