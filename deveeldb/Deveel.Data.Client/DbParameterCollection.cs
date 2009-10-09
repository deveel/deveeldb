//  
//  DbParameterCollection.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;
using System.Data;

namespace Deveel.Data.Client {
	public sealed class DbParameterCollection : IDataParameterCollection {
		internal DbParameterCollection() {
			list = new ArrayList();
		}

		private readonly ArrayList list;

		#region Implementation of IEnumerable

		public IEnumerator GetEnumerator() {
			return list.GetEnumerator();
		}

		#endregion

		#region Implementation of ICollection

		void ICollection.CopyTo(Array array, int index) {
			list.CopyTo(array, index);
		}

		public int Count {
			get { return list.Count; }
		}

		object ICollection.SyncRoot {
			get { return list; }
		}

		bool ICollection.IsSynchronized {
			get { return list.IsSynchronized; }
		}

		#endregion

		public DbParameter this[int index] {
			get { return list[index] as DbParameter; }
			set { list[index] = value; }
		}

		public int Add(DbParameter parameter) {
			int i = list.Add(parameter);
			parameter.index = i;
			return i;
		}

		public DbParameter Add(object value) {
			DbParameter parameter;
			if (value is DbParameter) {
				parameter = (DbParameter) value;
			} else {
				parameter = new DbParameter(value);
			}

			Add(parameter);
			return parameter;
		}

		public DbParameter Add(object value, int size) {
			return Add((object)(new DbParameter(value)));
		}

		public DbParameter Add(object value, int size, byte scale) {
			DbParameter parameter = Add(value, size);
			parameter.Scale = scale;
			return parameter;
		}

		#region Implementation of IList

		int IList.Add(object value) {
			return Add((DbParameter)value);
		}

		bool IList.Contains(object value) {
			if (!(value is DbParameter))
				throw new ArgumentException();

			return Contains((DbParameter) value);
		}

		public bool Contains(DbParameter parameter) {
			return IndexOf(parameter) != -1;
		}

		public void Clear() {
			list.Clear();
		}

		int IList.IndexOf(object value) {
			if (!(value is DbParameter))
				throw new ArgumentException();

			throw new NotImplementedException();
		}

		public int IndexOf(DbParameter parameter) {
			//TODO: is this assumption correct?
			return parameter.index;
		}

		void IList.Insert(int index, object value) {
			throw new NotImplementedException();
		}

		void IList.Remove(object value) {
			//TODO: remove the parameter at the given index and rehash the indexes
			//      of the parameters...
			throw new NotImplementedException();
		}

		public void RemoveAt(int index) {
			//TODO: remove the parameter at the given index and rehash the indexes
			//      of the parameters...
			list.RemoveAt(index);
		}

		object IList.this[int index] {
			get { return this[index]; }
			set {
				if (!(value is DbParameter))
					throw new ArgumentException();
				this[index] = (DbParameter) value;
			}
		}

		bool IList.IsReadOnly {
			get { return false; }
		}

		bool IList.IsFixedSize {
			get { return false; }
		}

		#endregion

		#region Implementation of IDataParameterCollection

		bool IDataParameterCollection.Contains(string parameterName) {
			// we silently ignore named parameters (for the momernt)...
			return false;
		}

		int IDataParameterCollection.IndexOf(string parameterName) {
			// we silently ignore named parameters (for the momernt)...
			return -1;
		}

		void IDataParameterCollection.RemoveAt(string parameterName) {
			// we silently ignore named parameters (for the momernt)...
		}

		object IDataParameterCollection.this[string parameterName] {
			get {
				// we silently ignore named parameters (for the momernt)...
				return null;
			}
			set { throw new NotSupportedException(); }
		}

		#endregion
	}
}