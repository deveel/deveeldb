// 
//  DbParameterCollection.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
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
		}

		public int Add(DbParameter parameter) {
			int i = list.Add(parameter);
			parameter.index = i;
			return i;
		}

		public int Add(object value) {
			if (value is DbParameter)
				return Add((DbParameter) value);

			DbParameter parameter = new DbParameter();
			parameter.Value = value;
			return Add(parameter);
		}

		#region Implementation of IList

		int IList.Add(object value) {
			return Add((DbParameter)value);
		}

		bool IList.Contains(object value) {
			throw new NotImplementedException();
		}

		public void Clear() {
			list.Clear();
		}

		int IList.IndexOf(object value) {
			throw new NotImplementedException();
		}

		void IList.Insert(int index, object value) {
			throw new NotImplementedException();
		}

		void IList.Remove(object value) {
			throw new NotImplementedException();
		}

		public void RemoveAt(int index) {
			list.RemoveAt(index);
		}

		object IList.this[int index] {
			get { return this[index]; }
			set { throw new NotImplementedException(); }
		}

		bool IList.IsReadOnly {
			get { throw new NotImplementedException(); }
		}

		bool IList.IsFixedSize {
			get { throw new NotImplementedException(); }
		}

		#endregion

		#region Implementation of IDataParameterCollection

		bool IDataParameterCollection.Contains(string parameterName) {
			throw new NotSupportedException();
		}

		int IDataParameterCollection.IndexOf(string parameterName) {
			throw new NotSupportedException();
		}

		void IDataParameterCollection.RemoveAt(string parameterName) {
			throw new NotSupportedException();
		}

		object IDataParameterCollection.this[string parameterName] {
			get { throw new NotSupportedException(); }
			set { throw new NotSupportedException(); }
		}

		#endregion
	}
}