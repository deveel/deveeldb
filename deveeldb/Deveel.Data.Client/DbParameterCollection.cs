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
		internal DbParameterCollection(DbCommand command) {
			this.command = command;
			list = new ArrayList();
		}

		private readonly DbCommand command;
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

		public bool Contains(string parameterName) {
			return IndexOf(parameterName) != -1;
		}

		public int IndexOf(string parameterName) {
			CheckNamedStyle();
			if (list.Count == 0)
				return -1;

			for (int i = 0; i < list.Count; i++) {
				DbParameter parameter = (DbParameter) list[i];
				if (parameter.ParameterName == null)
					continue;
				if (String.Compare(parameter.ParameterName, parameterName, false) == 0)
					return i;
			}

			return -1;
		}

		public void RemoveAt(string parameterName) {
			CheckNamedStyle();
			int index = IndexOf(parameterName);
			if (index == -1)
				throw new ArgumentException("Parameter with name '" + parameterName + "' not found.");
			list.RemoveAt(index);
		}

		object IDataParameterCollection.this[string parameterName] {
			get { return this[parameterName]; }
			set { this[parameterName] = (DbParameter) value; }
		}

		#endregion

		private void CheckNamedStyle() {
			if (command.Connection.ConnectionString.ParameterStyle != ParameterStyle.Named)
				throw new NotSupportedException("Named parameters are not supported in this context.");
		}

		public DbParameter this[string parameterName] {
			get {
				CheckNamedStyle();
				int index = IndexOf(parameterName);
				return (index == -1 ? null : (DbParameter)list[index]);
			}
			set {
				CheckNamedStyle();
				int index = IndexOf(parameterName);
				if (index == -1)
					throw new ArgumentException("Unable to find a parameter with the given name '" + parameterName + "'.");
				list[index] = value;
			}
		}
	}
}