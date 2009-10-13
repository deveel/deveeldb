//  
//  DeveelDbParameterCollection.cs
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
using System.Data.Common;

namespace Deveel.Data.Client {
	public sealed class DeveelDbParameterCollection : DbParameterCollection {
		internal DeveelDbParameterCollection(DeveelDbCommand command) {
			this.command = command;
			list = new ArrayList();
		}

		private readonly DeveelDbCommand command;
		private readonly ArrayList list;

		public new DeveelDbParameter this[int index] {
			get { return base[index] as DeveelDbParameter; }
			set { list[index] = value; }
		}

		public DeveelDbParameter Add(DeveelDbParameter parameter) {
			int i = Add((object)parameter);
			return list[i] as DeveelDbParameter;
		}

		public override int Add(object value) {
			DeveelDbParameter parameter;
			if (value is DeveelDbParameter) {
				parameter = (DeveelDbParameter) value;
			} else {
				parameter = new DeveelDbParameter(value);
			}

			if (command.Connection.Settings.ParameterStyle == ParameterStyle.Named) {
				if (parameter.ParameterName == null)
					throw new ArgumentException();
				if (Contains(parameter.ParameterName))
					throw new InvalidOperationException();
			}

			return list.Add(parameter);
		}

		public DeveelDbParameter Add(object value, int size) {
			return Add(new DeveelDbParameter(value));
		}

		public DeveelDbParameter Add(object value, int size, byte scale) {
			DeveelDbParameter parameter = Add(value, size);
			parameter.Scale = scale;
			return parameter;
		}

		public DeveelDbParameter Add(string name, object value) {
			CheckNamedStyle();
			DeveelDbParameter parameter = new DeveelDbParameter(value);
			parameter.paramStyle = command.Connection.Settings.ParameterStyle;
			parameter.ParameterName = name;
			return parameter;
		}


		public void AddRange(DeveelDbParameter[] values) {
			for (int i = 0; i < values.Length; i++) {
				Add(values.GetValue(i));
			}
		}

		public override void AddRange(Array values) {
			AddRange((DeveelDbParameter[])values);
		}

		public bool Contains(DeveelDbParameter parameter) {
			return IndexOf(parameter) != -1;
		}

		public override bool Contains(object value) {
			if (command.Connection.Settings.ParameterStyle == ParameterStyle.Marker) {
				string parameterName;
				if (value is string) {
					parameterName = (string) value;
				} else if (value is DeveelDbParameter) {
					parameterName = ((DeveelDbParameter) value).ParameterName;
				} else {
					throw new ArgumentException();
				}

				return IndexOf(parameterName) != -1;
			} 
			if (value is DeveelDbParameter) {
				return IndexOf((DeveelDbParameter) value) != -1;
			} else {
				throw new NotSupportedException();
			}
		}

		public int IndexOf(DeveelDbParameter parameter) {
			if (parameter == null)
				throw new ArgumentNullException("parameter");

			if (command.Connection.Settings.ParameterStyle != ParameterStyle.Named)
				throw new InvalidOperationException();

			if (parameter.ParameterName == null)
				throw new ArgumentException();

			return IndexOf(parameter.ParameterName);
		}

		public override int IndexOf(object value) {
			throw new NotImplementedException();
		}

		public override void RemoveAt(int index) {
			list.RemoveAt(index);
		}


		public override bool Contains(string parameterName) {
			return IndexOf(parameterName) != -1;
		}

		public override int IndexOf(string parameterName) {
			CheckNamedStyle();
			if (list.Count == 0)
				return -1;

			for (int i = 0; i < list.Count; i++) {
				DeveelDbParameter parameter = (DeveelDbParameter) list[i];
				if (parameter.ParameterName == null)
					continue;
				if (String.Compare(parameter.ParameterName, parameterName, false) == 0)
					return i;
			}

			return -1;
		}

		public override void RemoveAt(string parameterName) {
			CheckNamedStyle();
			int index = IndexOf(parameterName);
			if (index == -1)
				throw new ArgumentException("Parameter with name '" + parameterName + "' not found.");
			list.RemoveAt(index);
		}

		public override void Clear() {
			list.Clear();
		}

		public override void CopyTo(Array array, int index) {
			list.CopyTo(array, index);
		}

		public override IEnumerator GetEnumerator() {
			return list.GetEnumerator();
		}

		protected override DbParameter GetParameter(int index) {
			return this[index];
		}

		protected override DbParameter GetParameter(string parameterName) {
			return this[parameterName];
		}

		public override void Insert(int index, object value) {
			if (value is DeveelDbParameter) {
				list.Insert(index, value);
				return;
			}

			if (command.Connection.Settings.ParameterStyle == ParameterStyle.Named)
				throw new InvalidOperationException();

			list.Insert(index, new DeveelDbParameter(value));
		}

		public override void Remove(object value) {
			if (value is string) {
				Remove((string)value);
				return;
			}

			if (value is DeveelDbParameter) {
				Remove((DeveelDbParameter)value);
				return;
			}
		}

		public void Remove(string parameterName) {
			CheckNamedStyle();
			int index = IndexOf(parameterName);
			if (index == -1)
				throw new ArgumentException();
			RemoveAt(index);
		}

		public void Remove(DeveelDbParameter parameter) {
			int index = IndexOf(parameter);
			if (index == -1)
				throw new ArgumentException();
			RemoveAt(index);
		}

		protected override void SetParameter(int index, DbParameter value) {
			this[index] = (DeveelDbParameter) value;
		}

		protected override void SetParameter(string parameterName, DbParameter value) {
			this[parameterName] = (DeveelDbParameter) value;
		}

		public override int Count {
			get { return list.Count; }
		}

		public override bool IsFixedSize {
			get { return false; }
		}

		public override bool IsReadOnly {
			get { return false; }
		}

		public override bool IsSynchronized {
			get { return list.IsSynchronized; }
		}

		public override object SyncRoot {
			get { return list; }
		}

		private void CheckNamedStyle() {
			if (command.Connection.Settings.ParameterStyle != ParameterStyle.Named)
				throw new NotSupportedException("Named parameters are not supported in this context.");
		}

		public new DeveelDbParameter this[string parameterName] {
			get {
				CheckNamedStyle();
				int index = IndexOf(parameterName);
				return (index == -1 ? null : (DeveelDbParameter)list[index]);
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