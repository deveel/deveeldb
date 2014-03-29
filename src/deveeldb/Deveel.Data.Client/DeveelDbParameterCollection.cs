// 
//  Copyright 2010-2014  Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Collections;
using System.Data;
using System.Data.Common;

using Deveel.Data.Sql;

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
			if (parameter.ParameterName != "?") {
				// This is a lucky guess: a best check later at execution

				if (parameter.ParameterName == null)
					throw new ArgumentException();
				if (Contains(parameter.ParameterName))
					throw new InvalidOperationException();
			}

			var index = Add((object)parameter);
			return (DeveelDbParameter) list[index];
		}

		public override int Add(object value) {
			DeveelDbParameter parameter;
			if (value is DeveelDbParameter) {
				parameter = (DeveelDbParameter) value;
			} else {
				parameter = new DeveelDbParameter(value);
			}

			return list.Add(parameter);
		}

		public DeveelDbParameter Add(object value, int size) {
			return Add(new DeveelDbParameter(value) { Size = size });
		}

		public DeveelDbParameter Add(object value, int size, byte scale) {
			return Add(new DeveelDbParameter(value) { Size = size, Scale = scale});
		}

		public DeveelDbParameter Add(string name, object value) {
			return Add(new DeveelDbParameter(value) { ParameterName = name });
		}


		public void AddRange(DeveelDbParameter[] values) {
			foreach (DeveelDbParameter parameter in values) {
				Add(parameter);
			}
		}

		public override void AddRange(Array values) {
			AddRange((DeveelDbParameter[])values);
		}

		public bool Contains(DeveelDbParameter parameter) {
			return IndexOf(parameter) != -1;
		}

		public override bool Contains(object value) {
			string parameterName;
			if (value is string) {
				parameterName = (string) value;
			} else if (value is DeveelDbParameter) {
				parameterName = ((DeveelDbParameter) value).ParameterName;
			} else {
				throw new ArgumentException();
			}

			if (parameterName == "?")
				throw new NotSupportedException("Method not supported for 'Marker' parameter style.");

			return IndexOf(parameterName) != -1;
		}

		public int IndexOf(DeveelDbParameter parameter) {
			if (parameter == null)
				throw new ArgumentNullException("parameter");

			if (parameter.ParameterName == "?")
				throw new NotSupportedException("Method not supported for 'Marker' parameter style.");

			if (String.IsNullOrEmpty(parameter.ParameterName))
				throw new ArgumentException("Paremeter name is null");

			return IndexOf(parameter.ParameterName);
		}

		public override int IndexOf(object value) {
			if (value == null)
				throw new ArgumentNullException("value");

			string parameterName;
			if (value is string) {
				parameterName = (string)value;
			} else if (value is DeveelDbParameter) {
				parameterName = ((DeveelDbParameter)value).ParameterName;
			} else {
				throw new ArgumentException();
			}

			if (parameterName == "?")
				throw new NotSupportedException("Method not supported for 'Marker' parameter style.");

			return IndexOf(parameterName);
		}

		public override void RemoveAt(int index) {
			list.RemoveAt(index);
		}

		public override bool Contains(string parameterName) {
			return IndexOf(parameterName) != -1;
		}

		public override int IndexOf(string parameterName) {
			if (parameterName == "?")
				throw new ArgumentException();

			if (list.Count == 0)
				return -1;

			for (int i = 0; i < list.Count; i++) {
				var parameter = (DeveelDbParameter) list[i];
				if (parameter.ParameterName == null)
					continue;

				if (String.CompareOrdinal(parameter.ParameterName, parameterName) == 0)
					return i;
			}

			return -1;
		}

		public override void RemoveAt(string parameterName) {
			int index = IndexOf(parameterName);
			if (index == -1)
				throw new ArgumentException("Parameter with name '" + parameterName + "' not found.");

			list.RemoveAt(index);
		}

		public override void Clear() {
			list.Clear();
		}

		public override void CopyTo(Array array, int index) {
			if (array != null)
				list.CopyTo(array, index);
		}

		public override IEnumerator GetEnumerator() {
			return list.GetEnumerator();
		}

		protected override DbParameter GetParameter(int index) {
			return list[index] as DbParameter;
		}

		protected override DbParameter GetParameter(string parameterName) {
			return this[parameterName];
		}

		public override void Insert(int index, object value) {
			if (value is DeveelDbParameter) {
				list.Insert(index, (DeveelDbParameter) value);
				return;
			}

			list.Insert(index, new DeveelDbParameter(value));
		}

		public override void Remove(object value) {
			if (value is string) {
				Remove((string)value);
				return;
			}

			if (value is DeveelDbParameter) {
				Remove((DeveelDbParameter)value);
			}
		}

		public void Remove(string parameterName) {
			if (parameterName == "?")
				throw new NotSupportedException("Method not supported for 'Marker' parameter style.");

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
			DeveelDbParameter parameter;
			if (value is DeveelDbParameter) {
				parameter = (DeveelDbParameter) value;
			} else {
				parameter = new DeveelDbParameter(value.Value) {
					ParameterName = value.ParameterName,
					IsNullable = value.IsNullable,
					Size = value.Size,
					Direction = value.Direction,
					DbType = value.DbType,
					SourceColumn = value.SourceColumn,
					SourceVersion = value.SourceVersion
				};
			}

			this[index] = parameter;
		}

		protected override void SetParameter(string parameterName, DbParameter value) {
			var index = IndexOf(value.ParameterName);
			if (index != -1)
				SetParameter(index, value);
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
			get { return false; }
		}

		public override object SyncRoot {
			get { return list; }
		}

		public new DeveelDbParameter this[string parameterName] {
			get {
				int index = IndexOf(parameterName);
				return (index == -1 ? null : (DeveelDbParameter)list[index]);
			}
			set {
				int index = IndexOf(parameterName);
				if (index == -1)
					throw new ArgumentException("Unable to find a parameter with the given name '" + parameterName + "'.");
				list[index] = value;
			}
		}

		internal void VerifyParameterNames(ParameterStyle parameterStyle) {
			foreach (DeveelDbParameter parameter in list) {
				if (parameterStyle == ParameterStyle.Marker) {
					if (!String.IsNullOrEmpty(parameter.ParameterName) && 
						parameter.ParameterName != "?")
						throw new InvalidOperationException("The connection is set to 'Marker' parameter style but one parameter is named.");
				} else if (parameterStyle == ParameterStyle.Named) {
					var parameterName = parameter.ParameterName;
					if (String.IsNullOrEmpty(parameterName) || parameterName == "?")
						throw new InvalidOperationException("The connection is set to 'Named' parameter style but one parameter is a marker.");
					if (parameterName.Length < 1)
						throw new InvalidOperationException(String.Format("The parameter {0} has an invalid name.", parameterName));

					if (parameterName[0] != '@') {
						parameterName = String.Format("@{0}", parameterName);
						parameter.ParameterName = parameterName;
					}
				}
			}
		}
	}
}