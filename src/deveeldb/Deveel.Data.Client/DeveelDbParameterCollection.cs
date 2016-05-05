// 
//  Copyright 2010-2016 Deveel
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
//


using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Runtime.CompilerServices;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Client {
	public sealed class DeveelDbParameterCollection : DbParameterCollection {
		private readonly List<DeveelDbParameter> parameters;

		internal DeveelDbParameterCollection(DeveelDbCommand command) {
			if (command == null)
				throw new ArgumentNullException("command");

			Command = command;
			parameters = new List<DeveelDbParameter>();
		}

		private DeveelDbCommand Command { get; set; }

		public new DeveelDbParameter this[int offset] {
			get { return(DeveelDbParameter) GetParameter(offset); }
			set { SetParameter(offset, value); }
		}

		public new DeveelDbParameter this[string name] {
			get { return (DeveelDbParameter) GetParameter(name); }
			set { SetParameter(name, value); }
		}

		private QueryParameterStyle ParameterStyle {
			get { return Command.Connection.Settings.ParameterStyle; }
		}

		private void AssertValidName(string paramName) {
			if (ParameterStyle == QueryParameterStyle.Named) {
				if (String.IsNullOrEmpty(paramName))
					throw new ArgumentNullException("paramName");
				if (String.Equals(paramName, QueryParameter.Marker))
					throw new ArgumentException("The marker parameter name is not permitted in a named context.");

				if (Contains(paramName))
					throw new ArgumentException(String.Format("The parameter '{0}' already exists in the collection.", paramName));
			} else {
				if (!String.IsNullOrEmpty(paramName) &&
					!String.Equals(paramName, QueryParameter.Marker))
					throw new ArgumentException("Cannot specify the name of the parameter.");
			}
		}

		private DeveelDbParameter CreateParemeter(object value) {
			var dbType = GetDbType(value);
			var sqlType = GetSqlType(value);

			return new DeveelDbParameter {
				DbType = dbType,
				SqlType = sqlType,
				Direction = ParameterDirection.Input,
				Value = value,
				ParameterName = QueryParameter.Marker,
			};
		}

		private SqlTypeCode GetSqlType(object value) {
			if (value == null)
				return SqlTypeCode.Null;

			var type = value.GetType();
			return SqlType.GetTypeCode(type);
		}

		private DbType GetDbType(object value) {
			if (value is bool)
				return DbType.Boolean;
			if (value is byte)
				return DbType.Byte;
			if (value is int)
				return DbType.Int32;
			if (value is short)
				return DbType.Int16;
			if (value is long)
				return DbType.Int64;
			if (value is double)
				return DbType.Double;
			if (value is float)
				return DbType.Single;
			if (value is string)
				return DbType.String;
			if (value is DateTime)
				return DbType.DateTime2;
			if (value is DateTimeOffset)
				return DbType.DateTimeOffset;
			if (value is byte[])
				return DbType.Binary;

			throw new NotSupportedException();
		}

		public override int Add(object value) {
			if (value is DeveelDbParameter)
				return AddParameter((DeveelDbParameter) value);
			if (value is IDbDataParameter)
				return AddDbDataParameter((IDbDataParameter) value);

			return AddValue(value);
		}

		public int Add(DeveelDbParameter parameter) {
			return AddParameter(parameter);
		}

		public DeveelDbParameter Add(string name, object value) {
			var dbType = GetDbType(value);

			var parameter = new DeveelDbParameter(name, dbType, value);
			Add(parameter);
			return parameter;
		}

		private int AddValue(object value) {
			if (ParameterStyle != QueryParameterStyle.Marker)
				throw new ArgumentException("Cannot add an unnamed parameter in this context.");

			var parameter = CreateParemeter(value);
			parameters.Add(parameter);
			return parameters.Count - 1;
		}

		private int AddDbDataParameter(IDbDataParameter parameter) {
			AssertValidName(parameter.ParameterName);

			DeveelDbParameter dbParameter;
			if (parameter is DeveelDbParameter) {
				dbParameter = (DeveelDbParameter) parameter;
			} else {
				dbParameter = new DeveelDbParameter();
			}

			parameters.Add(dbParameter);
			return parameters.Count - 1;
		}

		private int AddParameter(DeveelDbParameter parameter) {
			AssertValidName(parameter.ParameterName);
			parameters.Add(parameter);
			return parameters.Count - 1;
		}

		public override bool Contains(object value) {
			if (value is string)
				return Contains((string) value);
			if (value is IDbDataParameter)
				return Contains(((IDbDataParameter) value).ParameterName);

			return false;
		}

		public override void Clear() {
			parameters.Clear();
		}

		public override int IndexOf(object value) {
			if (value is string)
				return IndexOf((string) value);
			if (value is IDbDataParameter)
				return IndexOf(((IDbDataParameter) value).ParameterName);

			return -1;
		}

		public override void Insert(int index, object value) {
			throw new NotImplementedException();
		}

		public override void Remove(object value) {
			if (value is string) {
				var paramName = (string) value;
				RemoveAt(paramName);
			} else if (value is int) {
				var index = (int) value;
				RemoveAt(index);
			} else if (value is IDbDataParameter) {
				var param = (IDbDataParameter) value;
				throw new NotImplementedException();
			}
		}

		public override void RemoveAt(int index) {
			if (index < 0 || index >= parameters.Count)
				throw new ArgumentOutOfRangeException("index");

			parameters.RemoveAt(index);
		}

		public override void RemoveAt(string parameterName) {
			var index = IndexOf(parameterName);
			if (index == -1)
				return;

			RemoveAt(index);
		}

		protected override void SetParameter(int index, DbParameter value) {
			if (index < 0 || index >= parameters.Count)
				throw new ArgumentOutOfRangeException("index");
			if (!(value is DeveelDbParameter))
				throw new ArgumentException("The parameter type is invalid.", "value");

			parameters[index] = (DeveelDbParameter) value;
		}

		protected override void SetParameter(string parameterName, DbParameter value) {
			if (ParameterStyle != QueryParameterStyle.Named)
				throw new ArgumentException();
			if (String.IsNullOrEmpty(parameterName))
				throw new ArgumentNullException("parameterName");
			if (!(value is DeveelDbParameter))
				throw new ArgumentException("The parameter type is invalid.", "value");

			var index = parameters.FindIndex(x => x.ParameterName == parameterName);
			if (index == -1)
				throw new ArgumentException(String.Format("Parameter '{0}' was not found in collection", parameterName));

			SetParameter(index, value);
		}

		public override int Count {
			get { return parameters.Count; }
		}

		public override object SyncRoot {
			get { return this; }
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

		public override int IndexOf(string parameterName) {
			if (ParameterStyle != QueryParameterStyle.Named)
				return -1;

			for (int i = 0; i < parameters.Count; i++) {
				if (parameters[i].ParameterName == parameterName)
					return i;
			}

			return -1;
		}

		public override IEnumerator GetEnumerator() {
			return parameters.GetEnumerator();
		}

		protected override DbParameter GetParameter(int index) {
			if (index < 0 || index >= parameters.Count)
				throw new ArgumentOutOfRangeException("index");

			return parameters[index];
		}

		protected override DbParameter GetParameter(string parameterName) {
			if (ParameterStyle != QueryParameterStyle.Named)
				return null;

			return parameters.FirstOrDefault(x => x.ParameterName == parameterName);
		}

		public override bool Contains(string value) {
			if (ParameterStyle != QueryParameterStyle.Named)
				return false;

			return parameters.Any(x => x.ParameterName == value);
		}

		public override void CopyTo(Array array, int index) {
			if (array == null)
				throw new ArgumentNullException("array");

			var paramArray = parameters.ToArray();
			var count = System.Math.Min(paramArray.Length, array.Length);
			Array.Copy(paramArray, 0, array, index, count);
		}

		public override void AddRange(Array values) {
			foreach (var value in values) {
				Add(value);
			}
		}
	}
}
