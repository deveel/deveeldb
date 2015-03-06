// 
//  Copyright 2010-2015 Deveel
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
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Math;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class StatementTree : IPreparable, ISerializable, IEnumerable<KeyValuePair<string, object>> {
		private readonly Dictionary<string, object> values;

		public StatementTree(Type statementType) {
			if (statementType == null)
				throw new ArgumentNullException("statementType");
			if (!typeof(Statement).IsAssignableFrom(statementType))
				throw new ArgumentException(String.Format("Type {0} is not a Statement", statementType));

			StatementType = statementType;
			values = new Dictionary<string, object>();
		}

		private StatementTree(SerializationInfo info, StreamingContext context) {
			foreach (var entry in info) {
				var name = entry.Name;
				if (String.Equals(name, "StatementType", StringComparison.Ordinal)) {
					var typeString = (string) entry.Value;
					StatementType = Type.GetType(typeString, true, true);
				} else {
					values[entry.Name] = entry.Value;
				}
			}
		}

		public Type StatementType { get; private set; }

		public StatementTree Prepare(IExpressionPreparer preparer) {
			var prepared = new StatementTree(StatementType);

			foreach (var pair in values) {
				var key = pair.Key;
				var value = pair.Value;
				if (value != null)
					value = PrepareValue(value, preparer);

				prepared.values[key] = value;
			}

			return prepared;
		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			foreach (var pair in values) {
				info.AddValue(pair.Key, pair.Value);
			}
		}

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			return Prepare(preparer);
		}

		private object PrepareValue(object value, IExpressionPreparer preparer) {
			if (value is SqlExpression)
				return ((SqlExpression) value).Prepare(preparer);
			if (value is SqlExpression[]) {
				var array = (SqlExpression[]) value;
				var prepared = new SqlExpression[array.Length];
				for (int i = 0; i < array.Length; i++) {
					var exp = array[i];
					if (exp != null)
						prepared[i] = exp.Prepare(preparer);
				}

				return prepared;
			}

			if (value is IPreparable)
				return ((IPreparable) value).Prepare(preparer);

			if (value is IEnumerable) {
				var en = (IEnumerable) value;
				var prepared = new List<object>();
				foreach (var obj in en) {
					var item = obj;
					if (item != null)
						item = PrepareValue(obj, preparer);
					prepared.Add(item);
				}

				return prepared.AsEnumerable();
			}

			throw new NotSupportedException();
		}

		public Statement CreateStatement() {
			var statement = Activator.CreateInstance(StatementType,
				BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
				null,
				new object[0],
				CultureInfo.InvariantCulture)
				as Statement;
			if (statement != null) {
				statement.SetTree(this);
			}
			return statement;
		}

		public IEnumerator<KeyValuePair<string, object>> GetEnumerator() {
			return values.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public void SetValue(string key, object value) {
			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

			values[key] = value;
		}

		public T GetValue<T>(string key) {
			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

			object value;
			if (!values.TryGetValue(key, out value))
				return default(T);

			if (!(value is T)) {
				if (value is IConvertible) {
					value = Convert.ChangeType(value, typeof (T));
				} else {
					throw new InvalidCastException();
				}
			}

			return (T) value;
		}

		public IList<T> GetList<T>(string key) {
			var list = GetValue<IList<T>>(key);
			if (list == null) {
				list = new List<T>();
				SetValue(key, list);
			}

			return list;
		} 

		public bool GetBoolean(string key) {
			return GetValue<bool>(key);
		}

		public SqlBoolean GetSqlBoolean(string key) {
			return GetValue<SqlBoolean>(key);
		}

		public byte GetByte(string key) {
			return GetValue<byte>(key);
		}

		[CLSCompliant(false)]
		public sbyte GetSByte(string key) {
			return GetValue<sbyte>(key);
		}

		public short GetInt16(string key) {
			return GetValue<short>(key);
		}

		[CLSCompliant(false)]
		public ushort GetUInt16(string key) {
			return GetValue<ushort>(key);
		}

		public int GetInt32(string key) {
			return GetValue<int>(key);
		}

		[CLSCompliant(false)]
		public uint GetUInt32(string key) {
			return GetValue<uint>(key);
		}

		public long GetInt64(string key) {
			return GetValue<long>(key);
		}

		[CLSCompliant(false)]
		public ulong GetUInt64(string key) {
			return GetValue<ulong>(key);
		}

		public float GetSingle(string key) {
			return GetValue<float>(key);
		}

		public double GetDouble(string key) {
			return GetValue<double>(key);
		}

		public BigDecimal GetBigDecimal(string key) {
			return GetValue<BigDecimal>(key);
		}

		public SqlNumber GetSqlNumber(string key) {
			return GetValue<SqlNumber>(key);
		}

		public string GetString(string key) {
			return GetValue<string>(key);
		}

		public SqlString GetSqlString(string key) {
			return GetValue<SqlString>(key);
		}
	}
}