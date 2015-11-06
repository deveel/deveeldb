using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Types;

using IQToolkit;
using IQToolkit.Data.Common;

using QueryParameter = IQToolkit.Data.Common.QueryParameter;

namespace Deveel.Data.Linq {
	class DeveelDbExecutor : QueryExecutor {
		private IQueryContext context;
		private int rowsAffected;

		public DeveelDbExecutor(IQueryContext context) {
			this.context = context;
		}

		public override object Convert(object value, Type type) {
			if (value == null) {
				return TypeHelper.GetDefault(type);
			}

			type = TypeHelper.GetNonNullableType(type);
			var vtype = value.GetType();

			if (type != vtype) {
				if (type.IsEnum) {
					if (vtype == typeof (string))
						return Enum.Parse(type, (string) value);

					Type utype = Enum.GetUnderlyingType(type);
					if (utype != vtype)
						value = System.Convert.ChangeType(value, utype);

					return Enum.ToObject(type, value);
				}

				return System.Convert.ChangeType(value, type);
			}

			return value;
		}

		private SqlQuery GetQuery(QueryCommand command, object[] paramValues) {
			var query = new SqlQuery(command.CommandText);
			SetParameters(command, query, paramValues);
			return query;
		}

		private void SetParameters(QueryCommand command, SqlQuery query, object[] paramValues) {
			if (command.Parameters.Count > 0 && query.Parameters.Count == 0) {
				for (int i = 0, n = command.Parameters.Count; i < n; i++) {
					AddParameter(query, command.Parameters[i], paramValues != null ? paramValues[i] : null);
				}
			} else if (paramValues != null) {
				var queryParams = query.Parameters.ToList();
				for (int i = 0, n = queryParams.Count; i < n; i++) {
					var p = queryParams[i];
					if (p.Direction == QueryParameterDirection.In
					 || p.Direction == QueryParameterDirection.InOut) {
						p.Value = (ISqlObject) paramValues[i] ?? SqlNull.Value;
					}
				}
			}
		}

		private void AddParameter(SqlQuery query, QueryParameter queryParameter, object value) {
			var sqlType = GetSqlType(queryParameter.Type);
			var param = new Deveel.Data.Sql.QueryParameter(queryParameter.Name, sqlType, (ISqlObject)value);
			param.Direction = QueryParameterDirection.In;
			query.Parameters.Add(param);
		}

		private SqlType GetSqlType(Type type) {
			throw new NotImplementedException();
		}

		public override IEnumerable<T> Execute<T>(QueryCommand command, Func<FieldReader, T> fnProjector, MappingEntity entity, object[] paramValues) {
			var query = GetQuery(command, paramValues);
			var queryResult = context.ExecuteQuery(query);

			return Project(queryResult[0], fnProjector, entity);
		}

		private IEnumerable<T> Project<T>(ITable result, Func<FieldReader, T> fnProjector, MappingEntity entity) {
			var reader = new TableFieldReader(result);
			while (reader.NextRow()) {
				yield return fnProjector(reader);
			}
		}

		public override IEnumerable<int> ExecuteBatch(QueryCommand query, IEnumerable<object[]> paramSets, int batchSize, bool stream) {
			throw new NotImplementedException();
		}

		public override IEnumerable<T> ExecuteBatch<T>(QueryCommand query, IEnumerable<object[]> paramSets, Func<FieldReader, T> fnProjector, MappingEntity entity,
			int batchSize, bool stream) {
			throw new NotImplementedException();
		}

		public override IEnumerable<T> ExecuteDeferred<T>(QueryCommand command, Func<FieldReader, T> fnProjector, MappingEntity entity, object[] paramValues) {
			var query = GetQuery(command, paramValues);
			var queryResult = context.ExecuteQuery(query);

			return Project(queryResult[0], fnProjector, entity);
		}

		public override int ExecuteCommand(QueryCommand query, object[] paramValues) {
			var sqlQuery = GetQuery(query, paramValues);
			var result = context.ExecuteQuery(sqlQuery);
			rowsAffected = result[0].RowCount;
			return rowsAffected;
		}

		public override int RowsAffected {
			get { return rowsAffected; }
		}

		#region TableFieldReader

		class TableFieldReader : FieldReader {
			private readonly ITable table;
			private int rowOffset = -1;
			private Row row;

			public TableFieldReader(ITable table) {
				this.table = table;
				Init();
			}

			public bool NextRow() {
				if (++rowOffset >= table.RowCount)
					return false;

				row = table.GetRow(rowOffset);
				return true;
			}

			protected override Type GetFieldType(int ordinal) {
				var sqlType = row[ordinal].Type;
				return sqlType.GetRuntimeType();
			}

			protected override bool IsDBNull(int ordinal) {
				return row[ordinal].IsNull;
			}

			protected override T GetValue<T>(int ordinal) {
				var value = row[ordinal];
				return (T) value.Type.ConvertTo(value.Value, typeof (T));
			}

			protected override byte GetByte(int ordinal) {
				return GetValue<byte>(ordinal);
			}

			protected override char GetChar(int ordinal) {
				return GetValue<char>(ordinal);
			}

			protected override DateTime GetDateTime(int ordinal) {
				return GetValue<DateTime>(ordinal);
			}

			protected override decimal GetDecimal(int ordinal) {
				throw new NotImplementedException();
			}

			protected override double GetDouble(int ordinal) {
				return GetValue<double>(ordinal);
			}

			protected override float GetSingle(int ordinal) {
				return GetValue<float>(ordinal);
			}

			protected override Guid GetGuid(int ordinal) {
				throw new NotImplementedException();
			}

			protected override short GetInt16(int ordinal) {
				return GetValue<short>(ordinal);
			}

			protected override int GetInt32(int ordinal) {
				return GetValue<int>(ordinal);
			}

			protected override long GetInt64(int ordinal) {
				return GetValue<long>(ordinal);
			}

			protected override string GetString(int ordinal) {
				return GetValue<string>(ordinal);
			}

			protected override int FieldCount {
				get { return table.TableInfo.ColumnCount; }
			}
		}

		#endregion
	}
}
