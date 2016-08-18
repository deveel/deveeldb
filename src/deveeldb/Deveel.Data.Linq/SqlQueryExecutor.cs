using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Compile;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using IQToolkit.Data.Common;

using QueryParameter = Deveel.Data.Sql.QueryParameter;
using QueryType = IQToolkit.Data.Common.QueryType;

namespace Deveel.Data.Linq {
	class SqlQueryExecutor : QueryExecutor {
		public SqlQueryExecutor(QueryProvider provider) {
			Provider = provider;
		}

		public QueryProvider Provider { get; private set; }

		public override object Convert(object value, Type type) {
			throw new NotImplementedException();
		}

		public override IEnumerable<T> Execute<T>(QueryCommand command, Func<FieldReader, T> fnProjector, MappingEntity entity, object[] paramValues) {
			var sqlQuery = new SqlQuery(command.CommandText, QueryParameterStyle.Named);
			var queryParams = command.Parameters.Select(x => new Sql.QueryParameter(EnsureParamName(x.Name), GetSqlType(x.Type, x.QueryType))).ToArray();
			for (int i = 0; i < queryParams.Length; i++) {
				queryParams[i].Value = ToSqlObject(queryParams[i].SqlType, paramValues[i]);
			}
			foreach (var parameter in queryParams) {
				sqlQuery.Parameters.Add(parameter);
			}

			var results = Provider.Context.Query.ExecuteQuery(sqlQuery);
			if (results.Length > 1)
				throw new InvalidOperationException("Too many results from query");

			var result = results[0];
			if (result.Type == StatementResultType.Exception)
				throw new InvalidOperationException("The execution of the command caused an error.", result.Error);
			if (result.Type == StatementResultType.CursorRef)
				return Project(result.Cursor, fnProjector, entity, true);

			throw new NotImplementedException();
		}

		private string EnsureParamName(string name) {
			if (name == QueryParameter.Marker)
				return name;
			if (name[0] != QueryParameter.NamePrefix)
				name = String.Format("{0}{1}", QueryParameter.NamePrefix, name);

			return name;
		}

		private IEnumerable<T> Project<T>(ICursor cursor, Func<FieldReader, T> fnProjector, MappingEntity entity, bool closeReader) {
			try {
				foreach (var row in cursor) {
					var reader = new RowFieldReader(row);
					yield return fnProjector(reader);
				}
			} finally {
				if (closeReader) {
					cursor.Dispose();
				}
			}
		}


		private ISqlObject ToSqlObject(SqlType sqlType, object paramValue) {
			return sqlType.CreateFrom(paramValue);
		}

		private SqlTypeCode GetSqlTypeCode(Type type) {
			if (type == typeof(bool))
				return SqlTypeCode.Boolean;
			if (type == typeof(byte))
				return SqlTypeCode.TinyInt;
			if (type == typeof(short))
				return SqlTypeCode.SmallInt;
			if (type == typeof(int))
				return SqlTypeCode.Integer;
			if (type == typeof(long))
				return SqlTypeCode.BigInt;
			if (type == typeof(float))
				return SqlTypeCode.Real;
			if (type == typeof(double))
				return SqlTypeCode.Double;
			if (type == typeof(string))
				return SqlTypeCode.VarChar;
			if (type == typeof(byte[]))
				return SqlTypeCode.VarBinary;
			if (type == typeof(DateTime) ||
				type == typeof(DateTimeOffset))
				return SqlTypeCode.TimeStamp;
			if (type == typeof(TimeSpan))
				return SqlTypeCode.DayToSecond;

			throw new NotSupportedException();
		}

		private SqlType GetSqlType(Type type, QueryType queryType) {
			var sqlTypeCode = GetSqlTypeCode(type);

			switch (sqlTypeCode) {
				case SqlTypeCode.Bit:
				case SqlTypeCode.Boolean:
					return PrimitiveTypes.Boolean(sqlTypeCode);
				case SqlTypeCode.TinyInt:
				case SqlTypeCode.SmallInt:
				case SqlTypeCode.Integer:
				case SqlTypeCode.BigInt:
					return PrimitiveTypes.Numeric(sqlTypeCode);
				case SqlTypeCode.Real:
				case SqlTypeCode.Float:
				case SqlTypeCode.Double:
				case SqlTypeCode.Numeric:
				case SqlTypeCode.Decimal:
					return PrimitiveTypes.Numeric(sqlTypeCode, queryType.Precision, queryType.Scale);
				case SqlTypeCode.String:
				case SqlTypeCode.VarChar:
				case SqlTypeCode.Char:
					return PrimitiveTypes.String(sqlTypeCode, queryType.Length);
				case SqlTypeCode.Binary:
				case SqlTypeCode.VarBinary:
					return PrimitiveTypes.Binary(sqlTypeCode, queryType.Length);
				case SqlTypeCode.Date:
				case SqlTypeCode.DateTime:
				case SqlTypeCode.Time:
				case SqlTypeCode.TimeStamp:
					return PrimitiveTypes.DateTime(sqlTypeCode);
				case SqlTypeCode.DayToSecond:
					return PrimitiveTypes.DayToSecond();
				default:
					throw new NotSupportedException();
			}
		}

		public override IEnumerable<int> ExecuteBatch(QueryCommand query, IEnumerable<object[]> paramSets, int batchSize, bool stream) {
			throw new NotImplementedException();
		}

		public override IEnumerable<T> ExecuteBatch<T>(QueryCommand query, IEnumerable<object[]> paramSets, Func<FieldReader, T> fnProjector, MappingEntity entity,
			int batchSize, bool stream) {
			throw new NotImplementedException();
		}

		public override IEnumerable<T> ExecuteDeferred<T>(QueryCommand query, Func<FieldReader, T> fnProjector, MappingEntity entity, object[] paramValues) {
			throw new NotImplementedException();
		}

		public override int ExecuteCommand(QueryCommand query, object[] paramValues) {
			throw new NotImplementedException();
		}

		public override int RowsAffected { get; }

		#region RowFieldReader

		class RowFieldReader : FieldReader {
			private readonly Row row;

			public RowFieldReader(Row row) {
				this.row = row;
				Init();
			}

			protected override Type GetFieldType(int ordinal) {
				return row.Table.TableInfo[ordinal].ColumnType.GetRuntimeType();
			}

			protected override bool IsDBNull(int ordinal) {
				return row.GetValue(ordinal).IsNull;
			}

			protected override T GetValue<T>(int ordinal) {
				throw new NotImplementedException();
			}

			protected override byte GetByte(int ordinal) {
				throw new NotImplementedException();
			}

			protected override char GetChar(int ordinal) {
				throw new NotImplementedException();
			}

			protected override DateTime GetDateTime(int ordinal) {
				throw new NotImplementedException();
			}

			protected override decimal GetDecimal(int ordinal) {
				throw new NotImplementedException();
			}

			protected override double GetDouble(int ordinal) {
				throw new NotImplementedException();
			}

			protected override float GetSingle(int ordinal) {
				throw new NotImplementedException();
			}

			protected override Guid GetGuid(int ordinal) {
				throw new NotImplementedException();
			}

			protected override short GetInt16(int ordinal) {
				throw new NotImplementedException();
			}

			protected override int GetInt32(int ordinal) {
				var value = row.GetValue(ordinal);
				if (!(value.Type is NumericType))
					value = value.CastTo(PrimitiveTypes.Integer());
				return ((SqlNumber)value.Value).ToInt32();
			}

			protected override long GetInt64(int ordinal) {
				throw new NotImplementedException();
			}

			protected override string GetString(int ordinal) {
				var value = row.GetValue(ordinal);
				if (!(value.Type is StringType))
					value = value.CastTo(PrimitiveTypes.String());
				return ((SqlString) value.Value).ToString();
			}

			protected override int FieldCount {
				get {
					return row.ColumnCount;
				}
			}
		}

		#endregion
	}
}
