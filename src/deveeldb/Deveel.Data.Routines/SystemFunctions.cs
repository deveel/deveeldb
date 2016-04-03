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

using Deveel.Data;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Sequences;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Routines {
	public static class SystemFunctions {
		static SystemFunctions() {
			Provider = new SystemFunctionsProvider();
		}

		public static FunctionProvider Provider { get; private set; }

		public static Field Or(Field ob1, Field ob2) {
			return ob1 != null ? (ob2.IsNull ? ob1 : (!ob1.IsNull ? ob1.Or(ob2) : ob2)) : ob2;
		}

		public static Field User(IRequest query) {
			return Field.String(query.User().Name);
		}


		public static Field ToDate(Field obj) {
			return obj.CastTo(PrimitiveTypes.Date());
		}

		public static Field ToDate(SqlString value) {
			return ToDate(Field.String(value));
		}

		public static Field ToDateTime(Field obj) {
			return obj.CastTo(PrimitiveTypes.DateTime());
		}

		public static Field ToDateTime(SqlString value) {
			return ToDateTime(Field.String(value));
		}

		public static Field ToTimeStamp(Field obj) {
			return obj.CastTo(PrimitiveTypes.TimeStamp());
		}

		public static Field ToTimeStamp(SqlString value) {
			return ToTimeStamp(Field.String(value));
		}

		public static Field Cast(Field value, SqlType destType) {
			return value.CastTo(destType);
		}

		public static Field Cast(IQuery query, Field value, SqlString typeString) {
			var destType = SqlType.Parse(query.Context, typeString.ToString());
			return Cast(value, destType);
		}

		public static Field ToNumber(Field value) {
			return value.CastTo(PrimitiveTypes.Numeric());
		}

		public static Field ToString(Field value) {
			return value.CastTo(PrimitiveTypes.String());
		}

		public static Field ToBinary(Field value) {
			return value.CastTo(PrimitiveTypes.Binary());
		}

		#region Sequences

		public static Field UniqueKey(IRequest request, Field tableName) {
			var tableNameString = (SqlString)tableName.Value;
			var value = UniqueKey(request, tableNameString);
			return Field.Number(value);
		}

		public static SqlNumber UniqueKey(IRequest request, SqlString tableName) {
			var tableNameString = tableName.ToString();
			var resolvedName = request.Access().ResolveTableName(tableNameString);
			return request.Access().GetNextValue(resolvedName);
		}

		public static Field CurrentKey(IRequest request, Field tableName) {
			var tableNameString = (SqlString)tableName.Value;
			var result = CurrentKey(request, tableNameString);
			return Field.Number(result);
		}

		public static SqlNumber CurrentKey(IRequest request, SqlString tableName) {
			var tableNameString = tableName.ToString();
			var resolvedName = request.Access().ResolveTableName(tableNameString);
			return request.Access().GetCurrentValue(resolvedName);
		}

		public static Field CurrentValue(IRequest query, Field tableName) {
			var sequenceName = (SqlString)tableName.Value;
			var value = CurrentValue(query, sequenceName);
			return Field.Number(value);
		}

		public static SqlNumber CurrentValue(IRequest query, SqlString sequenceName) {
			var objName = ObjectName.Parse(sequenceName.ToString());
			var resolvedName = query.Access().ResolveObjectName(DbObjectType.Sequence, objName);
			return query.Access().GetCurrentValue(resolvedName);
		}

		public static Field NextValue(IRequest request, Field sequenceName) {
			var sequenceNameString = (SqlString)sequenceName.Value;
			var value = NextValue(request, sequenceNameString);
			return Field.Number(value);
		}

		public static SqlNumber NextValue(IRequest request, SqlString sequenceName) {
			var objName = ObjectName.Parse(sequenceName.ToString());
			var resolvedName = request.Access().ResolveObjectName(DbObjectType.Sequence, objName);
			return request.Access().GetNextValue(resolvedName);
		}

		#endregion

		#region Dates

		public static Field CurrentDate(IRequest request) {
			var systemDate = SqlDateTime.Now;
			var sessionOffset = request.Query.Session.TimeZoneOffset();
			var offset = new SqlDayToSecond(sessionOffset.Hours, sessionOffset.Minutes, 0);
			return Field.Date(systemDate.Add(offset).DatePart);
		}

		public static Field CurrentTime(IRequest request) {
			var systemDate = SqlDateTime.Now;
			var sessionOffset = request.Query.Session.TimeZoneOffset();
			var offset = new SqlDayToSecond(sessionOffset.Hours, sessionOffset.Minutes, 0);
			return Field.Time(systemDate.Add(offset).TimePart);
		}

		public static Field CurrentTimeStamp(IRequest request) {
			var systemDate = SqlDateTime.Now;
			var sessionOffset = request.Query.Session.TimeZoneOffset();
			var offset = new SqlDayToSecond(sessionOffset.Hours, sessionOffset.Minutes, 0);
			return Field.TimeStamp(systemDate.Add(offset));
		}

		public static Field SystemDate() {
			return Field.Date(SqlDateTime.Now.DatePart);
		}

		public static Field SystemTime() {
			return Field.Time(SqlDateTime.Now.TimePart);
		}

		public static Field SystemTimeStamp() {
			return Field.TimeStamp(SqlDateTime.Now);
		}

		public static Field AddDate(Field dateField, Field datePart, Field value) {
			if (dateField.IsNull)
				return Field.Date(SqlDateTime.Null);

			var date = (SqlDateTime) dateField.AsDate().Value;
			var partString = datePart.AsVarChar().Value.ToString();
			var iValue = ((SqlNumber) value.AsInteger().Value).ToInt32();

			SqlDateTime result;

			switch (partString.ToUpperInvariant()) {
				case "YEAR":
					result = date.Add(new SqlYearToMonth(iValue, 0));
					break;
				case "MONTH":
					result = date.Add(new SqlYearToMonth(iValue));
					break;
				case "DAY":
					result = date.Add(new SqlDayToSecond(iValue, 0, 0, 0, 0));
					break;
				case "HOUR":
					result = date.Add(new SqlDayToSecond(0, iValue, 0, 0));
					break;
				case "MINUTE":
					result = date.Add(new SqlDayToSecond(0, 0, iValue, 0));
					break;
				case "SECOND":
					result = date.Add(new SqlDayToSecond(0, 0, 0, iValue, 0));
					break;
				case "MILLIS":
				case "MILLISECOND":
					result = date.Add(new SqlDayToSecond(0, 0, 0, 0, iValue));
					break;
				default:
					throw new ArgumentException(String.Format("The date part '{0}' is invalid", partString));
			}

			return Field.Date(result);
		}

		public static Field Extract(Field dateField, Field datePart) {
			if (dateField.IsNull)
				return Field.Number(SqlNumber.Null);

			var date = (SqlDateTime)dateField.AsDate().Value;
			var partString = datePart.AsVarChar().Value.ToString();

			int result;

			switch (partString.ToUpperInvariant()) {
				case "YEAR":
					result = date.Year;
					break;
				case "MONTH":
					result = date.Month;
					break;
				case "DAY":
					result = date.Day;
					break;
				case "HOUR":
					result = date.Hour;
					break;
				case "MINUTE":
					result = date.Minute;
					break;
				case "SECOND":
					result = date.Second;
					break;
				case "MILLIS":
				case "MILLISECOND":
					result = date.Millisecond;
					break;
				default:
					throw new ArgumentException(String.Format("The date part '{0}' is invalid", partString));
			}

			return Field.Integer(result);
		}

		public static Field DateFormat(Field dateField, Field format) {
			if (Field.IsNullField(dateField))
				return Field.String(SqlString.Null);

			var date = (SqlDateTime) dateField.Value;
			var formatString = format.Value.ToString();

			// TODO: Get the current context's culture for formatting
			var result = date.ToString(formatString);
			return Field.String(result);
		}

		public static Field NextDay(Field dateField, Field dayOfWeek) {
			if (Field.IsNullField(dateField))
				return dateField;

			var date = (SqlDateTime) dateField.Value;
			var dow = ParseDayOfWeek(dayOfWeek);
			var result = date.GetNextDateForDay(dow);
			return Field.Date(result);
		}

		private static DayOfWeek ParseDayOfWeek(Field value) {
			var s = value.Value.ToString();
			return (DayOfWeek) Enum.Parse(typeof (DayOfWeek), s, true);
		}

		#endregion

		internal static InvokeResult Iif(InvokeContext context) {
			var result = Field.Null();

			var evalContext = new EvaluateContext(context.Request, context.VariableResolver, context.GroupResolver);

			var condition = context.Arguments[0].EvaluateToConstant(evalContext);
			if (condition.Type is BooleanType) {
				if (condition.Equals(Field.BooleanTrue)) {
					result = context.Arguments[1].EvaluateToConstant(evalContext);
				} else if (condition.Equals(Field.BooleanFalse)) {
					result = context.Arguments[2].EvaluateToConstant(evalContext);
				}
			}

			return context.Result(result);
		}

		internal static Field FRuleConvert(Field obj) {
			if (obj.Type is StringType) {
				String str = null;
				if (!obj.IsNull) {
					str = obj.Value.ToString();
				}
				int v;
				if (str == null || str.Equals("") || str.Equals("NO ACTION")) {
					v = ImportedKey.NoAction;
				} else if (str.Equals("CASCADE")) {
					v = ImportedKey.Cascade;
				} else if (str.Equals("SET NULL")) {
					v = ImportedKey.SetNull;
				} else if (str.Equals("SET DEFAULT")) {
					v = ImportedKey.SetDefault;
				} else if (str.Equals("RESTRICT")) {
					v = ImportedKey.Restrict;
				} else {
					throw new InvalidOperationException("Unrecognised foreign key rule: " + str);
				}

				// Return the correct enumeration
				return Field.Integer(v);
			}
			if (obj.Type is NumericType) {
				var code = ((SqlNumber)obj.AsBigInt().Value).ToInt32();
				string v;
				if (code == (int)ForeignKeyAction.Cascade) {
					v = "CASCADE";
				} else if (code == (int)ForeignKeyAction.NoAction) {
					v = "NO ACTION";
				} else if (code == (int)ForeignKeyAction.SetDefault) {
					v = "SET DEFAULT";
				} else if (code == (int)ForeignKeyAction.SetNull) {
					v = "SET NULL";
				} else {
					throw new InvalidOperationException("Unrecognised foreign key rule: " + code);
				}

				return Field.String(v);
			}

			throw new InvalidOperationException("Unsupported type in function argument");
		}
	}
}