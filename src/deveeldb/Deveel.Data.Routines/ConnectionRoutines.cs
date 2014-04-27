// 
//  Copyright 2010-2014 Deveel
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
using System.IO;
using System.Reflection;
using System.Text;

using Deveel.Data.DbSystem;
using Deveel.Data.Security;
using Deveel.Data.Transactions;
using Deveel.Data.Types;
using Deveel.Diagnostics;

namespace Deveel.Data.Routines {
	public static class ConnectionRoutines {
		public const string ExternalStaticType = "ExternalStatic";

		/// <summary>
		/// Formats a string that gives information about the routine, return
		/// type and param types.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="returnType"></param>
		/// <param name="parameters"></param>
		/// <returns></returns>
		private static String InfoString(RoutineName name, TType returnType, TType[] parameters) {
			var buf = new StringBuilder();
			if (returnType != null) {
				buf.Append(returnType.ToSqlString());
				buf.Append(" ");
			}
			buf.Append(name.Name);
			buf.Append("(");
			for (int i = 0; i < parameters.Length; ++i) {
				buf.Append(parameters[i].ToSqlString());
				if (i < parameters.Length - 1) {
					buf.Append(", ");
				}
			}
			buf.Append(")");
			return buf.ToString();
		}

		/// <summary>
		/// Gets a procedure entry.
		/// </summary>
		/// <param name="table">The table containing procedures informations (SystemFunctions).</param>
		/// <param name="routineName">Name of the procedure to return.</param>
		/// <returns>
		/// Returns a one.row table containing informations about the procedure
		/// entry wanted.
		/// </returns>
		private static Table FindEntry(this IDatabaseConnection connection, Table table, RoutineName routineName) {
			var context = new DatabaseQueryContext(connection);
			Operator equals = Operator.Equal;

			VariableName schemav = table.GetResolvedVariable(0);
			VariableName namev = table.GetResolvedVariable(1);

			Table t = table.SimpleSelect(context, namev, equals, new Expression(TObject.CreateString(routineName.Name)));
			t = t.ExhaustiveSelect(context, Expression.Simple(schemav, equals, TObject.CreateString(routineName.Schema)));

			// This should be at most 1 row in size
			if (t.RowCount > 1)
				throw new Exception("Assert failed: multiple procedure names for " + routineName);

			// Return the entries found.
			return t;
		}

		///<summary>
		/// Returns true if the procedure with the given name exists.
		///</summary>
		///<param name="routineName"></param>
		///<returns></returns>
		public static bool RoutineExists(this IDatabaseConnection connection, RoutineName routineName) {
			DataTable table = connection.GetTable(SystemSchema.Function);
			return connection.FindEntry(table, routineName).RowCount == 1;

		}

		///<summary>
		/// Returns true if the procedure with the given table name exists.
		///</summary>
		///<param name="routineName"></param>
		///<returns></returns>
		public static bool RoutineExists(this IDatabaseConnection connection, TableName routineName) {
			return connection.RoutineExists(new RoutineName(routineName));
		}

		/// <summary>
		/// Defines a stored procedure.
		/// </summary>
		/// <param name="routineName">The name of the procedure.</param>
		/// <param name="specification"></param>
		/// <param name="returnType">The return type of the procedure (if null,
		/// the procedure doesn't return any value).</param>
		/// <param name="paramTypes">The parameters types.</param>
		/// <param name="username">The name of the user defining the procedure.</param>
		/// <remarks>
		/// If the procedure has been defined then it is overwritten with 
		/// this informations.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If an ambigous reference was found for the given 
		/// <paramref name="routineName"/>.
		/// </exception>
		public static void DefineExternalRoutine(this IDatabaseConnection connection, RoutineName routineName, string specification, TType returnType, TType[] paramTypes, string username) {
			var routineTableName = new TableName(routineName.Schema, routineName.Name);

			// Check this name is not reserved
			connection.AssertAllowCreate(routineTableName);

			DataTable table = connection.GetTable(SystemSchema.Function);

			// The new row to insert/update    
			var dataRow = new DataRow(table);
			dataRow.SetValue(0, routineName.Schema);
			dataRow.SetValue(1, routineName.Name);
			dataRow.SetValue(2, ExternalStaticType);
			dataRow.SetValue(3, specification);
			if (returnType != null) {
				dataRow.SetValue(4, TType.Encode(returnType));
			}
			dataRow.SetValue(5, TType.Encode(paramTypes));
			dataRow.SetValue(6, username);

			// Find the entry from the procedure table that equal this name
			Table t = connection.FindEntry(table, routineName);

			// Delete the entry if it already exists.
			if (t.RowCount == 1) {
				table.Delete(t);
			}

			// Insert the new entry,
			table.Add(dataRow);

			// Notify that this database object has been successfully created.
			connection.OnDatabaseObjectCreated(routineTableName);

		}

		/// <summary>
		/// Deletes the procedure with the given name.
		/// </summary>
		/// <param name="routineName">Name of the procedure to delete.</param>
		/// <exception cref="StatementException">
		/// If an ambigous reference or none procedure was found for the 
		/// given <paramref name="routineName"/>.
		/// </exception>
		public static void DeleteRoutine(this IDatabaseConnection connection, RoutineName routineName) {
			DataTable table = connection.GetTable(SystemSchema.Function);

			// Find the entry from the procedure table that equal this name
			Table t = connection.FindEntry(table, routineName);

			// If no entries then generate error.
			if (t.RowCount == 0)
				throw new StatementException("Procedure " + routineName + " doesn't exist.");

			table.Delete(t);

			// Notify that this database object has been successfully dropped.
			connection.OnDatabaseObjectDropped(new TableName(routineName.Schema, routineName.Name));
		}

		/// <summary>
		/// Invokes the procedure with the given name and the given parameters.
		/// </summary>
		/// <param name="routineName">Name of the procedure to invoke.</param>
		/// <param name="parameters">The parameters to pass to the procedure for
		/// the invoke, or null if no parameters.</param>
		/// <returns>
		/// Returns a <see cref="TObject"/> result of the invoke of the procedure.
		/// </returns>
		/// <exception cref="StatementException">
		/// If an ambigous reference or none procedure was found for the 
		/// given <paramref name="routineName"/>.
		/// </exception>
		public static TObject InvokeRoutine(this IDatabaseConnection connection, RoutineName routineName, TObject[] parameters) {
			DataTable table = connection.GetTable(SystemSchema.Function);

			// Find the entry from the procedure table that equals this name
			Table t = connection.FindEntry(table, routineName);
			if (t.RowCount == 0)
				throw new StatementException("Routine " + routineName + " doesn't exist.");

			//TODO: check this...
			int rowIndex = t.GetRowEnumerator().RowIndex;
			TObject typeOb = t.GetCell(2, rowIndex);
			TObject locationOb = t.GetCell(3, rowIndex);
			TObject returnTypeOb = t.GetCell(4, rowIndex);
			TObject paramTypesOb = t.GetCell(5, rowIndex);
			TObject ownerOb = t.GetCell(6, rowIndex);

			String type = typeOb.Object.ToString();
			String location = locationOb.Object.ToString();
			TType returnType = null;
			if (!returnTypeOb.IsNull)
				returnType = TType.DecodeString(returnTypeOb.Object.ToString());

			TType[] paramTypes = TType.DecodeTypes(paramTypesOb.Object.ToString());
			String owner = ownerOb.Object.ToString();

			// Check the number of parameters given match the function parameters length
			if (parameters.Length != paramTypes.Length) {
				throw new StatementException(
					"Parameters given do not match the parameters of the procedure: " +
					InfoString(routineName, returnType, paramTypes));
			}

			// The different procedure types,
			if (type.Equals(ExternalStaticType)) {
				return connection.InvokeExternalStaticRoutine(routineName, location, returnType, paramTypes, parameters, owner);
			}

			throw new Exception("Unknown procedure type: " + type);
		}

		/// <summary>
		/// Invokes a static procedure.
		/// </summary>
		/// <param name="routineName"></param>
		/// <param name="locationStr"></param>
		/// <param name="returnType"></param>
		/// <param name="paramTypes"></param>
		/// <param name="paramValues"></param>
		/// <param name="owner"></param>
		/// <remarks>
		/// A type 1 procedure is represented by a single class with a 
		/// static invokation method (called Invoke). The parameters of 
		/// the static 'Invoke' method must be compatible class parameters 
		/// defined for the procedure, and the return class must also be
		/// compatible with the procedure return type.
		/// </remarks>
		/// <returns></returns>
		/// <exception cref="StatementException">
		/// If the Invoke method does not contain arguments that are compatible 
		/// with the parameters given or 
		/// </exception>
		/// <exception cref="SystemException">
		/// If the class contains more than a single public static <i>Invoke</i>
		/// method.
		/// </exception>
		private static TObject InvokeExternalStaticRoutine(this IDatabaseConnection connection, RoutineName routineName, string locationStr, TType returnType, TType[] paramTypes, TObject[] paramValues, string owner) {
			var extRoutineInfo = ExternalRoutineInfo.Parse(locationStr);

			// Search for the invokation method for this routine
			MethodInfo invokeMethod = extRoutineInfo.ResolveMethod(paramTypes);

			// Did we find an invoke method?
			if (invokeMethod == null) {
				throw new Exception("Could not find the invokation method for " +
									"the location string '" + locationStr + "'");
			}

			// Go through each argument of this class and work out how we are going
			// cast from the database engine object to the object.
			ParameterInfo[] methodParams = invokeMethod.GetParameters();

			// Is the first param a IProcedureConnection implementation?
			int startParam;
			Object[] values;
			if (methodParams.Length > 0 &&
				typeof(IProcedureConnection).IsAssignableFrom(methodParams[0].ParameterType)) {
				startParam = 1;
				values = new Object[paramTypes.Length + 1];
			} else {
				startParam = 0;
				values = new Object[paramTypes.Length];
			}

			// For each type    
			for (int i = 0; i < paramTypes.Length; ++i) {
				TObject value = paramValues[i];
				TType procType = paramTypes[i];
				Type parameterType = methodParams[i + startParam].ParameterType;
				String typeStr = parameterType.Name;

				// First null check,
				if (value.IsNull) {
					values[i + startParam] = null;
				} else {
					TType valueType = value.TType;
					// If not null, is the value and the procedure type compatible
					if (procType.IsComparableType(valueType)) {

						bool errorCast = false;
						Object castValue = null;

						// Compatible types,
						// Now we need to convert the parameter value into an object,
						if (valueType is TStringType) {
							// A String type can be represented as a System.String,
							// or as a System.IO.TextReader.
							var accessor = (IStringAccessor)value.Object;
							if (parameterType == typeof(String)) {
								castValue = accessor.ToString();
							} else if (parameterType == typeof(TextReader)) {
								castValue = accessor.GetTextReader();
							} else {
								errorCast = true;
							}
						} else if (valueType is TBooleanType) {
							if (parameterType == typeof(bool)) {
								castValue = value.Object;
							} else {
								errorCast = true;
							}
						} else if (valueType is TDateType) {
							DateTime d = (DateTime)value.Object;
							if (parameterType == typeof(DateTime)) {
								castValue = d;
							} else {
								errorCast = true;
							}
						} else if (valueType is TNumericType) {
							// Number can be cast to any one of the numeric types
							BigNumber num = (BigNumber)value.Object;
							if (parameterType == typeof(BigNumber)) {
								castValue = num;
							} else if (parameterType == typeof(byte)) {
								castValue = num.ToByte();
							} else if (parameterType == typeof(short)) {
								castValue = num.ToInt16();
							} else if (parameterType == typeof(int)) {
								castValue = num.ToInt32();
							} else if (parameterType == typeof(long)) {
								castValue = num.ToInt64();
							} else if (parameterType == typeof(float)) {
								castValue = num.ToSingle();
							} else if (parameterType == typeof(double)) {
								castValue = num.ToDouble();
							} else if (parameterType == typeof(decimal)) {
								castValue = num.ToBigDecimal();
							} else {
								errorCast = true;
							}
						} else if (valueType is TBinaryType) {
							// A binary type can translate to a System.IO.Stream or a
							// byte[] array.
							IBlobAccessor blob = (IBlobAccessor)value.Object;
							if (parameterType == typeof(Stream)) {
								castValue = blob.GetInputStream();
							} else if (parameterType == typeof(byte[])) {
								byte[] buf = new byte[blob.Length];
								try {
									Stream input = blob.GetInputStream();
									int n = 0;
									int len = blob.Length;
									while (len > 0) {
										int count = input.Read(buf, n, len);
										if (count == -1) {
											throw new IOException("End of stream.");
										}
										n += count;
										len -= count;
									}
								} catch (IOException e) {
									throw new Exception("IO Error: " + e.Message);
								}
								castValue = buf;
							} else {
								errorCast = true;
							}

						}

						// If the cast of the parameter was not possible, report the error.
						if (errorCast) {
							throw new StatementException("Unable to cast argument " + i +
														 " ... " + valueType.ToSqlString() + " to " + typeStr +
														 " for routine: " +
														 InfoString(routineName, returnType, paramTypes));
						}

						// Set the value for this parameter
						values[i + startParam] = castValue;

					} else {
						// The parameter is not compatible -
						throw new StatementException("Parameter (" + i + ") not compatible " +
													 value.TType.ToSqlString() + " -> " + procType.ToSqlString() +
													 " for procedure: " +
													 InfoString(routineName, returnType, paramTypes));
					}

				} // if not null

			} // for each parameter

			// Create the user that has the privs of this procedure.
			User privUser = new User(owner, connection.Database, "/Internal/Procedure/", DateTime.Now);

			// Create the IProcedureConnection object.
			IProcedureConnection procConnection = connection.CreateProcedureConnection(privUser);
			Object result;
			try {
				// Now the 'connection' will be set to the owner's user privs.

				// Set the IProcedureConnection object as an argument if necessary.
				if (startParam > 0) {
					values[0] = procConnection;
				}

				// The values array should now contain the parameter values formatted
				// as objects.

				// Invoke the method
				try {
					result = invokeMethod.Invoke(null, values);
				} catch (AccessViolationException e) {
					connection.Database.Context.Logger.Error(connection, e);
					throw new StatementException("Illegal access exception when invoking " +
												 "stored procedure: " + e.Message);
				} catch (TargetInvocationException e) {
					Exception realE = e.InnerException;
					connection.Database.Context.Logger.Error(connection, realE);
					throw new StatementException("Procedure Exception: " + realE.Message);
				}

			} finally {
				connection.DisposeProcedureConnection(procConnection);
			}

			// If return_type is null, there is no result from this procedure (void)
			if (returnType == null)
				return null;

			// Cast to a valid return object and return.
			return TObject.CreateAndCastFromObject(returnType, result);
		}
	}
}