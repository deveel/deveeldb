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
using System.Collections.Generic;
using System.IO;
using System.Linq;

using Deveel.Data.Serialization;

namespace Deveel.Data.Sql.Expressions {
	static class SqlExpressionSerializers {
		static SqlExpressionSerializers() {
			Resolver = new SqlExpressionSerializerResolver();
		}

		public static IObjectSerializerResolver Resolver { get; private set; }

		public static void Serialize(SqlExpression expression, BinaryWriter writer) {
			if (expression == null) {
				writer.Write((byte)0);
				return;
			}

			var expType = expression.GetType();
			var expTypeName = expType.FullName;

			var serializer = Resolver.ResolveSerializer(expType) as IObjectBinarySerializer;
			if (serializer == null)
				throw new InvalidOperationException(String.Format("Cannot find a valid binary serializer for expression of type '{0}'", expType));

			writer.Write((byte)1);
			writer.Write(expTypeName);

			serializer.Serialize(expression, writer);
		}

		public static SqlExpression Deserialize(BinaryReader reader) {
			var status = reader.ReadByte();
			if (status == 0)
				return null;

			var typeName = reader.ReadString();
#if PCL
			var type = Type.GetType(typeName, true);
#else
			var type = Type.GetType(typeName, true, true);
#endif

			var serializer = Resolver.ResolveSerializer(type) as IObjectBinarySerializer;
			if (serializer == null)
				throw new InvalidOperationException(String.Format("Cannot find a valid binary serializer for expression of type '{0}'", type));

			return (SqlExpression) serializer.Deserialize(reader);
		}

		#region SqlExpressionSerializerResolver

		class SqlExpressionSerializerResolver : ObjectSerializerProvider {
			protected override void Init() {
				Register<SqlConstantExpression, SqlConstantExpressionSerializer>();
				Register<SqlReferenceExpression, SqlReferenceExpressionSerializer>();
				Register<SqlVariableReferenceExpression, SqlVariableReferenceExpressionSerializer>();
				Register<SqlAssignExpression, SqlAssignExpressionSerializer>();
				Register<SqlFunctionCallExpression, SqlFunctionCallExpressionSerializer>();
				Register<SqlUnaryExpression, SqlUnaryExpressionSerializer>();
				Register<SqlBinaryExpression, SqlBinaryExpressionSerializer>();
				Register<SqlQueryExpression, SqlQueryExpressionSerializer>();
				Register<SqlConditionalExpression, SqlConditionalExpressionSerializer>();
				Register<SqlTupleExpression, SqlTupleExpressionSerializer>();
			}
		}

		#endregion

		#region SqlExpressionSerializer

		abstract class SqlExpressionSerializer<TExpression> : ObjectBinarySerializer<TExpression> where TExpression : SqlExpression {
			protected static void WriteExpression(SqlExpression expression, BinaryWriter writer) {
				SqlExpressionSerializers.Serialize(expression, writer);
			}

			protected static void WriteExpressions(SqlExpression[] expressions, BinaryWriter writer) {
				var expCount = expressions == null ? 0 : expressions.Length;

				writer.Write(expCount);

				if (expressions != null) {
					for (int i = 0; i < expCount; i++) {
						WriteExpression(expressions[i], writer);
					}
				}
			}

			protected static SqlExpression ReadExpression(BinaryReader reader) {
				return SqlExpressionSerializers.Deserialize(reader);
			}

			protected static SqlExpression[] ReadExpressions(BinaryReader reader) {
				var expCount = reader.ReadInt32();

				var exps = new SqlExpression[expCount];
				for (int i = 0; i < expCount; i++) {
					exps[i] = ReadExpression(reader);
				}

				return exps;
			}
		}

		#endregion

		#region SqlConstantExpressionSerializer

		class SqlConstantExpressionSerializer : SqlExpressionSerializer<SqlConstantExpression> {
			public override void Serialize(SqlConstantExpression expression, BinaryWriter writer) {
				DataObject.Serialize(expression.Value, writer);
			}

			public override SqlConstantExpression Deserialize(BinaryReader reader) {
				var value = DataObject.Deserialize(reader, null);
				return SqlExpression.Constant(value);
			}
		}

		#endregion

		#region SqlReferenceExpressionSerializer

		class SqlReferenceExpressionSerializer : SqlExpressionSerializer<SqlReferenceExpression> {
			public override void Serialize(SqlReferenceExpression expression, BinaryWriter writer) {
				ObjectName.Serialize(expression.ReferenceName, writer);
			}

			public override SqlReferenceExpression Deserialize(BinaryReader reader) {
				var name = ObjectName.Deserialize(reader);
				return SqlExpression.Reference(name);
			}
		}

		#endregion

		#region SqlVariableReferenceExpressionSerializer

		class SqlVariableReferenceExpressionSerializer : SqlExpressionSerializer<SqlVariableReferenceExpression> {
			public override void Serialize(SqlVariableReferenceExpression expression, BinaryWriter writer) {
				writer.Write(expression.VariableName);
			}

			public override SqlVariableReferenceExpression Deserialize(BinaryReader reader) {
				var varName = reader.ReadString();
				return SqlExpression.VariableReference(varName);
			}
		}

		#endregion

		#region SqlAssignExpressionSerializer

		class SqlAssignExpressionSerializer : SqlExpressionSerializer<SqlAssignExpression> {
			public override void Serialize(SqlAssignExpression expression, BinaryWriter writer) {
				SqlExpressionSerializers.Serialize(expression.Reference, writer);
				SqlExpressionSerializers.Serialize(expression.ValueExpression, writer);
			}

			public override SqlAssignExpression Deserialize(BinaryReader reader) {
				var reference = SqlExpressionSerializers.Deserialize(reader);
				var value = SqlExpressionSerializers.Deserialize(reader);

				return SqlExpression.Assign(reference, value);
			}
		}

		#endregion

		#region SqlFunctionCallExpressionSerializer

		class SqlFunctionCallExpressionSerializer : SqlExpressionSerializer<SqlFunctionCallExpression> {
			public override void Serialize(SqlFunctionCallExpression expression, BinaryWriter writer) {
				ObjectName.Serialize(expression.FunctioName, writer);
				WriteExpressions(expression.Arguments, writer);

			}

			public override SqlFunctionCallExpression Deserialize(BinaryReader reader) {
				var functionName = ObjectName.Deserialize(reader);
				var args = ReadExpressions(reader);

				return SqlExpression.FunctionCall(functionName, args);
			}
		}

		#endregion

		#region SqlUnaryExpressionSerializer

		class SqlUnaryExpressionSerializer : SqlExpressionSerializer<SqlUnaryExpression> {
			public override void Serialize(SqlUnaryExpression expression, BinaryWriter writer) {
				writer.Write((byte)expression.ExpressionType);
				WriteExpression(expression.Operand, writer);
			}

			public override SqlUnaryExpression Deserialize(BinaryReader reader) {
				var expType = (SqlExpressionType) reader.ReadByte();
				var exp = ReadExpression(reader);

				return SqlExpression.Unary(expType, exp);
			}
		}

		#endregion

		#region SqlBinaryExpressionSerializer

		class SqlBinaryExpressionSerializer : SqlExpressionSerializer<SqlBinaryExpression> {
			public override void Serialize(SqlBinaryExpression expression, BinaryWriter writer) {
				WriteExpression(expression.Left, writer);
				writer.Write((byte)expression.ExpressionType);
				WriteExpression(expression.Right, writer);
			}

			public override SqlBinaryExpression Deserialize(BinaryReader reader) {
				var left = ReadExpression(reader);
				var exptype = (SqlExpressionType) reader.ReadByte();
				var right = ReadExpression(reader);

				return SqlExpression.Binary(left, exptype, right);
			}
		}

		#endregion

		#region SqlQueryExpressionSerializer

		class SqlQueryExpressionSerializer : SqlExpressionSerializer<SqlQueryExpression> {
			public override void Serialize(SqlQueryExpression expression, BinaryWriter writer) {
				SerializeSelectColumns(expression.SelectColumns, writer);
				
				writer.Write(expression.Distinct ? (byte)1 : (byte)0);

				if (expression.FromClause != null) {
					writer.Write((byte) 1);
					FromClause.Serialize(expression.FromClause, writer);
				} else {
					writer.Write((byte)0);
				}

				if (expression.WhereExpression != null) {
					writer.Write((byte) 1);
					SqlExpression.Serialize(expression.WhereExpression, writer);
				} else {
					writer.Write((byte)0);
				}

				if (expression.GroupBy != null) {
					throw new NotImplementedException();
				}

				if (expression.GroupMax != null) {
					writer.Write((byte) 1);
					ObjectName.Serialize(expression.GroupMax, writer);
				} else {
					writer.Write((byte)0);
				}

				if (expression.HavingExpression != null) {
					writer.Write((byte) 1);
					SqlExpression.Serialize(expression.HavingExpression, writer);
				} else {
					writer.Write((byte)0);
				}

				// TODO: Composites!!
			}

			private void SerializeSelectColumns(IEnumerable<SelectColumn> selectColumns, BinaryWriter writer) {
				var list = selectColumns == null ? new List<SelectColumn>() : selectColumns.ToList();
				writer.Write(list.Count);
				for (int i = 0; i < list.Count; i++) {
					var column = list[i];
					SelectColumn.Serialize(column, writer);
				}
			}

			public override SqlQueryExpression Deserialize(BinaryReader reader) {
				var selectColumns = DeserializeSelectColumns(reader);

				var queryExp = new SqlQueryExpression(selectColumns);

				var isDistinct = reader.ReadByte() == 1;
				queryExp.Distinct = isDistinct;

				var hasFrom = reader.ReadByte() == 1;
				if (hasFrom) {
					queryExp.FromClause = FromClause.Deserialize(reader);
				}

				var hasWhere = reader.ReadByte() == 1;
				if (hasWhere)
					queryExp.WhereExpression = SqlExpression.Deserialize(reader);

				var hasGroupMax = reader.ReadByte() == 1;
				if (hasGroupMax)
					queryExp.GroupMax = ObjectName.Deserialize(reader);

				var hasHaving = reader.ReadByte() == 1;
				if (hasHaving)
					queryExp.HavingExpression = SqlExpression.Deserialize(reader);

				return queryExp;
			}

			private IEnumerable<SelectColumn> DeserializeSelectColumns(BinaryReader reader) {
				var count = reader.ReadInt32();
				var columns = new SelectColumn[count];
				for (int i = 0; i < count; i++) {
					columns[i] = SelectColumn.Deserialize(reader);
				}

				return columns;
			}
		}

		#endregion

		#region SqlConditionalExpressionSerializer

		class SqlConditionalExpressionSerializer : SqlExpressionSerializer<SqlConditionalExpression> {
			public override void Serialize(SqlConditionalExpression expression, BinaryWriter writer) {
				WriteExpression(expression.TestExpression, writer);
				WriteExpression(expression.TrueExpression, writer);
				WriteExpression(expression.FalseExpression, writer);
			}

			public override SqlConditionalExpression Deserialize(BinaryReader reader) {
				var testExp = ReadExpression(reader);
				var trueExp = ReadExpression(reader);
				var falseExp = ReadExpression(reader);

				return SqlExpression.Conditional(testExp, trueExp, falseExp);
			}
		}

		#endregion

		#region SqlTupleExpressionSerializer

		class SqlTupleExpressionSerializer : SqlExpressionSerializer<SqlTupleExpression> {
			public override void Serialize(SqlTupleExpression expression, BinaryWriter writer) {
				WriteExpressions(expression.Expressions, writer);
			}

			public override SqlTupleExpression Deserialize(BinaryReader reader) {
				var exps = ReadExpressions(reader);
				return SqlExpression.Tuple(exps);
			}
		}

		#endregion
	}
}
