using System;
using System.IO;

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
			var type = Type.GetType(typeName, true)
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
				throw new NotImplementedException();
			}

			public override SqlQueryExpression Deserialize(BinaryReader reader) {
				throw new NotImplementedException();
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
