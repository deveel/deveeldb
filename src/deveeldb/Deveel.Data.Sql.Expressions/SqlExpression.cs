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
using System.IO;
using System.Text;

using Deveel.Data;
using Deveel.Data.Sql.Parser;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Expressions {
	/// <summary>
	/// Defines the base class for instances that represent SQL expression tree nodes.
	/// </summary>
	/// <remarks>
	/// The architecture of the SQL Expression domain is to keep the implementation
	/// internal to the project, that means it will be possible to construct expressions
	/// only through this class, calling factory methods (for example <see cref="Binary"/>).
	/// </remarks>
	public abstract class SqlExpression {
		private int precedence;

		/// <summary>
		/// Internally constructs the SQL expression, avoiding external implementations
		/// to be allowed to inherit this class.
		/// </summary>
		internal SqlExpression() {
		}

		/// <summary>
		/// Gets the type code of this SQL expression.
		/// </summary>
		public abstract SqlExpressionType ExpressionType { get; }

		/// <summary>
		/// Gets a value indicating whether the expression can be evaluated
		/// to another simpler one.
		/// </summary>
		/// <seealso cref="Evaluate(EvaluateContext)"/>
		public virtual bool CanEvaluate {
			get { return false; }
		}

		internal int EvaluatePrecedence {
			get {
				if (precedence > 0)
					return precedence;

				// Primary
				if (ExpressionType == SqlExpressionType.Reference ||
				    ExpressionType == SqlExpressionType.FunctionCall ||
					ExpressionType == SqlExpressionType.Constant)
					return 150;

				// Unary
				if (ExpressionType == SqlExpressionType.UnaryPlus ||
				    ExpressionType == SqlExpressionType.Negate ||
				    ExpressionType == SqlExpressionType.Not)
					return 140;

				// Cast
				if (ExpressionType == SqlExpressionType.Cast)
					return 139;

				// Multiplicative
				if (ExpressionType == SqlExpressionType.Multiply ||
				    ExpressionType == SqlExpressionType.Divide ||
				    ExpressionType == SqlExpressionType.Modulo)
					return 130;

				// Additive
				if (ExpressionType == SqlExpressionType.Add ||
				    ExpressionType == SqlExpressionType.Subtract)
					return 120;

				// Relational
				if (ExpressionType == SqlExpressionType.GreaterThan ||
				    ExpressionType == SqlExpressionType.GreaterOrEqualThan ||
				    ExpressionType == SqlExpressionType.SmallerThan ||
				    ExpressionType == SqlExpressionType.SmallerOrEqualThan ||
					ExpressionType == SqlExpressionType.Is ||
					ExpressionType == SqlExpressionType.IsNot ||
					ExpressionType == SqlExpressionType.Like ||
					ExpressionType == SqlExpressionType.NotLike)
					return 110;

				// Equality
				if (ExpressionType == SqlExpressionType.Equal ||
				    ExpressionType == SqlExpressionType.NotEqual)
					return 100;

				// Logical
				if (ExpressionType == SqlExpressionType.And)
					return 90;
				if (ExpressionType == SqlExpressionType.Or)
					return 89;
				if (ExpressionType == SqlExpressionType.XOr)
					return 88;

				if (ExpressionType == SqlExpressionType.Conditional)
					return 80;

				if (ExpressionType == SqlExpressionType.Assign)
					return 70;

				if (ExpressionType == SqlExpressionType.Tuple)
					return 60;

				return -1;
			}
			set { precedence = value; }
		}

		public virtual SqlExpression Prepare(IExpressionPreparer preparer) {
			var visitor = new PreparerVisitor(preparer);
			return visitor.Visit(this);
		}

		public virtual SqlExpression Accept(SqlExpressionVisitor visitor) {
			return visitor.Visit(this);
		}

		/// <summary>
		/// When overridden by a derived class, this method evaluates the expression
		/// within the provided context.
		/// </summary>
		/// <param name="context">The context for the evaluation of the expression, providing
		/// access to the system or to the execution context.</param>
		/// <remarks>
		/// <para>
		/// This method is only executed is <see cref="CanEvaluate"/> is <c>true</c>, and the
		/// override method can reduce this expression to a simpler form.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns a new <seealso cref="SqlExpression"/> that is the result of the
		/// evaluation of this expression, within the context given.
		/// </returns>
		/// <exception cref="ExpressionEvaluateException">
		/// If any error occurred while evaluating the expression.
		/// </exception>
		public virtual SqlExpression Evaluate(EvaluateContext context) {
			var visitor = new ExpressionEvaluatorVisitor(context);
			return visitor.Visit(this);
		}


		/// <summary>
		/// Statically evaluates the expression, outside any context.
		/// </summary>
		/// <para>
		/// This overload of the <c>Evaluate</c> logic provides an empty context
		/// to <see cref="Evaluate(EvaluateContext)"/>, so that dynamic resolutions
		/// (eg. function calls, states assessments, etc.) will throw an exception.
		/// </para>
		/// <para>
		/// Care must be taken when calling this method, that the expression tree
		/// represented does not contain any reference to dynamically resolved
		/// expressions (<see cref="SqlFunctionCallExpression"/> for example), otherwise
		/// its evaluation will result in an exception state.
		/// </para>
		/// <returns>
		/// Returns a new <seealso cref="SqlExpression"/> that is the result of the
		/// static evaluation of this expression.
		/// </returns>
		/// <exception cref="ExpressionEvaluateException">
		/// If any error occurred while evaluating the expression.
		/// </exception>
		public SqlExpression Evaluate() {
			return Evaluate(null, null);
		}

		public SqlExpression Evaluate(IQueryContext context, IVariableResolver variables) {
			return Evaluate(context, variables, null);
		}

		public SqlExpression Evaluate(IQueryContext context, IVariableResolver variables, IGroupResolver group) {
			return Evaluate(new EvaluateContext(context, variables, group));
		}

		public override string ToString() {
			var builder = new ExpressionStringBuilder();
			return builder.ToSqlString(this);
		}

		/// <summary>
		/// Parses the given SQL string to an expression that can be evaluated.
		/// </summary>
		/// <param name="s">The string to parse.</param>
		/// <param name="context"></param>
		/// <returns>
		/// Returns an instance of <seealso cref="SqlExpression"/> that represents
		/// the given SQL string parsed.
		/// </returns>
		public static SqlExpression Parse(string s) {
			return Parse(s, null);
		}

		/// <summary>
		/// Parses the given SQL string to an expression that can be evaluated.
		/// </summary>
		/// <param name="s">The string to parse.</param>
		/// <param name="context"></param>
		/// <returns>
		/// Returns an instance of <seealso cref="SqlExpression"/> that represents
		/// the given SQL string parsed.
		/// </returns>
		public static SqlExpression Parse(string s, ISystemContext context) {
			try {
				// TODO: Get the expression compiler from the context
				var compiler = SqlParsers.Expression;
				var result = compiler.Parse(s);

				if (result.HasErrors)
					throw new SqlParseException();

				var expNode = result.RootNode as IExpressionNode;
				if (expNode == null)
					throw new SqlExpressionException(ExpressionErrorCodes.CannotParse, "The parse of the text did not result into an expression.");

				return ExpressionBuilder.Build(expNode);
			} catch (SqlParseException ex) {
				throw new SqlExpressionException(ExpressionErrorCodes.CannotParse,
					"Could not parse input expression: see inner exception for details.", ex);
			}
		}

		#region Factory Methods 

		#region Primary

		public static SqlConstantExpression Constant(object value) {
			return Constant(DataObject.Create(value));
		}

		public static SqlConstantExpression Constant(DataObject value) {
			return new SqlConstantExpression(value);
		}

		public static SqlCastExpression Cast(SqlExpression value, SqlType destType) {
			return new SqlCastExpression(value, destType);
		}

		public static SqlFunctionCallExpression FunctionCall(ObjectName functionName) {
			return FunctionCall(functionName, new SqlExpression[0]);
		}

		public static SqlFunctionCallExpression FunctionCall(ObjectName functionName, SqlExpression[] args) {
			return new SqlFunctionCallExpression(functionName, args);
		}

		public static SqlFunctionCallExpression FunctionCall(string functionName) {
			return FunctionCall(functionName, new SqlExpression[0]);
		}

		public static SqlFunctionCallExpression FunctionCall(string functionName, SqlExpression[] args) {
			return FunctionCall(ObjectName.Parse(functionName), args);
		}

		public static SqlReferenceExpression Reference(ObjectName objectName) {
			return new SqlReferenceExpression(objectName);
		}

		public static SqlVariableReferenceExpression VariableReference(string varName) {
			return new SqlVariableReferenceExpression(varName);
		}
 
		#endregion

		public static SqlConditionalExpression Conditional(SqlExpression testExpression, SqlExpression ifTrue) {
			return Conditional(testExpression, ifTrue, null);
		}

		public static SqlConditionalExpression Conditional(SqlExpression testExpression, SqlExpression ifTrue, SqlExpression ifFalse) {
			return new SqlConditionalExpression(testExpression, ifTrue, ifFalse);
		}

		#region Binary Expressions

		public static SqlBinaryExpression Binary(SqlExpression left, SqlExpressionType expressionType, SqlExpression right) {
			if (expressionType == SqlExpressionType.Add)
				return Add(left, right);
			if (expressionType == SqlExpressionType.Subtract)
				return Subtract(left, right);
			if (expressionType == SqlExpressionType.Multiply)
				return Multiply(left, right);
			if (expressionType == SqlExpressionType.Divide)
				return Divide(left, right);
			if (expressionType == SqlExpressionType.Modulo)
				return Modulo(left, right);

			if (expressionType == SqlExpressionType.Equal)
				return Equal(left, right);
			if (expressionType == SqlExpressionType.NotEqual)
				return NotEqual(left, right);
			if (expressionType == SqlExpressionType.Is)
				return Is(left, right);
			if (expressionType == SqlExpressionType.IsNot)
				return IsNot(left, right);
			if (expressionType == SqlExpressionType.GreaterThan)
				return GreaterThan(left, right);
			if (expressionType == SqlExpressionType.GreaterOrEqualThan)
				return GreaterOrEqualThan(left, right);
			if (expressionType == SqlExpressionType.SmallerThan)
				return SmallerThan(left, right);
			if (expressionType == SqlExpressionType.SmallerOrEqualThan)
				return SmallerOrEqualThan(left, right);

			if (expressionType == SqlExpressionType.Like)
				return Like(left, right);
			if (expressionType == SqlExpressionType.NotLike)
				return NotLike(left, right);

			if (expressionType == SqlExpressionType.And)
				return And(left, right);
			if (expressionType == SqlExpressionType.Or)
				return Or(left, right);
			if (expressionType == SqlExpressionType.XOr)
				return XOr(left, right);

			if (expressionType.IsAny())
				return Any(left, expressionType.SubQueryPlainType(), right);
			if (expressionType.IsAll())
				return All(left, expressionType.SubQueryPlainType(), right);

			throw new ArgumentException(String.Format("Expression type {0} is not a Binary", expressionType));
		}

		public static SqlBinaryExpression Equal(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Equal, right);
		}

		public static SqlBinaryExpression NotEqual(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.NotEqual, right);
		}

		public static SqlBinaryExpression Is(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Is, right);
		}

		public static SqlBinaryExpression IsNot(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.IsNot, right);
		}

		public static SqlBinaryExpression SmallerOrEqualThan(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.SmallerOrEqualThan, right);
		}

		public static SqlBinaryExpression GreaterOrEqualThan(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.GreaterOrEqualThan, right);
		}

		public static SqlBinaryExpression SmallerThan(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.SmallerThan, right);
		}

		public static SqlBinaryExpression GreaterThan(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.GreaterThan, right);
		}

		public static SqlBinaryExpression Like(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Like, right);
		}

		public static SqlBinaryExpression NotLike(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.NotLike, right);
		}

		public static SqlBinaryExpression And(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.And, right);
		}

		public static SqlBinaryExpression Or(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Or, right);
		}

		public static SqlBinaryExpression XOr(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.XOr, right);
		}

		public static SqlBinaryExpression Add(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Add, right);
		}

		public static SqlBinaryExpression Subtract(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Subtract, right);
		}

		public static SqlBinaryExpression Multiply(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Multiply, right);
		}

		public static SqlBinaryExpression Divide(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Divide, right);
		}

		public static SqlBinaryExpression Modulo(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Modulo, right);
		}

		public static SqlBinaryExpression Any(SqlExpression left, SqlExpressionType anyType, SqlExpression right) {
			if (anyType == SqlExpressionType.Equal ||
			    anyType == SqlExpressionType.AnyEqual)
				return AnyEqual(left, right);
			if (anyType == SqlExpressionType.NotEqual ||
			    anyType == SqlExpressionType.AnyNotEqual)
				return AnyNotEqual(left, right);
			if (anyType == SqlExpressionType.AnyGreaterThan ||
			    anyType == SqlExpressionType.GreaterThan)
				return AnyGreaterThan(left, right);
			if (anyType == SqlExpressionType.SmallerThan ||
			    anyType == SqlExpressionType.AnySmallerThan)
				return AnySmallerThan(left, right);
			if (anyType == SqlExpressionType.GreaterOrEqualThan ||
			    anyType == SqlExpressionType.AnyGreaterOrEqualThan)
				return AnyGreaterOrEqualThan(left, right);
			if (anyType == SqlExpressionType.GreaterOrEqualThan ||
			    anyType == SqlExpressionType.AnySmallerOrEqualThan)
				return AnySmallerOrEqualThan(left, right);

			throw new ArgumentException(String.Format("The type '{0}' cannot be part of an ANY operator.", anyType));
		}

		public static SqlBinaryExpression AnyEqual(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.AnyEqual, right);
		}

		public static SqlBinaryExpression AnyNotEqual(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.AnyNotEqual, right);
		}

		public static SqlBinaryExpression AnyGreaterThan(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.AnyGreaterThan, right);
		}

		public static SqlBinaryExpression AnyGreaterOrEqualThan(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.AnyGreaterOrEqualThan, right);
		}

		public static SqlBinaryExpression AnySmallerThan(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.AnySmallerThan, right);
		}

		public static SqlBinaryExpression AnySmallerOrEqualThan(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.AnySmallerOrEqualThan, right);
		}

		public static SqlBinaryExpression All(SqlExpression left, SqlExpressionType allType, SqlExpression right) {
			if (allType == SqlExpressionType.Equal ||
			    allType == SqlExpressionType.AllEqual)
				return AllEqual(left, right);
			if (allType == SqlExpressionType.NotEqual ||
			    allType == SqlExpressionType.AllNotEqual)
				return AllNotEqual(left, right);
			if (allType == SqlExpressionType.GreaterThan ||
			    allType == SqlExpressionType.AllGreaterThan)
				return AllGreaterThan(left, right);
			if (allType == SqlExpressionType.SmallerThan ||
			    allType == SqlExpressionType.AllSmallerThan)
				return AllSmallerThan(left, right);
			if (allType == SqlExpressionType.GreaterOrEqualThan ||
			    allType == SqlExpressionType.AllGreaterOrEqualThan)
				return AllGreaterOrEqualThan(left, right);
			if (allType == SqlExpressionType.SmallerOrEqualThan ||
			    allType == SqlExpressionType.AllSmallerOrEqualThan)
				return AllSmallerOrEqualThan(left, right);

			throw new ArgumentException(String.Format("The type '{0}' cannot be part of an ALL operator.", allType));
		}

		public static SqlBinaryExpression AllEqual(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.AllEqual, right);
		}

		public static SqlBinaryExpression AllNotEqual(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.AllNotEqual, right);
		}

		public static SqlBinaryExpression AllGreaterThan(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.AllGreaterThan, right);
		}

		public static SqlBinaryExpression AllGreaterOrEqualThan(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.AllGreaterOrEqualThan, right);
		}

		public static SqlBinaryExpression AllSmallerThan(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.AllSmallerThan, right);
		}

		public static SqlBinaryExpression AllSmallerOrEqualThan(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.AllSmallerOrEqualThan, right);
		}

		#endregion

		#region Unary Expressions

		public static SqlUnaryExpression Unary(SqlExpressionType expressionType, SqlExpression operand) {
			if (expressionType == SqlExpressionType.UnaryPlus)
				return UnaryPlus(operand);
			if (expressionType == SqlExpressionType.Negate)
				return Negate(operand);
			if (expressionType == SqlExpressionType.Not)
				return Not(operand);

			throw new ArgumentException(String.Format("Expression Type {0} is not an Unary.", expressionType));
		}

		public static SqlUnaryExpression Not(SqlExpression operand) {
			return new SqlUnaryExpression(SqlExpressionType.Not, operand);
		}

		public static SqlUnaryExpression Negate(SqlExpression operand) {
			return new SqlUnaryExpression(SqlExpressionType.Negate, operand);
		}

		public static SqlUnaryExpression UnaryPlus(SqlExpression operand) {
			return new SqlUnaryExpression(SqlExpressionType.UnaryPlus, operand);
		}

		#endregion

		public static SqlAssignExpression Assign(SqlExpression reference, SqlExpression valueExpression) {
			return new SqlAssignExpression(reference, valueExpression);
		}


		public static SqlTupleExpression Tuple(SqlExpression[] expressions) {
			return new SqlTupleExpression(expressions);
		}

		public static SqlTupleExpression Tuple(SqlExpression expr1, SqlExpression exp2) {
			return Tuple(new[] {expr1, exp2});
		}

		public static SqlTupleExpression Tuple(SqlExpression expr1, SqlExpression expr2, SqlExpression expr3) {
			return Tuple(new[] {expr1, expr2, expr3});
		}

		#endregion

		public static void Serialize(SqlExpression expression, Stream stream) {
			Serialize(expression, stream, Encoding.Unicode);
		}

		public static void Serialize(SqlExpression expression, Stream stream, Encoding encoding) {
			var writer = new BinaryWriter(stream, encoding);
			Serialize(expression, writer);
		}

		public static SqlExpression Deserialize(Stream stream) {
			return Deserialize(stream, Encoding.Unicode);
		}

		public static SqlExpression Deserialize(Stream stream, Encoding encoding) {
			var reader = new BinaryReader(stream, encoding);
			return Deserialize(reader);
		}

		public static void Serialize(SqlExpression expression, BinaryWriter writer) {
			SqlExpressionSerializers.Serialize(expression, writer);
		}

		public static SqlExpression Deserialize(BinaryReader reader) {
			return SqlExpressionSerializers.Deserialize(reader);
		}

		public static byte[] Serialize(SqlExpression[] expressions) {
			using (var stream = new MemoryStream()) {
				Serialize(expressions, stream);
				return stream.ToArray();
			}
		}

		public static void Serialize(SqlExpression[] expressions, Stream stream) {
			using (var writer = new BinaryWriter(stream, Encoding.Unicode)) {
				Serialize(expressions, writer);
			}
		}

		public static void Serialize(SqlExpression[] expressions, BinaryWriter writer) {
			int argc = expressions == null ? 0 : expressions.Length;
			if (expressions != null) {
				for (int i = 0; i < argc; i++) {
					Serialize(expressions[i], writer);
				}
			}
		}
	}
}