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
using System.Linq;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Compile;

namespace Deveel.Data.Sql.Expressions {
	/// <summary>
	/// Defines the base class for instances that represent SQL expression tree nodes.
	/// </summary>
	/// <remarks>
	/// The architecture of the SQL Expression domain is to keep the implementation
	/// internal to the project, that means it will be possible to construct expressions
	/// only through this class, calling factory methods (for example <see cref="Binary"/>).
	/// </remarks>
	[Serializable]
	public abstract class SqlExpression {
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
				// TODO: support XOR?

				if (ExpressionType == SqlExpressionType.Conditional)
					return 80;

				if (ExpressionType == SqlExpressionType.Assign)
					return 70;

				return -1;
			}
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
			return this;
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

		/// <summary>
		/// Parses the given SQL string to an expression that can be evaluated.
		/// </summary>
		/// <param name="s">The string to parse.</param>
		/// <returns>
		/// Returns an instance of <seealso cref="SqlExpression"/> that represents
		/// the given SQL string parsed.
		/// </returns>
		public static SqlExpression Parse(string s) {
			try {
				var compiler = SqlParsers.Expression;
				var result = compiler.Parse(s);

				if (result.HasErrors)
					throw new SqlParseException();

				var visitor = new ExpressionBuilder();
				return visitor.Build(result.RootNode);
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

			throw new ArgumentException(String.Format("Expression type {0} is not a Binary", expressionType));
		}

		public static SqlBinaryExpression Equal(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Equal, right, BinaryOperator.Equal);
		}

		public static SqlBinaryExpression NotEqual(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.NotEqual, right, BinaryOperator.NotEqual);
		}

		public static SqlBinaryExpression Is(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Is, right, BinaryOperator.Is);
		}

		public static SqlBinaryExpression IsNot(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.IsNot, right, BinaryOperator.IsNot);
		}

		public static SqlBinaryExpression SmallerOrEqualThan(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.SmallerOrEqualThan, right, BinaryOperator.SmallerOrEqualThan);
		}

		public static SqlBinaryExpression GreaterOrEqualThan(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.GreaterOrEqualThan, right, BinaryOperator.GreaterOrEqualThan);
		}

		public static SqlBinaryExpression SmallerThan(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.SmallerThan, right, BinaryOperator.SmallerThan);
		}

		public static SqlBinaryExpression GreaterThan(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.GreaterThan, right, BinaryOperator.GreaterThan);
		}

		public static SqlBinaryExpression Like(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Like, right, BinaryOperator.Like);
		}

		public static SqlBinaryExpression NotLike(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.NotLike, right, BinaryOperator.NotLike);
		}

		public static SqlBinaryExpression And(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.And, right, BinaryOperator.And);
		}

		public static SqlBinaryExpression Or(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Or, right, BinaryOperator.Or);
		}

		public static SqlBinaryExpression XOr(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.XOr, right, BinaryOperator.XOr);
		}

		public static SqlBinaryExpression Add(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Add, right, BinaryOperator.Add);
		}

		public static SqlBinaryExpression Subtract(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Subtract, right, BinaryOperator.Subtract);
		}

		public static SqlBinaryExpression Multiply(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Multiply, right, BinaryOperator.Multiply);
		}

		public static SqlBinaryExpression Divide(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Divide, right, BinaryOperator.Divide);
		}

		public static SqlBinaryExpression Modulo(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Modulo, right, BinaryOperator.Modulo);
		}

		public static SqlBinaryExpression AnyEqual(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.AnyEqual, right, BinaryOperator.AnyEqual);
		}

		public static SqlBinaryExpression AnyNotEqual(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.AnyNotEqual, right, BinaryOperator.AnyNotEqual);
		}

		public static SqlBinaryExpression AnyGreaterThan(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.AnyGreaterThan, right, BinaryOperator.AnyGreaterThan);
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

			throw new ArgumentException(String.Format("Epression Type {0} is not an Unary.", expressionType));
		}

		public static SqlUnaryExpression Not(SqlExpression operand) {
			return new SqlUnaryExpression(SqlExpressionType.Not, operand, o => o.Negate());
		}

		public static SqlUnaryExpression Negate(SqlExpression operand) {
			return new SqlUnaryExpression(SqlExpressionType.Negate, operand, o => o.Negate());
		}

		public static SqlUnaryExpression UnaryPlus(SqlExpression operand) {
			throw new NotImplementedException();
		}

		#endregion

		public static SqlAssignExpression Assign(string reference, SqlExpression valueExpression) {
			return Assign(VariableReference(reference), valueExpression);
		}

		public static SqlAssignExpression Assign(SqlVariableReferenceExpression reference, SqlExpression expression) {
			return new SqlAssignExpression(reference, expression);
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

		public string ToSqlString() {
			throw new NotImplementedException();
		}

		public void SerializeTo(Stream stream) {
			throw new NotImplementedException();
		}
	}
}