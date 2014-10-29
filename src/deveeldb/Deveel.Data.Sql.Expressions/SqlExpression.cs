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

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Compile;

namespace Deveel.Data.Sql.Expressions {
	[Serializable]
	public abstract class SqlExpression {
		public abstract SqlExpressionType ExpressionType { get; }

		/// <summary>
		/// Gets a value indicating whether the expression can be evaluated
		/// to another simpler one.
		/// </summary>
		/// <seealso cref="Evaluate"/>
		public virtual bool CanEvaluate {
			get { return false; }
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
		public virtual SqlExpression Evaluate(EvaluateContext context) {
			return this;
		}

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
		/// <exception cref="SqlExpressionException">
		/// If an error occurrs while parsing the string or if it's not possible
		/// to construct the <see cref="SqlExpression"/> from the parsed <see cref="IExpressionNode"/>
		/// returned by the <see cref="SqlCompiler"/> used to parse the expression.
		/// </exception>
		/// <seealso cref="IExpressionNode"/>
		/// <seealso cref="SqlCompiler"/>
		public static SqlExpression Parse(string s) {
			try {
				var compiler = new SqlCompiler(true);
				var expressionNode = compiler.CompileExpression(s);

				var visitor = new ExpressionConvertVisitor(expressionNode);
				return visitor.Convert();
			} catch (SqlParseException ex) {
				throw new SqlExpressionException(ExpressionErrorCodes.CannotParse,
					"Could not parse input expression: see inner exception for details.", ex);
			}
		}

		#region Factory Methods 

		public static SqlConstantExpression Constant(DataObject value) {
			return new SqlConstantExpression(value);
		}

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

			if (expressionType == SqlExpressionType.BitwiseAnd)
				return BitwiseAnd(left, right);
			if (expressionType == SqlExpressionType.BitwiseOr)
				return BitwiseOr(left, right);

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

			if (expressionType == SqlExpressionType.LogicalAnd)
				return LogicalAnd(left, right);
			if (expressionType == SqlExpressionType.LogicalOr)
				return LogicalOr(left, right);

			throw new ArgumentException(String.Format("Expression type {0} is not a Binary", expressionType));
		}

		public static SqlBinaryExpression Equal(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Equal, right, (obj1, obj2) => obj1.IsEqualTo(obj2));
		}

		public static SqlBinaryExpression NotEqual(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.NotEqual, right, (obj1, obj2) => obj1.IsNotEqualTo(obj2));
		}

		public static SqlBinaryExpression Is(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Is, right, (obj1, obj2) => obj1.Is(obj2));
		}

		public static SqlBinaryExpression IsNot(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.IsNot, right, (obj1, obj2) => obj1.IsNot(obj2));
		}

		public static SqlBinaryExpression SmallerOrEqualThan(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.SmallerOrEqualThan, right, (obj1, obj2) => obj1.IsSmallerOrEqualThan(obj2));
		}

		public static SqlBinaryExpression GreaterOrEqualThan(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.GreaterOrEqualThan, right, (obj1, obj2) => obj1.IsGreterOrEqualThan(obj2));
		}

		public static SqlBinaryExpression SmallerThan(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.SmallerThan, right, (obj1, obj2) => obj1.IsSmallerThan(obj2));
		}

		public static SqlBinaryExpression GreaterThan(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.GreaterThan, right, (obj1, obj2) => obj1.IsGreaterThan(obj2));
		}

		public static SqlBinaryExpression LogicalAnd(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.LogicalAnd, right, (obj1, obj2) => obj1.And(obj2));
		}

		public static SqlBinaryExpression LogicalOr(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.LogicalOr, right, (obj1, obj2) => obj1.Or(obj2));
		}

		public static SqlBinaryExpression BitwiseAnd(SqlExpression left, SqlExpression right) {
			throw new NotImplementedException();
		}

		public static SqlBinaryExpression BitwiseOr(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.BitwiseOr, right, (obj1, obj2) => obj1.XOr(obj2));
		}

		public static SqlBinaryExpression Add(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Add, right, (obj1, obj2) => obj1.Add(obj2));
		}

		public static SqlBinaryExpression Subtract(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Subtract, right, (obj1, obj2) => obj1.Subtract(obj2));
		}

		public static SqlBinaryExpression Multiply(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Multiply, right, (obj1, obj2) => obj1.Multiply(obj2));
		}

		public static SqlBinaryExpression Divide(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Divide, right, (obj1, obj2) => obj1.Divide(obj2));
		}

		public static SqlBinaryExpression Modulo(SqlExpression left, SqlExpression right) {
			return new SqlBinaryExpression(left, SqlExpressionType.Modulo, right, (obj1, obj2) => obj1.Modulus(obj2));
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

		#endregion
	}
}