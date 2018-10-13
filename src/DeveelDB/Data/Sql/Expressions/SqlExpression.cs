// 
//  Copyright 2010-2018 Deveel
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
using System.Linq;
using System.Threading.Tasks;

using Deveel.Data.Sql.Methods;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Expressions {
	/// <summary>
	/// The abstraction of a SQL expression in an operational context
	/// </summary>
	/// <remarks>
	/// <para>
	/// SQL expressions are atomic parts of a language context, encapsulating
	/// values, operators and functions to interact with the underlying SQL system.
	/// </para>
	/// <para>
	/// The kind of SQL expressions handled by the system is a limited set
	/// (as enumerated by <see cref="SqlExpressionType"/>), but it is possible
	/// by implementors to derive from this class for defining context-specific
	/// expressions, together with other infrastructure elements (like
	/// <see cref="SqlExpressionVisitor"/>).
	/// </para>
	/// </remarks>
	/// <seealso cref="SqlExpressionType"/>
	public abstract class SqlExpression : ISqlFormattable {
		/// <summary>
		/// Constructs an instance of a <see cref="SqlExpression"/> for the given
		/// <see cref="SqlExpressionType"/>.
		/// </summary>
		/// <param name="expressionType">The type of the expression</param>
		protected SqlExpression(SqlExpressionType expressionType) {
			ExpressionType = expressionType;
			Precedence = GetPrecedence();
		}

		private int GetPrecedence() {
			switch (ExpressionType) {
				// Group
				case SqlExpressionType.Group:
					return 151;

				// References
				case SqlExpressionType.Reference:
				case SqlExpressionType.Function:
				case SqlExpressionType.VariableAssign:
				case SqlExpressionType.ReferenceAssign:
				case SqlExpressionType.Variable:
				case SqlExpressionType.Parameter:
					return 150;

				// Unary
				case SqlExpressionType.UnaryPlus:
				case SqlExpressionType.Negate:
				case SqlExpressionType.Not:
					return 140;

				// Cast
				case SqlExpressionType.Cast:
					return 139;

				// Multiplicative
				case SqlExpressionType.Multiply:
				case SqlExpressionType.Divide:
				case SqlExpressionType.Modulo:
					return 130;

				// Additive
				case SqlExpressionType.Add:
				case SqlExpressionType.Subtract:
					return 120;

				// Relational
				case SqlExpressionType.GreaterThan:
				case SqlExpressionType.GreaterThanOrEqual:
				case SqlExpressionType.LessThan:
				case SqlExpressionType.LessThanOrEqual:
				case SqlExpressionType.Is:
				case SqlExpressionType.IsNot:
				case SqlExpressionType.Like:
				case SqlExpressionType.NotLike:
					return 110;

				// Equality
				case SqlExpressionType.Equal:
				case SqlExpressionType.NotEqual:
					return 100;

				// Logical
				case SqlExpressionType.And:
					return 90;
				case SqlExpressionType.Or:
					return 89;
				case SqlExpressionType.XOr:
					return 88;

				// Conditional
				case SqlExpressionType.Condition:
					return 80;

				// Constant
				case SqlExpressionType.Constant:
					return 70;
			}

			return -1;
		}

		/// <summary>
		/// Gets a value indicating if the expression can be reduced to
		/// a simpler form.
		/// </summary>
		public virtual bool CanReduce => IsReference;

		/// <summary>
		/// Gets the type of the expression
		/// </summary>
		public SqlExpressionType ExpressionType { get; }

		/// <summary>
		/// Gets the static <see cref="SqlType"/> result of the expression
		/// </summary>
		/// <remarks>
		/// If the expression requires a context to evaluate the type returned,
		/// an exception will be thrown
		/// </remarks>
		/// <seealso cref="GetSqlType"/>
		public SqlType Type => GetSqlType(null);

		/// <summary>
		/// Gets a value indicating if the expression is a reference
		/// </summary>
		/// <remarks>
		/// <para>
		/// Reference expressions (eg. <see cref="SqlReferenceExpression"/>, <see cref="SqlFunctionExpression"/>)
		/// cannot be statically evaluated, and require a context to produce
		/// a valid result, since they require to access the referenced resource.
		/// </para>
		/// <para>
		/// An expression is considered a reference if in its tree encapsulates
		/// any other reference expression, even if it is not a reference itself
		/// </para>
		/// </remarks>
		/// <seealso cref="SqlReferenceExpression"/>
		/// <seealso cref="SqlReferenceAssignExpression"/>
		/// <seealso cref="SqlFunctionExpression"/>
		/// <seealso cref="SqlVariableExpression"/>
		/// <seealso cref="SqlVariableAssignExpression"/>
		public virtual bool IsReference => true;

		internal int Precedence { get; }

		public override string ToString() {
			return this.ToSqlString();
		}

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			AppendTo(builder);
		}

		protected virtual void AppendTo(SqlStringBuilder builder) {

		}

		/// <summary>
		/// Reduces synchronously this expression in a simpler form 
		/// using the context given
		/// </summary>
		/// <param name="context">The context for the reduction of the expression.</param>
		/// <remarks>
		/// This method is a stub to the asynchronous <see cref="ReduceAsync"/>.
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="SqlExpression"/> that represents a simpler
		/// form of this expression.
		/// </returns>
		/// <seealso cref="ReduceAsync"/>
		public SqlExpression Reduce(QueryContext context) {
			return ReduceAsync(context).Result;
		}

		/// <summary>
		/// Reduces synchronously this expression in a simpler form 
		/// using the context given
		/// </summary>
		/// <param name="context">The context for the reduction of the expression.</param>
		/// <remarks>
		/// <para>
		/// Reducing an expression is a process of simplification that produces a result
		/// from the execution of the expression: for example, reducing a <see cref="SqlBinaryExpression"/>
		/// <c>a + 22</c> will produce the result of the addition of the value referenced by <c>a</c>
		/// with the constant value <c>22</c>.
		/// </para>
		/// <para>
		/// Some expressions are static values (eg. <see cref="SqlConstantExpression"/>) and
		/// reducing them will not produce the expression itself.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="SqlExpression"/> that represents a simpler
		/// form of this expression.
		/// </returns>
		public virtual Task<SqlExpression> ReduceAsync(QueryContext context) {
			return Task.FromResult(this);
		}

		/// <summary>
		/// When overridden by a derived class, determines the type of the value
		/// returned by the reduction of this expression
		/// </summary>
		/// <param name="context">The context used to evaluate the <see cref="SqlType"/> returned
		/// by this expression</param>
		/// <summary>
		/// <para>
		/// The determination of the returned type of an expression is an exponentially
		/// complex process, that increases its complexity in reference expressions: while
		/// <see cref="SqlConstantExpression"/> is of easy determination by the analysis of
		/// the type of the value encapsulated, the resolution of a <see cref="SqlFunctionExpression"/>
		/// requires the access to the referenced function, to extract the defined return type, 
		/// and in some cases more complex analysis (such as deterministic return types).
		/// </para>
		/// </summary>
		/// <returns>
		/// Returns the <see cref="SqlType"/> that this expression will return from its reduction.
		/// </returns>
		public abstract SqlType GetSqlType(QueryContext context);

		/// <summary>
		/// Accepts the visit of a SQL visitor
		/// </summary>
		/// <param name="visitor">The <see cref="SqlExpressionVisitor"/> that is visiting
		/// the expression.</param>
		/// <returns>
		/// Returns an instance of <see cref="SqlExpression"/> resulting from the visit
		/// of the given <see cref="SqlExpressionVisitor"/>.
		/// </returns>
		/// <seealso cref="SqlExpressionVisitor.Visit"/>
		public virtual SqlExpression Accept(SqlExpressionVisitor visitor) {
			return visitor.Visit(this);
		}

		#region Factories

		/// <summary>
		/// Creates a new constant expression that encapsulates the given value.
		/// </summary>
		/// <param name="value">The constant value of the expression</param>
		/// <returns>
		/// Returns an instance of <see cref="SqlConstantExpression"/> that encapsulates
		/// the given value.
		/// </returns>
		/// <exception cref="ArgumentNullException">If the provided <paramref name="value"/>
		/// is <c>null</c>.</exception>
		/// <seealso cref="SqlConstantExpression"/>
		/// <seealso cref="SqlExpressionType.Constant"/>
		public static SqlConstantExpression Constant(SqlObject value) {
			return new SqlConstantExpression(value);
		}

		#region Binary


		/// <summary>
		/// Creates a new binary expression of the given type.
		/// </summary>
		/// <param name="expressionType">The type of the binary expression to be created</param>
		/// <param name="left">The left as side of the binary expression</param>
		/// <param name="right">The right as side of the binary expression</param>
		/// <remarks>
		/// <para>
		/// The allowed binary <see cref="SqlExpressionType">expression types</see> are:
		/// <list type="bullet">
		/// <item><description><see cref="SqlExpressionType.Add"/></description></item>
		/// <item><description><see cref="SqlExpressionType.Subtract"/></description></item>
		/// <item><description><see cref="SqlExpressionType.Multiply"/></description></item>
		/// <item><description><see cref="SqlExpressionType.Divide"/></description></item>
		/// <item><description><see cref="SqlExpressionType.Modulo"/></description></item>
		/// <item><description><see cref="SqlExpressionType.And"/></description></item>
		/// <item><description><see cref="SqlExpressionType.Or"/></description></item>
		/// <item><description><see cref="SqlExpressionType.XOr"/></description></item>
		/// <item><description><see cref="SqlExpressionType.Equal"/></description></item>
		/// <item><description><see cref="SqlExpressionType.NotEqual"/></description></item>
		/// <item><description><see cref="SqlExpressionType.LessThan"/></description></item>
		/// <item><description><see cref="SqlExpressionType.LessThanOrEqual"/></description></item>
		/// <item><description><see cref="SqlExpressionType.GreaterThan"/></description></item>
		/// <item><description><see cref="SqlExpressionType.GreaterThanOrEqual"/></description></item>
		/// <item><description><see cref="SqlExpressionType.Is"/></description></item>
		/// <item><description><see cref="SqlExpressionType.IsNot"/></description></item>
		/// </list>
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="SqlBinaryExpression"/> of the given
		/// <paramref name="expressionType"/> and with the given operands.
		/// </returns>
		/// <exception cref="ArgumentException">If the given <paramref name="expressionType"/> is not a valid
		/// binary type.</exception>
		/// <exception cref="ArgumentNullException">If either <paramref name="left"/> or
		/// <paramref name="right"/> is <c>null</c></exception>
		/// <seealso cref="SqlExpressionType.Add"/>
		/// <seealso cref="SqlExpressionType.Subtract"/>
		/// <seealso cref="SqlExpressionType.Multiply"/>
		/// <seealso cref="SqlExpressionType.Modulo"/>
		/// <seealso cref="SqlExpressionType.Divide"/>
		/// <seealso cref="SqlExpressionType.And"/>
		/// <seealso cref="SqlExpressionType.Or"/>
		/// <seealso cref="SqlExpressionType.XOr"/>
		/// <seealso cref="SqlExpressionType.LessThan"/>
		/// <seealso cref="SqlExpressionType.LessThanOrEqual"/>
		/// <seealso cref="SqlExpressionType.GreaterThan"/>
		/// <seealso cref="SqlExpressionType.GreaterThanOrEqual"/>
		/// <seealso cref="SqlExpressionType.Is"/>
		/// <seealso cref="SqlExpressionType.IsNot"/>
		/// <seealso cref="SqlExpressionType.Equal"/>
		/// <seealso cref="SqlExpressionType.NotEqual"/>
		public static SqlBinaryExpression Binary(SqlExpressionType expressionType, SqlExpression left, SqlExpression right) {
			if (!expressionType.IsBinary())
				throw new ArgumentException($"Expression type {expressionType} is not binary");

			return new SqlBinaryExpression(expressionType, left, right);
		}

		public static SqlBinaryExpression Add(SqlExpression left, SqlExpression right)
			=> Binary(SqlExpressionType.Add, left, right);

		public static SqlBinaryExpression Subtract(SqlExpression left, SqlExpression right)
			=> Binary(SqlExpressionType.Subtract, left, right);

		public static SqlBinaryExpression Divide(SqlExpression left, SqlExpression right)
			=> Binary(SqlExpressionType.Divide, left, right);

		public static SqlBinaryExpression Modulo(SqlExpression left, SqlExpression right)
			=> Binary(SqlExpressionType.Modulo, left, right);

		public static SqlBinaryExpression Multiply(SqlExpression left, SqlExpression right)
			=> Binary(SqlExpressionType.Multiply, left, right);

		public static SqlBinaryExpression Equal(SqlExpression left, SqlExpression right)
			=> Binary(SqlExpressionType.Equal, left, right);

		public static SqlBinaryExpression NotEqual(SqlExpression left, SqlExpression right)
			=> Binary(SqlExpressionType.NotEqual, left, right);

		public static SqlBinaryExpression GreaterThan(SqlExpression left, SqlExpression right)
			=> Binary(SqlExpressionType.GreaterThan, left, right);

		public static SqlBinaryExpression GreaterThanOrEqual(SqlExpression left, SqlExpression right)
			=> Binary(SqlExpressionType.GreaterThanOrEqual, left, right);

		public static SqlBinaryExpression LessThan(SqlExpression left, SqlExpression right)
			=> Binary(SqlExpressionType.LessThan, left, right);

		public static SqlBinaryExpression LessThanOrEqual(SqlExpression left, SqlExpression right)
			=> Binary(SqlExpressionType.LessThanOrEqual, left, right);

		public static SqlBinaryExpression Is(SqlExpression left, SqlExpression rigth)
			=> Binary(SqlExpressionType.Is, left, rigth);

		public static SqlBinaryExpression And(SqlExpression left, SqlExpression right)
			=> Binary(SqlExpressionType.And, left, right);

		public static SqlBinaryExpression Or(SqlExpression left, SqlExpression right)
			=> Binary(SqlExpressionType.Or, left, right);

		public static SqlBinaryExpression XOr(SqlExpression left, SqlExpression right)
			=> Binary(SqlExpressionType.XOr, left, right);

		#endregion

		#region String Match

		
		/// <summary>
		/// Creates a new expression that matches a pattern against a given expression.
		/// </summary>
		/// <param name="expressionType">The type of string match expression</param>
		/// <param name="left">The left as side that references or encapsulates the text to be matched</param>
		/// <param name="pattern">The pattern to be matched</param>
		/// <summary>
		/// The most common name within the SQL syntax for this expression type is <c>LIKE</c>
		/// (and <c>NOT LIKE</c>): the reason of this naming convention derives from the performance
		/// reasons of not having a <c>NOT</c> expression and a <c>LIKE</c> expression.
		/// </summary>
		/// <returns>
		/// Returns an instance of <see cref="SqlStringMatchExpression"/> of the given type
		/// and having the given text to be matched against the given pattern.
		/// </returns>
		/// <exception cref="ArgumentNullException">If either the <paramref name="left"/> or 
		/// <paramref name="pattern"/> is <c>null</c></exception>
		/// <exception cref="ArgumentException">If the given <paramref name="expressionType"/> is not a valid
		/// string match expression type.</exception>
		public static SqlStringMatchExpression StringMatch(SqlExpressionType expressionType, SqlExpression left, SqlExpression pattern) {
			return StringMatch(expressionType, left, pattern, null);
		}

		/// <summary>
		/// Creates a new expression that matches a pattern against a given expression.
		/// </summary>
		/// <param name="expressionType">The type of string match expression</param>
		/// <param name="left">The left as side that references or encapsulates the text to be matched</param>
		/// <param name="pattern">The pattern to be matched</param>
		/// <param name="escape">The optional escape sequence for the text</param>
		/// <summary>
		/// The most common name within the SQL syntax for this expression type is <c>LIKE</c>
		/// (and <c>NOT LIKE</c>): the reason of this naming convention derives from the performance
		/// reasons of not having a <c>NOT</c> expression and a <c>LIKE</c> expression.
		/// </summary>
		/// <returns>
		/// Returns an instance of <see cref="SqlStringMatchExpression"/> of the given type
		/// and having the given text to be matched against the given pattern.
		/// </returns>
		/// <exception cref="ArgumentNullException">If either the <paramref name="left"/> or 
		/// <paramref name="pattern"/> is <c>null</c></exception>
		/// <exception cref="ArgumentException">If the given <paramref name="expressionType"/> is not a valid
		/// string match expression type.</exception>
		/// <seealso cref="SqlStringMatchExpression"/>
		public static SqlStringMatchExpression StringMatch(SqlExpressionType expressionType, SqlExpression left, SqlExpression pattern, SqlExpression escape) {
			return new SqlStringMatchExpression(expressionType, left, pattern, escape);
		}

		public static SqlStringMatchExpression Like(SqlExpression left, SqlExpression pattern) {
			return Like(left, pattern, null);
		}

		public static SqlStringMatchExpression Like(SqlExpression left, SqlExpression pattern, SqlExpression escape)
			=> StringMatch(SqlExpressionType.Like, left, pattern, escape);

		public static SqlStringMatchExpression NotLike(SqlExpression left, SqlExpression pattern) {
			return NotLike(left, pattern, null);
		}

		public static SqlStringMatchExpression NotLike(SqlExpression left, SqlExpression pattern, SqlExpression escape)
			=> StringMatch(SqlExpressionType.NotLike, left, pattern, escape);

		#endregion

		#region Unary

		public static SqlUnaryExpression Unary(SqlExpressionType expressionType, SqlExpression operand) {
			if (!expressionType.IsUnary())
				throw new ArgumentException($"Expression type {expressionType} is not unary");

			return new SqlUnaryExpression(expressionType, operand);
		}

		public static SqlUnaryExpression Not(SqlExpression operand) {
			return Unary(SqlExpressionType.Not, operand);
		}

		public static SqlUnaryExpression Negate(SqlExpression operand) {
			return Unary(SqlExpressionType.Negate, operand);
		}

		public static SqlUnaryExpression Plus(SqlExpression operand) {
			return new SqlUnaryExpression(SqlExpressionType.UnaryPlus, operand);
		}

		#endregion

		public static SqlCastExpression Cast(SqlExpression value, SqlType targetType) {
			return new SqlCastExpression(value, targetType);
		}

		public static SqlReferenceExpression Reference(ObjectName reference) {
			return new SqlReferenceExpression(reference);
		}

		public static SqlVariableExpression Variable(string name) {
			return new SqlVariableExpression(name);
		}

		public static SqlVariableAssignExpression VariableAssign(string name, SqlExpression value) {
			return new SqlVariableAssignExpression(name, value);
		}

		public static SqlReferenceAssignExpression ReferenceAssign(ObjectName referenceName, SqlExpression value) {
			return new SqlReferenceAssignExpression(referenceName, value);
		}

		public static SqlConditionExpression Condition(SqlExpression test, SqlExpression ifTrue) {
			return Condition(test, ifTrue, null);
		}

		public static SqlConditionExpression Condition(SqlExpression test, SqlExpression ifTrue, SqlExpression ifFalse)
			=> new SqlConditionExpression(test, ifTrue, ifFalse);

		public static SqlParameterExpression Parameter() => new SqlParameterExpression();

		public static SqlGroupExpression Group(SqlExpression expression)
			=> new SqlGroupExpression(expression);

		#region Quantify

		//public static SqlQuantifyExpression Quantify(SqlExpressionType expressionType, SqlBinaryExpression expression) {
		//	if (!expressionType.IsQuantify())
		//		throw new ArgumentException($"The expression type {expressionType} is not a quantification expression");

		//	return new SqlQuantifyExpression(expressionType, expression);
		//}

		//public static SqlQuantifyExpression Any(SqlBinaryExpression expression)
		//	=> Quantify(SqlExpressionType.Any, expression);

		//public static SqlQuantifyExpression All(SqlBinaryExpression expression)
		//	=> Quantify(SqlExpressionType.All, expression);

		#endregion

		public static SqlFunctionExpression Function(ObjectName functionName, params InvokeArgument[] args)
			=> new SqlFunctionExpression(functionName, args);

		public static SqlFunctionExpression Function(string functionName, params InvokeArgument[] args)
			=> Function(ObjectName.Parse(functionName), args);

		public static SqlFunctionExpression Function(ObjectName functionName, params SqlExpression[] args)
			=> Function(functionName, args == null ? new InvokeArgument[0] : args.Select(x => new InvokeArgument(x)).ToArray());

		public static SqlFunctionExpression Function(string functionName, params SqlExpression[] args)
			=> Function(ObjectName.Parse(functionName), args);

		public static SqlFunctionExpression Function(ObjectName functionName)
			=> Function(functionName, new InvokeArgument[0]);

		public static SqlFunctionExpression Function(string functionName)
			=> Function(ObjectName.Parse(functionName));

		#endregion

		#region Parse

		//private static bool TryParse(IContext context, string text, out SqlExpression expression, out string[] errors) {
		//	ISqlExpressionParser parser = null;
		//	if (context != null)
		//		parser = context.Scope.GetService<ISqlExpressionParser>();
		//	//if (parser == null)
		//	//	parser = new DefaultSqlExpressionParser();

		//	var result = parser.Parse(context, text);
		//	expression = result.Expression;
		//	errors = result.Errors;
		//	return result.Valid;
		//}

		///// <summary>
		///// Tries to parse the given string to a SQL expression and returns
		///// a value indicating if the parsing was successful.
		///// </summary>
		///// <param name="text">The text to attempt to parse into a <see cref="SqlExpression"/></param>
		///// <param name="expression">The result of a successful parse</param>
		///// <remarks>
		///// This is an overload of the method <see cref="TryParse(IContext,string,out SqlExpression)"/> that
		///// passes a null context, to trigger the usage of the default SQL expression parser
		///// </remarks>
		///// <returns>
		///// Returns <c>true</c> if the parse was successful or <c>false</c> if it was not possible to
		///// parse the text into a valid expression.
		///// </returns>
		//public static bool TryParse(string text, out SqlExpression expression) {
		//	return TryParse(null, text, out expression);
		//}

		///// <summary>
		///// Tries to parse the given string to a SQL expression and returns
		///// a value indicating if the parsing was successful.
		///// </summary>
		///// <param name="context">The context used to resolve the <see cref="ISqlExpressionParser"/> to use for the
		///// parsing of the text</param>
		///// <param name="text">The text to attempt to parse into a <see cref="SqlExpression"/></param>
		///// <param name="expression">The result of a successful parse</param>
		///// <remarks>
		///// If the passed <paramref name="context"/> is null of it does not provide any instance
		///// of <see cref="ISqlExpressionParser"/>, the default internal SQL expression parser will be used.
		///// </remarks>
		///// <returns>
		///// Returns <c>true</c> if the parse was successful or <c>false</c> if it was not possible to
		///// parse the text into a valid expression.
		///// </returns>
		//public static bool TryParse(IContext context, string text, out SqlExpression expression) {
		//	string[] errors;
		//	return TryParse(context, text, out expression, out errors);
		//}

		//public static SqlExpression Parse(string text) {
		//	return Parse(null, text);
		//}

		//public static SqlExpression Parse(IContext context, string text) {
		//	string[] errors;
		//	SqlExpression result;
		//	if (!TryParse(context, text, out result, out errors))
		//		throw new AggregateException(errors.Select(x => new SqlExpressionException(x)));

		//	return result;
		//}

		#endregion

		#region SqlDefaultExpressionParser

		//class DefaultSqlExpressionParser : ISqlExpressionParser {
		//	public SqlExpressionParseResult Parse(IContext context, string expression) {
		//		var parser = new SqlParser();
		//		return parser.ParseExpression(context, expression);
		//	}
		//}

		#endregion
	}
}