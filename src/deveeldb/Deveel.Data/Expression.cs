// 
//  Copyright 2010  Deveel
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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.Text;

using Deveel.Data.Functions;
using Deveel.Data.Query;
using Deveel.Data.Sql;
using Deveel.Data.Types;
using Deveel.Math;

namespace Deveel.Data {
	/// <summary>
	/// An expression that can be evaluated in a statement.
	/// </summary>
	/// <remarks>
	/// This is used as a more complete and flexible version of <i>Condition</i> as 
	/// well as representing column and aggregate functions.
	/// <para>
	/// This class can represent constant expressions (expressions that have no variable 
	/// input), as well as variable expressions. Optimizations may be possible when 
	/// evaluating constant expressions.
	/// </para>
	/// <para>
	/// <b>Note</b>: the expression is stored in postfix orientation. 
	/// eg. "8 + 9 * 3" becomes "8,9,3,*,+"
	/// </para>
	/// <para>
	/// <b>Note</b>: This class is <b>not</b> thread safe. Do not use an 
	/// expression instance between threads.
	/// </para>
	/// </remarks>
	/// <example>
	/// Some examples of constant expressions:
	/// <code>
	///    ( 9 + 3 ) * 90
	///    ( ? * 9 ) / 1
	///    lower("CaPS MUMma")
	///    40 &amp; 0x0FF != 39
	/// </code>
	/// 
	/// Some examples of variable expressions:
	/// <code>
	///    upper(Part.description)
	///    Part.id &gt;= 50
	///    VendorMakesPart.part_id == Part.id
	///    Part.value_of &lt;= Part.cost_of / 4
	/// </code>
	/// </example>
	[Serializable]
	public sealed class Expression : ICloneable, IDeserializationCallback {
		/// <summary>
		/// The list of elements followed by operators in our expression.
		/// </summary>
		/// <remarks>
		/// The expression elements may be of any type represented by the database
		/// (see <see cref="AddElement"/> method for the accepted objects).  The 
		/// expression operators may be <c>+</c>, <c>-</c>, <c>*</c>, <c>/</c>, 
		/// <c>=</c>, <c>&gt;=</c>, <c>&lt;&gt;</c>, etc (as an <see cref="Operator"/>
		/// object (see the <see cref="Operator"/> class for details)).
		/// <para>
		/// This list is stored in postfix order.
		/// </para>
		/// </remarks>
		private ArrayList elements = new ArrayList();

		/// <summary>
		/// The evaluation stack for when the expression is evaluated.
		/// </summary>
		private ArrayList eval_stack;

		/// <summary>
		/// The expression as a plain human readable string.
		/// </summary>
		/// <remarks>
		/// This is in a form that can be readily parsed to an Expression object.
		/// </remarks>
		private StringBuilder text;


		/// <summary>
		/// Instantiate an empty <see cref="Expression"/> object.
		/// </summary>
		public Expression() {
			text = new StringBuilder();
		}

		/// <summary>
		/// Constructs a new <see cref="Expression"/> with a single object element.
		/// </summary>
		/// <param name="ob"></param>
		public Expression(Object ob)
			: this() {
			AddElement(ob);
		}

		/// <summary>
		/// Constructs a copy of the given <see cref="Expression"/>.
		/// </summary>
		/// <param name="exp"></param>
		public Expression(Expression exp) {
			Concat(exp);
			text = new StringBuilder(exp.Text.ToString());
		}

		/// <summary>
		/// Constructs a new <see cref="Expression"/> from the concatination of 
		/// <paramref name="exp1"/> and <paramref name="exp2"/> and the 
		/// <paramref name="op">operator</paramref> for them.
		/// </summary>
		/// <param name="exp1"></param>
		/// <param name="op"></param>
		/// <param name="exp2"></param>
		public Expression(Expression exp1, Operator op, Expression exp2) {
			// Remember, this is in postfix notation.
			elements.AddRange(exp1.elements);
			elements.AddRange(exp2.elements);
			elements.Add(op);
		}

		/// <summary>
		/// Returns the <see cref="StringBuilder"/> that we can use to append plain 
		/// text representation as we are parsing the expression.
		/// </summary>
		public StringBuilder Text {
			get { return text; }
		}

		/// <summary>
		/// Copies the text from the given expression.
		/// </summary>
		/// <param name="e"></param>
		public void CopyTextFrom(Expression e) {
			text = new StringBuilder(e.Text.ToString());
		}

		/// <summary>
		/// Static method that parses the given string which contains an 
		/// expression into an <see cref="Expression"/> object.
		/// </summary>
		/// <param name="expression"></param>
		/// <remarks>
		/// Care should be taken to not use this method inside an inner loop 
		/// because it creates a lot of objects.
		/// </remarks>
		/// <returns>
		/// Returns an <see cref="Expression"/> instance that represents the
		/// string passed as argument.
		/// </returns>
		/// <exception cref="SqlParseException">
		/// If the <paramref name="expression"/> string is invalid.
		/// </exception>
		public static Expression Parse(String expression) {
			lock (expression_parser) {
				try {
					expression_parser.ReInit(new StringReader(expression));
					expression_parser.Reset();
					Expression exp = expression_parser.ParseExpression();

					exp.Text.Length = 0;
					exp.Text.Append(expression);
					return exp;
				} catch (ParseException e) {
					throw new SqlParseException(e, expression);
				}
			}
		}

		/// <summary>
		/// Statically evaluates a string expression into a result.
		/// </summary>
		/// <param name="expression">The expression to evaluate.</param>
		/// <param name="resolver">An instance of <see cref="IVariableResolver"/>
		/// used to resolve every variable in the context.</param>
		/// <returns>
		/// Returns a <see cref="TObject"/> that is the result of the evaluation
		/// of the given expression string.
		/// </returns>
		public static TObject Evaluate(string expression, IVariableResolver resolver) {
			Expression exp = Parse(expression);
			return exp.Evaluate(resolver, null);
		}


		public static TObject Evaluate(string expression, IEnumerable<KeyValuePair<string, object>> args) {
			VariableResolver resolver = new VariableResolver();
			if (args != null) {
				foreach(KeyValuePair<string, object> entry in args) {
					string argName = entry.Key;
					if (string.IsNullOrEmpty(argName))
						throw new ArgumentException("Found an argument with a name not specified.");

					if (!Char.IsLetterOrDigit(argName[0]))
						throw new ArgumentException("The argument name '" + argName + "' is invalid.");

					try {
						resolver.AddVariable(argName, entry.Value);
					} catch(Exception e) {
						throw new ArgumentException("It was not possible to add the variable &" + argName + ": " + e.Message, e);
					}
				}
			}

			Expression exp = Parse(expression);
			exp.Prepare(new VariableExpressionPreparer());
			return exp.Evaluate(resolver, null);
		}

		public static TObject Evaluate(string expression) {
			return Evaluate(expression, (IVariableResolver) null);
		}

		/// <summary>
		/// Expression preparer used to replace a variable-reference
		/// to it's name.
		/// </summary>
		private sealed class VariableExpressionPreparer : IExpressionPreparer {
			public bool CanPrepare(object element) {
				return (element is VariableRef);
			}

			public object Prepare(object element) {
				VariableRef varRef = (VariableRef) element;
				return new VariableName(varRef.Variable);
			}
		}

		/// <summary>
		/// A static expression parser.  To use this we must first synchronize over 
		/// the object.
		/// </summary>
		private readonly static SQL expression_parser = new SQL(new StringReader(""));

		/// <summary>
		/// Generates a simple expression from two objects and an operator.
		/// </summary>
		/// <param name="ob1"></param>
		/// <param name="op"></param>
		/// <param name="ob2"></param>
		/// <returns></returns>
		public static Expression Simple(object ob1, Operator op, object ob2) {
			Expression exp = new Expression(ob1);
			exp.AddElement(ob2);
			exp.AddElement(op);
			return exp;
		}


		/// <summary>
		/// Adds a new element into the expression.
		/// </summary>
		/// <param name="ob">The object to add to the expression.</param>
		/// <remarks>
		/// The element will be added in postfix order.
		/// <para>
		/// The elements that can be added to an expression are
		/// <list type="table">
		///	  <listheader>
		///	    <term>Type</term>
		///	  </listheader>
		///	  <item>
		///	    <term><see cref="DBNull"/></term>
		///	    <description>A <b>null</b> value.</description>
		///	  </item>
		///	  <item>
		///	    <term><see cref="TObject"/></term>
		///	    <term>A Minosse object representation.</term>
		///	  </item>
		///	  <item>
		///	    <term><see cref="ParameterSubstitution"/></term>
		///	    <description>The representation of a <c>?</c> parameter.</description>
		///	  </item>
		///	  <item>
		///	    <term><see cref="CorrelatedVariable"/></term>
		///	    <description></description>
		///	  </item>
		///	  <item>
		///	    <term><see cref="VariableName"/></term>
		///	    <description>A variable that can be a constant value or a 
		///	    reference to a column in a table.</description>
		///	  </item>
		///	  <item>
		///	    <term><see cref="FunctionDef"/></term>
		///	    <description>A descriptor that represents the information
		///	    to call a function.</description>
		///	  </item>
		///	  <item>
		///	    <term><see cref="Operator"/></term>
		///	    <description>An operator between two operands in the expression.</description>
		///	  </item>
		///	  <item>
		///	    <term><see cref="IStatementTreeObject"/></term>
		///	    <description>Represents objects within statements.</description>
		///	  </item>
		/// </list>
		/// </para>
		/// </remarks>
		/// <exception cref="ArgumentException">
		/// If the given <paramref name="ob">object</paramref> is not permitted.
		/// </exception>
		public void AddElement(Object ob) {
			if (ob == null) {
				elements.Add(TObject.Null);
			} else if (ob is TObject ||
			           ob is ParameterSubstitution ||
			           ob is VariableRef ||
			           ob is CorrelatedVariable ||
			           ob is VariableName ||
			           ob is FunctionDef ||
			           ob is Operator ||
			           ob is IStatementTreeObject) {
				elements.Add(ob);
			} else {
				throw new ApplicationException("Unknown element type added to expression: " +
				                               ob.GetType());
			}
		}

		/// <summary>
		/// Merges the given <see cref="Expression">expression</see> with this 
		/// expression.
		/// </summary>
		/// <param name="expr">The expression to merge with the current.</param>
		/// <remarks>
		/// For example, given the expression <c>ab</c>, if the expression 
		/// <c>abc+-</c> was added the expression would become <c>ababc+-</c>.
		/// <para>
		/// This method is useful when copying parts of other expressions when 
		/// forming an expression.
		/// </para>
		/// <para>
		/// This does not change <see cref="Text"/> property.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns always the current expression.
		/// </returns>
		public Expression Concat(Expression expr) {
			elements.AddRange(expr.elements);
			return this;
		}

		/// <summary>
		/// Adds a new <see cref="Operator">operator</see> into the expression.
		/// </summary>
		/// <param name="op">The operator to add to the current expression.</param>
		/// <remarks>
		/// The operator is added in postfix order.
		/// </remarks>
		public void AddOperator(Operator op) {
			elements.Add(op);
		}

		/// <summary>
		/// Returns the number of elements and operators that are in this postfix list.
		/// </summary>
		public int Count {
			get { return elements.Count; }
		}


		/// <summary>
		/// Gets or sets the element at the given position in the postfix list. 
		/// </summary>
		/// <remarks>
		/// Setting should be called after the expression has been setup to alter 
		/// variable alias names, etc.
		/// </remarks>
		public object this[int n] {
			get { return elements[n]; }
			set { elements[n] = value; }
		}


		/// <summary>
		/// Returns the element at the end of the postfix list (the last element).
		/// </summary>
		public object Last {
			get { return elements[Count - 1]; }
		}


		/// <summary>
		/// Pushes an element onto the evaluation stack.
		/// </summary>
		/// <param name="ob"></param>
		private void Push(Object ob) {
			eval_stack.Add(ob);
		}

		/// <summary>
		/// Pops an element from the evaluation stack.
		/// </summary>
		/// <returns></returns>
		private Object Pop() {
			int pos = eval_stack.Count - 1;
			object obj = eval_stack[pos];
			eval_stack.RemoveAt(pos);
			return obj;
		}

		/// <summary>
		/// Returns a complete <see cref="IList">list</see> of <see cref="VariableName"/> 
		/// objects in this expression not including correlated variables.
		/// </summary>
		public IList<VariableName> AllVariables {
			get {
				List<VariableName> vars = new List<VariableName>();
				for (int i = 0; i < elements.Count; ++i) {
					object ob = elements[i];
					if (ob is VariableName) {
						vars.Add((VariableName)ob);
					} else if (ob is FunctionDef) {
						Expression[] parameterss = ((FunctionDef) ob).Parameters;
						for (int n = 0; n < parameterss.Length; ++n) {
							vars.AddRange(parameterss[n].AllVariables);
						}
					} else if (ob is TObject) {
						TObject tob = (TObject) ob;
						if (tob.TType is TArrayType) {
							Expression[] expList = (Expression[]) tob.Object;
							for (int n = 0; n < expList.Length; ++n) {
								vars.AddRange(expList[n].AllVariables);
							}
						}
					}
				}
				return vars;
			}
		}

		/// <summary>
		/// Returns a complete list of all element objects that are in this expression 
		/// and in the parameters of the functions of this expression.
		/// </summary>
		public IList AllElements {
			get {
				ArrayList elems = new ArrayList();
				for (int i = 0; i < elements.Count; ++i) {
					Object ob = elements[i];
					if (ob is Operator) {
						// don't add operators...
					} else if (ob is FunctionDef) {
						Expression[] parameterss = ((FunctionDef) ob).Parameters;
						for (int n = 0; n < parameterss.Length; ++n) {
							elems.AddRange(parameterss[n].AllElements);
						}
					} else if (ob is TObject) {
						TObject tob = (TObject) ob;
						if (tob.TType is TArrayType) {
							Expression[] exp_list = (Expression[]) tob.Object;
							for (int n = 0; n < exp_list.Length; ++n) {
								elems.AddRange(exp_list[n].AllElements);
							}
						} else {
							elems.Add(ob);
						}
					} else {
						elems.Add(ob);
					}
				}
				return elems;
			}
		}

		/// <summary>
		/// A general prepare that cascades through the expression and its 
		/// parents and substitutes an elements that the preparer wants to 
		/// substitute.
		/// </summary>
		/// <remarks>
		/// <b>Note:</b> This will not cascade through to the parameters of 
		/// <see cref="IFunction"/> objects however it will cascade through 
		/// <see cref="FunctionDef"/> parameters. For this reason you <b>must</b>
		/// call <i>PrepareFunctions</i> after this method.
		/// </remarks>
		public void Prepare(IExpressionPreparer preparer) {
			for (int n = 0; n < elements.Count; ++n) {
				Object ob = elements[n];

				// If the preparer will prepare this type of object then set the
				// entry with the prepared object.
				if (preparer.CanPrepare(ob)) {
					elements[n] = preparer.Prepare(ob);
				}

				Expression[] exp_list = null;
				if (ob is FunctionDef) {
					FunctionDef func = (FunctionDef)ob;
					exp_list = func.Parameters;
				} else if (ob is TObject) {
					TObject tob = (TObject)ob;
					if (tob.TType is TArrayType) {
						exp_list = (Expression[])tob.Object;
					}
				} else if (ob is IStatementTreeObject) {
					IStatementTreeObject stree = (IStatementTreeObject)ob;
					stree.PrepareExpressions(preparer);
				}

				if (exp_list != null) {
					for (int p = 0; p < exp_list.Length; ++p) {
						exp_list[p].Prepare(preparer);
					}
				}

			}
		}


		/// <summary>
		/// Gets <b>true</b> if the expression doesn't include any variables or 
		/// non constant functions (is constant), otherwise <b>false</b>.
		/// </summary>
		/// <remarks>
		/// Note that a <see cref="CorrelatedVariable">correlated variable</see> 
		/// is considered a constant.
		/// </remarks>
		public bool IsConstant {
			get {
				for (int n = 0; n < elements.Count; ++n) {
					Object ob = elements[n];
					if (ob is TObject) {
						TObject tob = (TObject) ob;
						TType ttype = tob.TType;
						// If this is a query plan, return false
						if (ttype is TQueryPlanType)
							return false;
						// If this is an array, check the array for constants
						if (ttype is TArrayType) {
							Expression[] exp_list = (Expression[]) tob.Object;
							for (int p = 0; p < exp_list.Length; ++p) {
								if (!exp_list[p].IsConstant) {
									return false;
								}
							}
						}
					} else if (ob is VariableName) {
						return false;
					} else if (ob is FunctionDef) {
						Expression[] parameterss = ((FunctionDef) ob).Parameters;
						for (int p = 0; p < parameterss.Length; ++p) {
							if (!parameterss[p].IsConstant) {
								return false;
							}
						}
					}
				}
				return true;
			}
		}

		/// <summary>
		/// Gets <b>true</b> if the expression has a subquery (eg. <c>in ( select ... )</c>)
		/// defined within it (this cascades through function parameters also).
		/// </summary>
		public bool HasSubQuery {
			get {
				IList list = AllElements;
				int len = list.Count;
				for (int n = 0; n < len; ++n) {
					Object ob = list[n];
					if (ob is TObject) {
						TObject tob = (TObject) ob;
						if (tob.TType is TQueryPlanType) {
							return true;
						}
					}
				}
				return false;
			}
		}

		/// <summary>
		/// Gets <b>true</b> if the expression contains a NOT operator 
		/// within it.
		/// </summary>
		/// <returns>
		/// Returns <b>true</b> if the expression contains the NOT operator, 
		/// otherwise <b>false</b>.
		/// </returns>
		internal bool ContainsNotOperator() {
			for (int n = 0; n < elements.Count; ++n) {
				Object ob = elements[n];
				if (ob is Operator && ((Operator)ob).IsNegation)
					return true;
			}
			return false;
		}

		///<summary>
		/// Discovers all the correlated variables in this expression.
		///</summary>
		///<param name="level"></param>
		///<param name="list"></param>
		/// <remarks>
		/// If this expression contains a sub-query plan, we ask the plan to find 
		/// the list of correlated variables.  The discovery process increments the 
		/// <paramref name="level"/> variable for each sub-plan.
		/// </remarks>
		///<returns></returns>
		internal IList<CorrelatedVariable> DiscoverCorrelatedVariables(ref int level, IList<CorrelatedVariable> list) {
			IList elems = AllElements;
			int sz = elems.Count;
			// For each element
			for (int i = 0; i < sz; ++i) {
				Object ob = elems[i];
				if (ob is CorrelatedVariable) {
					CorrelatedVariable v = (CorrelatedVariable)ob;
					if (v.QueryLevelOffset == level) {
						list.Add(v);
					}
				} else if (ob is TObject) {
					TObject tob = (TObject)ob;
					if (tob.TType is TQueryPlanType) {
						IQueryPlanNode node = (IQueryPlanNode)tob.Object;
						list = node.DiscoverCorrelatedVariables(level + 1, list);
					}
				}
			}
			return list;
		}

		///<summary>
		/// Discovers all the tables in the sub-queries of this expression.
		///</summary>
		///<param name="list"></param>
		/// <remarks>
		/// This is used for determining all the tables that a query plan touches.
		/// </remarks>
		///<returns></returns>
		internal IList<TableName> DiscoverTableNames(IList<TableName> list) {
			IList elems = AllElements;
			int sz = elems.Count;
			// For each element
			for (int i = 0; i < sz; ++i) {
				Object ob = elems[i];
				if (ob is TObject) {
					TObject tob = (TObject)ob;
					if (tob.TType is TQueryPlanType) {
						IQueryPlanNode node = (IQueryPlanNode)tob.Object;
						list = node.DiscoverTableNames(list);
					}
				}
			}
			return list;
		}

		///<summary>
		/// Returns the <see cref="IQueryPlanNode"/> object in this expression, if it 
		/// evaluates to a single <see cref="IQueryPlanNode"/>, otherwise returns <b>null</b>.
		///</summary>
		internal IQueryPlanNode AsQueryPlanNode() {
			Object ob = this[0];
			if (Count == 1 && ob is TObject) {
				TObject tob = (TObject) ob;
				if (tob.TType is TQueryPlanType)
					return (IQueryPlanNode) tob.Object;
			}
			return null;
		}


		///<summary>
		/// Returns the <see cref="VariableName"/> if this expression evaluates to a single variable, 
		/// otherwise returns null.
		///</summary>
		/// <remarks>
		/// A correlated variable will not be returned.
		/// </remarks>
		public VariableName AsVariableName() {
			if (Count != 1)
				return null;

			object ob = this[0];
			if (ob is VariableName)
				return  (VariableName) ob;

			return null;
		}

		/// <summary>
		/// Gets an array of two <see cref="Expression"/> objects that 
		/// represent the left hand and right and side of the last operator in 
		/// the post fix notation.
		/// </summary>
		/// <returns>
		/// Returns a <see cref="Expression"/> array of two elements representing the
		/// current expression splitted by the last operator in the post fix notation.
		/// </returns>
		/// <example>
		/// For example: <c>(a + b) - (c + d)</c> will return <c>{ (a + b), (c + d) }</c>.
		/// <para>
		/// More useful example is:
		/// <c>id + 3 > part_id - 2</c> will return <c>( id + 3, part_id - 2 }</c>
		/// </para>
		/// </example>
		internal Expression[] Split() {
			if (Count <= 1)
				throw new ApplicationException("Can only split expressions with more than 1 element.");

			int midpoint = -1;
			int stack_size = 0;
			for (int n = 0; n < Count - 1; ++n) {
				object ob = this[n];
				if (ob is Operator) {
					--stack_size;
				} else {
					++stack_size;
				}

				if (stack_size == 1) {
					midpoint = n;
				}
			}

			if (midpoint == -1)
				throw new ApplicationException("postfix format error: Midpoint not found.");

			Expression lhs = new Expression();
			for (int n = 0; n <= midpoint; ++n)
				lhs.AddElement(this[n]);

			Expression rhs = new Expression();
			for (int n = midpoint + 1; n < Count - 1; ++n)
				rhs.AddElement(this[n]);

			return new Expression[] { lhs, rhs };
		}

		/// <summary>
		/// Gets the end <see cref="Expression">expresison</see> of the current one.
		/// </summary>
		/// <remarks>
		/// For example, an expression of <c>ab</c> has an end expression of 
		/// <c>b</c>. The expression <c>abc+=</c> has an end expression of <c>abc+=</c>.
		/// <para>
		/// This is useful to call in the middle of an <see cref="Expression"/> 
		/// object being formed. It allows for the last complete expression to 
		/// be returned.
		/// </para>
		/// <para>
		/// If this is called when an expression is completely formed it will 
		/// always return the complete expression.
		/// </para>
		/// </remarks>
		internal Expression EndExpression {
			get {
				int stack_size = 1;
				int end = Count - 1;
				for (int n = end; n > 0; --n) {
					Object ob = this[n];
					if (ob is Operator) {
						++stack_size;
					} else {
						--stack_size;
					}

					if (stack_size == 0) {
						// Now, n .. end represents the new expression.
						Expression new_exp = new Expression();
						for (int i = n; i <= end; ++i) {
							new_exp.AddElement(this[i]);
						}
						return new_exp;
					}
				}

				return new Expression(this);
			}
		}

		/// <summary>
		/// Breaks this expression into a list of sub-expressions that are 
		/// splitted by the given operator.
		/// </summary>
		/// <param name="list"></param>
		/// <param name="logicalOp"></param>
		/// <remarks>
		/// For example, given the expression <c>(a = b AND b = c AND (a = 2 OR c = 1))</c>,
		/// calling this method with <paramref name="logicalOp"/> = AND 
		/// will return a list of the three expressions.
		/// <para>
		/// This is a common function used to split up an expressions into 
		/// logical components for processing.
		/// </para>
		/// </remarks>
		internal IList<Expression> BreakByOperator(IList<Expression> list, string logicalOp) {
			// The last operator must be 'and'
			object ob = Last;
			if (ob is Operator) {
				Operator op = (Operator)ob;
				if (op.IsEquivalent(logicalOp)) {
					// Last operator is 'and' so split and recurse.
					Expression[] exps = Split();
					list = exps[0].BreakByOperator(list, logicalOp);
					list = exps[1].BreakByOperator(list, logicalOp);
					return list;
				}
			}
			// If no last expression that matches then add this expression to the
			// list.
			list.Add(this);
			return list;
		}

		/// <summary>
		/// Evaluates this expression and returns the result of the evaluation.
		/// </summary>
		/// <param name="resolver">The <see cref="IVariableResolver"/> used to 
		/// resolve a variable name to a value.</param>
		/// <param name="group">The <see cref="IGroupResolver"/> used if there 
		/// are any aggregate function in the evaluation. This parameter can be
		/// <b>null</b> if evaluating an expression without grouping aggregates.</param>
		/// <param name="context">A <see cref="IQueryContext"/> providing
		/// information about the environment of the query. This parameter can
		/// be <b>null</b> if the expression contains only constant values.</param>
		/// <remarks>
		/// This method is going to be called <b>a lot</b>, so it's need it to be optimal.
		/// <para>
		/// <b>Note:</b> This method is <b>not</b> thread safe cause of the 
		/// evaluation stack.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns a <see cref="TObject"/> that is the result of the evaluation
		/// of the expression in the given context.
		/// </returns>
		/// <threadsafety instance="false"/>
		public TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
			// Optimization - trivial case of 'a' or 'ab*' postfix are tested for
			//   here.
			int element_count = elements.Count;
			if (element_count == 1)
				return (TObject)ElementToObject(0, group, resolver, context);
			if (element_count == 3) {
				TObject o1 = (TObject)ElementToObject(0, group, resolver, context);
				TObject o2 = (TObject)ElementToObject(1, group, resolver, context);
				Operator op = (Operator)elements[2];
				return op.Evaluate(o1, o2, group, resolver, context);
			}

			if (eval_stack == null)
				eval_stack = new ArrayList();

			for (int n = 0; n < element_count; ++n) {
				object val = ElementToObject(n, group, resolver, context);
				if (val is Operator) {
					// Pop the last two values off the stack, evaluate them and push
					// the new value back on.
					Operator op = (Operator)val;

					TObject v2 = (TObject)Pop();
					TObject v1 = (TObject)Pop();

					Push(op.Evaluate(v1, v2, group, resolver, context));
				} else {
					Push(val);
				}
			}
			// We should end with a single value on the stack.
			return (TObject)Pop();
		}

		///<summary>
		///</summary>
		///<param name="resolver"></param>
		///<param name="context"></param>
		///<returns></returns>
		public TObject Evaluate(IVariableResolver resolver, IQueryContext context) {
			return Evaluate(null, resolver, context);
		}

		/// <summary>
		/// Gets the element at the given position in the expression list.
		/// </summary>
		/// <remarks>
		/// If the element is a variable then it is resolved on the 
		/// <paramref name="resolver"/>. If the element is a function then it 
		/// is evaluated and the result is returned.</remarks>
		private Object ElementToObject(int n, IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
			object ob = elements[n];
			if (ob is TObject ||
			    ob is Operator) {
				return ob;
			}
			if (ob is VariableName)
				return resolver.Resolve((VariableName) ob);
			if (ob is CorrelatedVariable)
				return ((CorrelatedVariable) ob).EvalResult;
			if (ob is FunctionDef) {
				IFunction fun = ((FunctionDef) ob).GetFunction(context);
				return fun.Evaluate(group, resolver, context);
			} 
			if (ob is VariableRef) {
				VariableRef variableRef = (VariableRef) ob;
				Variable variable = context.GetVariable(variableRef.Variable);
				return (variable == null ? TObject.Null : variable.Value);
			}

			if (ob is TableSelectExpression) {
				TableSelectExpression selectExpression = (TableSelectExpression) ob;

				// Generate the TableExpressionFromSet hierarchy for the expression,
				TableExpressionFromSet from_set = Planner.GenerateFromSet(selectExpression, context.Connection);

				// Form the plan
				IQueryPlanNode plan = Planner.FormQueryPlan(context.Connection, selectExpression, from_set, new List<ByColumn>());

				return TObject.CreateQueryPlan(plan);
			}

			if (ob == null)
				throw new NullReferenceException("Null element in expression");

			throw new ApplicationException("Unknown element type: " + ob.GetType());
		}

		/// <summary>
		/// Cascades through the expression to check if any aggregate functions
		/// are defined.
		/// </summary>
		/// <returns>
		/// Returns <b>true</b> if any aggregate functions is found within the
		/// current expression, otherwise <b>false</b>.
		/// </returns>
		public bool HasAggregateFunction(IQueryContext context) {
			for (int n = 0; n < elements.Count; ++n) {
				Object ob = elements[n];
				if (ob is FunctionDef) {
					if (((FunctionDef)ob).IsAggregate(context)) {
						return true;
					}
				} else if (ob is TObject) {
					TObject tob = (TObject)ob;
					if (tob.TType is TArrayType) {
						Expression[] list = (Expression[])tob.Object;
						for (int i = 0; i < list.Length; ++i) {
							if (list[i].HasAggregateFunction(context)) {
								return true;
							}
						}
					}
				}
			}
			return false;
		}

		/// <summary>
		/// Gets the type of object this expression evaluates to.
		/// </summary>
		/// <remarks>
		/// We determine the returned value of the expression by looking at the 
		/// last element of the expression. If the last element is a <see cref="TType"/> 
		/// object, it returns the type. If the last element is a <see cref="IFunction"/>, 
		/// <see cref="Operator"/> or <see cref="VariableName"/> then it returns 
		/// the type that these objects have set as their result type.
		/// </remarks>
		internal TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
			object ob = elements[elements.Count - 1];
			if (ob is FunctionDef) {
				IFunction fun = ((FunctionDef)ob).GetFunction(context);
				return fun.ReturnTType(resolver, context);
			}
			if (ob is TObject)
				return ((TObject)ob).TType;
			if (ob is Operator) {
				Operator op = (Operator)ob;
				return op.ReturnTType;
			} 
			if (ob is VariableName) {
				VariableName variable = (VariableName)ob;
				return resolver.ReturnTType(variable);
			} 
			if (ob is CorrelatedVariable) {
				CorrelatedVariable variable = (CorrelatedVariable)ob;
				return variable.ReturnTType;
			}
			if (ob is VariableRef) {
				VariableRef variableRef = (VariableRef) ob;
				Variable variable = context.GetVariable(variableRef.Variable);
				if (variable != null)
					return variable.Type;
			}
			
			throw new ApplicationException("Unable to determine type for expression.");
		}

		/// <summary>
		/// Performs a deep clone of this object, calling <see cref="ICloneable.Clone"/>
		/// on any elements that are mutable or shallow copying immutable members.
		/// </summary>
		/// <returns>
		/// Returns an <see cref="Expression"/> that is a clone of the current.
		/// </returns>
		public object Clone() {
			// Shallow clone
			Expression v = (Expression) MemberwiseClone();
			v.eval_stack = null;
			//    v.text = new StringBuffer(new String(text));
			int size = elements.Count;
			ArrayList cloned_elements = new ArrayList(size);
			v.elements = cloned_elements;

			// Clone items in the elements list
			for (int i = 0; i < size; ++i) {
				object element = elements[i];

				if (element is TObject) {
					// TObject is immutable except for TArrayType and TQueryPlanType
					TObject tob = (TObject) element;
					TType ttype = tob.TType;
					// For a query plan
					if (ttype is TQueryPlanType) {
						IQueryPlanNode node = (IQueryPlanNode) tob.Object;
						node = (IQueryPlanNode) node.Clone();
						element = new TObject(ttype, node);
					}
						// For an array
					else if (ttype is TArrayType) {
						Expression[] arr = (Expression[]) tob.Object;
						arr = (Expression[]) arr.Clone();
						for (int n = 0; n < arr.Length; ++n) {
							arr[n] = (Expression) arr[n].Clone();
						}
						element = new TObject(ttype, arr);
					}
				} else if (element is Operator ||
				           element is ParameterSubstitution ||
				           element is VariableRef) {
					// immutable so we do not need to clone these
				} else if (element is CorrelatedVariable) {
					element = ((CorrelatedVariable) element).Clone();
				} else if (element is VariableName) {
					element = ((VariableName) element).Clone();
				} else if (element is FunctionDef) {
					element = ((FunctionDef) element).Clone();
				} else if (element is IStatementTreeObject) {
					element = ((IStatementTreeObject) element).Clone();
				} else {
					throw new ApplicationException(element.GetType().ToString());
				}
				cloned_elements.Add(element);
			}

			return v;
		}

		/// <inheritdoc/>
		public override String ToString() {
			StringBuilder buf = new StringBuilder();
			buf.Append("[ Expression ");
			if (Text != null) {
				buf.Append("[");
				buf.Append(Text.ToString());
				buf.Append("]");
			}
			buf.Append(": ");
			for (int n = 0; n < elements.Count; ++n) {
				buf.Append(elements[n]);
				if (n < elements.Count - 1) {
					buf.Append(",");
				}
			}
			buf.Append(" ]");
			return buf.ToString();
		}

		void IDeserializationCallback.OnDeserialization(object sender) {
			int sz = elements.Count;
			for (int i = 0; i < sz; ++i) {
				Object ob = elements[i];
				TObject conv_object = null;
				if (ob == null) {
					conv_object = TObject.Null;
				} else if (ob is String) {
					conv_object = TObject.CreateString((String)ob);
				} else if (ob is BigDecimal) {
					conv_object = TObject.CreateBigNumber((BigDecimal)ob);
				} else if (ob is DateTime) {
					conv_object = TObject.CreateDateTime((DateTime)ob);
				} else if (ob is Boolean) {
					conv_object = TObject.CreateBoolean((bool)ob);
				}
				if (conv_object != null) {
					elements[i] = conv_object;
				}
			}
		}
	}
}