//  
//  Operator.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;
using System.Text;

namespace Deveel.Data {
	/// <summary>
	/// An operator for an expression.
	/// </summary>
	[Serializable]
	public abstract class Operator {
		// ---------- Statics ----------

		private static readonly AddOperator add_op = new AddOperator();
		private static readonly Hashtable all_map = new Hashtable();
		private static readonly AndOperator and_op = new AndOperator();
		private static readonly Hashtable any_map = new Hashtable();
		private static readonly ConcatOperator concat_op = new ConcatOperator();
		private static readonly DivideOperator div_op = new DivideOperator();
		private static readonly EqualOperator eq_op = new EqualOperator();
		private static readonly GreaterOperator g_op = new GreaterOperator();

		private static readonly GreaterEqualOperator geq_op =
			new GreaterEqualOperator();

		private static readonly Operator in_op;
		private static readonly IsOperator is_op = new IsOperator();
		private static readonly IsNotOperator isn_op = new IsNotOperator();
		private static readonly LesserOperator l_op = new LesserOperator();
		private static readonly LesserEqualOperator leq_op = new LesserEqualOperator();

		private static readonly PatternMatchTrueOperator like_op =
			new PatternMatchTrueOperator();

		private static readonly MultiplyOperator mul_op = new MultiplyOperator();
		private static readonly NotEqualOperator neq_op = new NotEqualOperator();
		private static readonly Operator nin_op;

		private static readonly PatternMatchFalseOperator nlike_op =
			new PatternMatchFalseOperator();

		private static readonly Operator not_op = new SimpleOperator("not", 3);
		private static readonly OrOperator or_op = new OrOperator();

		private static readonly ParenOperator par1_op = new ParenOperator("(");
		private static readonly ParenOperator par2_op = new ParenOperator(")");
		private static readonly RegexOperator regex_op = new RegexOperator();
		private static readonly SubtractOperator sub_op = new SubtractOperator();

		// ---------- Member ----------

		/// <summary>
		/// A string that represents this operator.
		/// </summary>
		private readonly String op;

		/// <summary>
		/// The precedence of this operator.
		/// </summary>
		private readonly int precedence;

		/// <summary>
		/// If this is a set operator such as ANY or ALL then this is set with the flag type.
		/// </summary>
		private readonly OperatorSet set_type;

		static Operator() {
			// Populate the static ANY and ALL mapping
			any_map.Add("=", new AnyOperator("="));
			any_map.Add("<>", new AnyOperator("<>"));
			any_map.Add(">", new AnyOperator(">"));
			any_map.Add(">=", new AnyOperator(">="));
			any_map.Add("<", new AnyOperator("<"));
			any_map.Add("<=", new AnyOperator("<="));

			all_map.Add("=", new AllOperator("="));
			all_map.Add("<>", new AllOperator("<>"));
			all_map.Add(">", new AllOperator(">"));
			all_map.Add(">=", new AllOperator(">="));
			all_map.Add("<", new AllOperator("<"));
			all_map.Add("<=", new AllOperator("<="));

			// The IN and NOT IN operator are '= ANY' and '<> ALL' respectively.
			in_op = (Operator) any_map["="];
			nin_op = (Operator) all_map["<>"];
		}

		protected Operator(String op)
			: this(op, 0, OperatorSet.NONE) {
		}

		protected Operator(String op, int precedence)
			: this(op, precedence, OperatorSet.NONE) {
		}

		protected Operator(String op, int precedence, OperatorSet set_type) {
			if (set_type != OperatorSet.NONE && set_type != OperatorSet.ANY && set_type != OperatorSet.ALL) {
				throw new ArgumentException("Invalid set_type.");
			}
			this.op = op;
			this.precedence = precedence;
			this.set_type = set_type;
		}


		/// <summary>
		/// Gets the operator precedence used to evaluate it within
		/// an <see cref="Expression"/>.
		/// </summary>
		public int Precedence {
			get { return precedence; }
		}

		/// <summary>
		/// Gets <b>true</b> if the operator is a condition operator,
		/// otherwise <b>false</b>.
		/// </summary>
		public bool IsCondition {
			get {
				return (Equals(eq_op) ||
				        Equals(neq_op) ||
				        Equals(g_op) ||
				        Equals(l_op) ||
				        Equals(geq_op) ||
				        Equals(leq_op) ||
				        Equals(is_op) ||
				        Equals(isn_op));
			}
		}

		/// <summary>
		/// Gets <b>true</b> if the operator is a mathematical operator,
		/// otherwise <b>false</b>.
		/// </summary>
		public bool IsMathematical {
			get {
				return (Equals(add_op) ||
				        Equals(sub_op) ||
				        Equals(mul_op) ||
				        Equals(div_op) ||
				        Equals(concat_op));
			}
		}

		/// <summary>
		/// Gets <b>true</b> if the operator is a pattern operator,
		/// otherwise <b>false</b>.
		/// </summary>
		public bool IsPattern {
			get {
				return (Equals(like_op) ||
				        Equals(nlike_op) ||
				        Equals(regex_op));
			}
		}

		/// <summary>
		/// Gets <b>true</b> if the operator is a logical operator,
		/// otherwise <b>false</b>.
		/// </summary>
		public bool IsLogical {
			get {
				return (Equals(and_op) ||
				        Equals(or_op));
			}
		}

		/// <summary>
		/// Gets the <i>is not</i> conditional operator (<pre>IS NOT</pre>).
		/// </summary>
		public bool IsNot {
			get { return Equals(not_op); }
		}

		/// <summary>
		/// Gets <b>true</b> if the operator is sub-query operator,
		/// otherwise <b>false</b>
		/// </summary>
		public bool IsSubQuery {
			get {
				return (set_type != OperatorSet.NONE ||
				        Equals(in_op) ||
				        Equals(nin_op));
			}
		}

		/// <summary>
		/// Returns true if this operator is not inversible.
		/// </summary>
		public bool IsNotInversible {
			get {
				// The REGEX op, and mathematical operators are not inversible.
				return Equals(regex_op) || IsMathematical;
			}
		}

		/// <summary>
		/// Returns the sub query representation of this operator.
		/// </summary>
		private OperatorSet SubQueryFormRepresentation {
			get { return set_type; }
		}

		/// <summary>
		/// The type of object this Operator evaluates to.
		/// </summary>
		public TType ReturnTType {
			get {
				if (Equals(concat_op))
					return TType.StringType;
				if (IsMathematical)
					return TType.NumericType;
				return TType.BooleanType;
			}
		}

		/// <summary>
		/// Returns the string value of this operator.
		/// </summary>
		internal string StringRepresentation {
			get { return op; }
		}

		///<summary>
		///</summary>
		///<param name="given_op"></param>
		///<returns></returns>
		public bool Is(String given_op) {
			return given_op.Equals(op);
		}

		/// <summary>
		/// Evaluates two <see cref="TObject"/>.
		/// </summary>
		/// <param name="ob1"></param>
		/// <param name="ob2"></param>
		/// <param name="group"></param>
		/// <param name="resolver"></param>
		/// <param name="context"></param>
		/// <returns>
		/// Returns a <see cref="TObject"/> as result of the evaluation.
		/// </returns>
		public abstract TObject Evaluate(TObject ob1, TObject ob2, IGroupResolver group, IVariableResolver resolver,
		                                 IQueryContext context);

		/// <summary>
		/// Returns an Operator that is the reverse of this Operator.
		/// </summary>
		/// <remarks>
		/// This is used for reversing a conditional expression. eg. <c>9 &gt; id</c> 
		/// becomes <c>id &lt; 9</c>.
		/// </remarks>
		public Operator Reverse() {
			if (Equals(eq_op) || Equals(neq_op) || Equals(is_op) || Equals(isn_op))
				return this;
			if (Equals(g_op))
				return l_op;
			if (Equals(l_op))
				return g_op;
			if (Equals(geq_op))
				return leq_op;
			if (Equals(leq_op))
				return geq_op;

			throw new ApplicationException("Can't reverse a non conditional operator.");
		}

		/// <summary>
		/// Returns the inverse operator of this operator.
		/// </summary>
		/// <remarks>
		/// For example, = becomes <c>&lt;&gt;</c>, <c>&gt;</c> becomes 
		/// <c>&lt;=</c>, <c>AND</c> becomes <c>OR</c>.
		/// </remarks>
		public Operator Inverse() {
			if (IsSubQuery) {
				OperatorSet inv_type;
				if (IsSubQueryForm(OperatorSet.ANY)) {
					inv_type = OperatorSet.ALL;
				} else if (IsSubQueryForm(OperatorSet.ALL)) {
					inv_type = OperatorSet.ANY;
				} else {
					throw new Exception("Can not handle sub-query form.");
				}

				Operator inv_op = Get(op).Inverse();

				return inv_op.GetSubQueryForm(inv_type);
			}
			if (Equals(eq_op))
				return neq_op;
			if (Equals(neq_op))
				return eq_op;
			if (Equals(g_op))
				return leq_op;
			if (Equals(l_op))
				return geq_op;
			if (Equals(geq_op))
				return l_op;
			if (Equals(leq_op))
				return g_op;
			if (Equals(and_op))
				return or_op;
			if (Equals(or_op))
				return and_op;
			if (Equals(like_op))
				return nlike_op;
			if (Equals(nlike_op))
				return like_op;
			if (Equals(is_op))
				return isn_op;
			if (Equals(isn_op))
				return is_op;

			throw new ApplicationException("Can't inverse operator '" + op + "'");
		}


		/// <summary>
		/// Given a parameter of either NONE, ANY, ALL or SINGLE, this returns true
		/// if this operator is of the given type.
		/// </summary>
		/// <param name="type"></param>
		/// <returns></returns>
		public bool IsSubQueryForm(OperatorSet type) {
			return type == set_type;
		}

		/// <summary>
		/// Returns the ANY or ALL form of this operator.
		/// </summary>
		/// <param name="type"></param>
		/// <returns></returns>
		public Operator GetSubQueryForm(OperatorSet type) {
			Operator result_op = null;
			if (type == OperatorSet.ANY) {
				result_op = (Operator) any_map[op];
			} else if (type == OperatorSet.ALL) {
				result_op = (Operator) all_map[op];
			} else if (type == OperatorSet.NONE) {
				result_op = Get(op);
			}

			if (result_op == null) {
				throw new ApplicationException("Couldn't change the form of operator '" + op + "'.");
			}
			return result_op;
		}

		/// <summary>
		/// Returns the ANY or ALL form of this operator.
		/// </summary>
		/// <param name="type_str"></param>
		/// <returns></returns>
		public Operator GetSubQueryForm(String type_str) {
			String s = type_str.ToUpper();
			if (s.Equals("SINGLE") || s.Equals("ANY") || s.Equals("SOME")) {
				return GetSubQueryForm(OperatorSet.ANY);
			} else if (s.Equals("ALL")) {
				return GetSubQueryForm(OperatorSet.ALL);
			}
			throw new ApplicationException("Do not understand subquery type '" + type_str + "'");
		}

		/// <inheritdoc/>
		public override string ToString() {
			StringBuilder buf = new StringBuilder();
			buf.Append(op);
			if (set_type == OperatorSet.ANY) {
				buf.Append(" ANY");
			} else if (set_type == OperatorSet.ALL) {
				buf.Append(" ALL");
			}
			return buf.ToString();
		}

		/// <inheritdoc/>
		public override bool Equals(Object ob) {
			Operator oob = (Operator)ob;
			return op.Equals(oob.op) && set_type == oob.set_type;
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return base.GetHashCode();
		}


		/// <summary>
		/// Returns an Operator with the given string.
		/// </summary>
		/// <param name="op"></param>
		/// <returns></returns>
		public static Operator Get(String op) {
			if (op.Equals("+"))
				return add_op;
			if (op.Equals("-"))
				return sub_op;
			if (op.Equals("*"))
				return mul_op;
			if (op.Equals("/"))
				return div_op;
			if (op.Equals("||"))
				return concat_op;
			if (op.Equals("=") | op.Equals("=="))
				return eq_op;
			if (op.Equals("<>") | op.Equals("!="))
				return neq_op;
			if (op.Equals(">"))
				return g_op;
			if (op.Equals("<"))
				return l_op;
			if (op.Equals(">="))
				return geq_op;
			if (op.Equals("<="))
				return leq_op;
			if (op.Equals("("))
				return par1_op;
			if (op.Equals(")"))
				return par2_op;

			// Operators that are words, convert to lower case...
			op = op.ToLower();
			if (op.Equals("is"))
				return is_op;
			if (op.Equals("is not"))
				return isn_op;
			if (op.Equals("like"))
				return like_op;
			if (op.Equals("not like"))
				return nlike_op;
			if (op.Equals("regex"))
				return regex_op;
			if (op.Equals("in"))
				return in_op;
			if (op.Equals("not in"))
				return nin_op;
			if (op.Equals("not"))
				return not_op;
			if (op.Equals("and"))
				return and_op;
			if (op.Equals("or"))
				return or_op;


			throw new ApplicationException("Unrecognised operator type: " + op);
		}

		// ---------- Convenience methods ----------

		/// <summary>
		/// Returns true if the given TObject is a bool and is true.  If the
		/// TObject is not a bool value or is null or is false, then it returns
		/// false.
		/// </summary>
		/// <param name="b"></param>
		/// <returns></returns>
		private static bool IsTrue(TObject b) {
			return (!b.IsNull &&
			        b.TType is TBooleanType &&
			        b.Object.Equals(true));
		}


		// ---------- The different types of operator's we can have ----------

		#region Nested type: AddOperator

		[Serializable]
		private sealed class AddOperator : Operator {
			public AddOperator()
				: base("+", 10) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2,
			                                 IGroupResolver group, IVariableResolver resolver,
			                                 IQueryContext context) {
				return ob1.Add(ob2);
			}
		} ;

		#endregion

		#region Nested type: AllOperator

		[Serializable]
		private sealed class AllOperator : Operator {
			public AllOperator(String op)
				: base(op, 8, OperatorSet.ALL) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2,
			                                 IGroupResolver group, IVariableResolver resolver,
			                                 IQueryContext context) {
				if (ob2.TType is TQueryPlanType) {
					// The sub-query plan
					IQueryPlanNode plan = (IQueryPlanNode)ob2.Object;
					// Discover the correlated variables for this plan.
					ArrayList list = plan.DiscoverCorrelatedVariables(1, new ArrayList());

					if (list.Count > 0) {
						// Set the correlated variables from the IVariableResolver
						for (int i = 0; i < list.Count; ++i) {
							((CorrelatedVariable) list[i]).SetFromResolver(resolver);
						}
						// Clear the cache in the context
						context.ClearCache();
					}

					// Evaluate the plan,
					Table t = plan.Evaluate(context);

					Operator rev_plain_op = GetSubQueryForm(OperatorSet.NONE).Reverse();
					if (t.AllColumnMatchesValue(0, rev_plain_op, ob1)) {
						return TObject.BooleanTrue;
					}
					return TObject.BooleanFalse;
				} else if (ob2.TType is TArrayType) {
					Operator plain_op = GetSubQueryForm(OperatorSet.NONE);
					Expression[] exp_list = (Expression[])ob2.Object;
					// Assume true unless otherwise found to be false or NULL.
					TObject ret_val = TObject.BooleanTrue;
					for (int i = 0; i < exp_list.Length; ++i) {
						TObject exp_item = exp_list[i].Evaluate(group, resolver, context);
						// If there is a null item, we return null if not otherwise found to
						// be false.
						if (exp_item.IsNull) {
							ret_val = TObject.BooleanNull;
						}
							// If it doesn't match return false
						else if (!IsTrue(plain_op.Evaluate(ob1, exp_item, null, null, null))) {
							return TObject.BooleanFalse;
						}
					}
					// Otherwise return true or null.  If all match and no NULLs return
					// true.  If all match and there are NULLs then return NULL.
					return ret_val;
				} else {
					throw new ApplicationException("Unknown RHS of ALL.");
				}
			}
		}

		#endregion

		#region Nested type: AndOperator

		[Serializable]
		private sealed class AndOperator : Operator {
			public AndOperator()
				: base("and", 2) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2,
			                                 IGroupResolver group, IVariableResolver resolver,
			                                 IQueryContext context) {
				bool isB1Null, isB2Null;
				Boolean b1 = ob1.ToBoolean(out isB1Null);
				Boolean b2 = ob2.ToBoolean(out isB2Null);

				// If either ob1 or ob2 are null
				if (isB1Null) {
					if (!isB2Null) {
						if (b2.Equals(false)) {
							return TObject.BooleanFalse;
						}
					}
					return TObject.BooleanNull;
				} else if (isB2Null) {
					if (b1.Equals(false)) {
						return TObject.BooleanFalse;
					}
					return TObject.BooleanNull;
				}

				// If both true.
				return TObject.GetBoolean(b1.Equals(true) &&
				                          b2.Equals(true));
			}
		}

		#endregion

		#region Nested type: AnyOperator

		[Serializable]
		private sealed class AnyOperator : Operator {
			public AnyOperator(String op)
				: base(op, 8, OperatorSet.ANY) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2,
			                                 IGroupResolver group, IVariableResolver resolver,
			                                 IQueryContext context) {
				if (ob2.TType is TQueryPlanType) {
					// The sub-query plan
					IQueryPlanNode plan = (IQueryPlanNode)ob2.Object;
					// Discover the correlated variables for this plan.
					ArrayList list = plan.DiscoverCorrelatedVariables(1, new ArrayList());

					if (list.Count > 0) {
						// Set the correlated variables from the IVariableResolver
						for (int i = 0; i < list.Count; ++i) {
							((CorrelatedVariable) list[i]).SetFromResolver(resolver);
						}
						// Clear the cache in the context
						context.ClearCache();
					}

					// Evaluate the plan,
					Table t = plan.Evaluate(context);

					// The ANY operation
					Operator rev_plain_op = GetSubQueryForm(OperatorSet.NONE).Reverse();
					if (t.ColumnMatchesValue(0, rev_plain_op, ob1)) {
						return TObject.BooleanTrue;
					}
					return TObject.BooleanFalse;
				} else if (ob2.TType is TArrayType) {
					Operator plain_op = GetSubQueryForm(OperatorSet.NONE);
					Expression[] exp_list = (Expression[])ob2.Object;
					// Assume there are no matches
					TObject ret_val = TObject.BooleanFalse;
					for (int i = 0; i < exp_list.Length; ++i) {
						TObject exp_item = exp_list[i].Evaluate(group, resolver, context);
						// If null value, return null if there isn't otherwise a match found.
						if (exp_item.IsNull) {
							ret_val = TObject.BooleanNull;
						}
							// If there is a match, the ANY set test is true
						else if (IsTrue(plain_op.Evaluate(ob1, exp_item, null, null, null))) {
							return TObject.BooleanTrue;
						}
					}
					// No matches, so return either false or NULL.  If there are no matches
					// and no nulls, return false.  If there are no matches and there are
					// nulls present, return null.
					return ret_val;
				} else {
					throw new ApplicationException("Unknown RHS of ANY.");
				}
			}
		}

		#endregion

		#region Nested type: ConcatOperator

		[Serializable]
		private sealed class ConcatOperator : Operator {
			public ConcatOperator()
				: base("||", 10) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2,
			                                 IGroupResolver group, IVariableResolver resolver,
			                                 IQueryContext context) {
				return ob1.Concat(ob2);
			}
		} ;

		#endregion

		#region Nested type: DivideOperator

		[Serializable]
		private sealed class DivideOperator : Operator {
			public DivideOperator()
				: base("/", 20) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2,
			                                 IGroupResolver group, IVariableResolver resolver,
			                                 IQueryContext context) {
				return ob1.Divide(ob2);
			}
		} ;

		#endregion

		#region Nested type: EqualOperator

		[Serializable]
		private sealed class EqualOperator : Operator {
			public EqualOperator()
				: base("=", 4) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2,
			                                 IGroupResolver group, IVariableResolver resolver,
			                                 IQueryContext context) {
				return ob1.IsEqual(ob2);
			}
		}

		#endregion

		#region Nested type: GreaterEqualOperator

		[Serializable]
		private sealed class GreaterEqualOperator : Operator {
			public GreaterEqualOperator()
				: base(">=", 4) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2,
			                                 IGroupResolver group, IVariableResolver resolver,
			                                 IQueryContext context) {
				return ob1.GreaterEquals(ob2);
			}
		}

		#endregion

		#region Nested type: GreaterOperator

		[Serializable]
		private sealed class GreaterOperator : Operator {
			public GreaterOperator()
				: base(">", 4) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2,
			                                 IGroupResolver group, IVariableResolver resolver,
			                                 IQueryContext context) {
				return ob1.Greater(ob2);
			}
		}

		#endregion

		#region Nested type: IsNotOperator

		[Serializable]
		private sealed class IsNotOperator : Operator {
			public IsNotOperator()
				: base("is not", 4) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2,
			                                 IGroupResolver group, IVariableResolver resolver,
			                                 IQueryContext context) {
				return ob1.Is(ob2).Not();
			}
		}

		#endregion

		#region Nested type: IsOperator

		[Serializable]
		private sealed class IsOperator : Operator {
			public IsOperator()
				: base("is", 4) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2,
			                                 IGroupResolver group, IVariableResolver resolver,
			                                 IQueryContext context) {
				return ob1.Is(ob2);
			}
		}

		#endregion

		#region Nested type: LesserEqualOperator

		[Serializable]
		private sealed class LesserEqualOperator : Operator {
			public LesserEqualOperator()
				: base("<=", 4) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2,
			                                 IGroupResolver group, IVariableResolver resolver,
			                                 IQueryContext context) {
				return ob1.LessEquals(ob2);
			}
		}

		#endregion

		#region Nested type: LesserOperator

		[Serializable]
		private sealed class LesserOperator : Operator {
			public LesserOperator()
				: base("<", 4) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2,
			                                 IGroupResolver group, IVariableResolver resolver,
			                                 IQueryContext context) {
				return ob1.Less(ob2);
			}
		}

		#endregion

		#region Nested type: MultiplyOperator

		[Serializable]
		private sealed class MultiplyOperator : Operator {
			public MultiplyOperator()
				: base("*", 20) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2,
			                                 IGroupResolver group, IVariableResolver resolver,
			                                 IQueryContext context) {
				return ob1.Multiply(ob2);
			}
		} ;

		#endregion

		#region Nested type: NotEqualOperator

		[Serializable]
		private sealed class NotEqualOperator : Operator {
			public NotEqualOperator()
				: base("<>", 4) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2,
			                                 IGroupResolver group, IVariableResolver resolver,
			                                 IQueryContext context) {
				return ob1.IsNotEqual(ob2);
			}
		}

		#endregion

		#region Nested type: OrOperator

		[Serializable]
		private sealed class OrOperator : Operator {
			public OrOperator()
				: base("or", 1) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2,
			                                 IGroupResolver group, IVariableResolver resolver,
			                                 IQueryContext context) {
				bool isB1Null, isB2Null;
				Boolean b1 = ob1.ToBoolean(out isB1Null);
				Boolean b2 = ob2.ToBoolean(out isB2Null);

				// If either ob1 or ob2 are null
				if (isB1Null) {
					if (!isB2Null) {
						if (b2.Equals(true)) {
							return TObject.BooleanTrue;
						}
					}
					return TObject.BooleanNull;
				} else if (isB2Null) {
					if (b1.Equals(true)) {
						return TObject.BooleanTrue;
					}
					return TObject.BooleanNull;
				}

				// If both true.
				return TObject.GetBoolean(b1.Equals(true) ||
				                          b2.Equals(true));
			}
		}

		#endregion

		#region Nested type: ParenOperator

		[Serializable]
		private sealed class ParenOperator : Operator {
			public ParenOperator(String paren)
				: base(paren) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2,
			                                 IGroupResolver group, IVariableResolver resolver,
			                                 IQueryContext context) {
				throw new ApplicationException("Parenthese should never be evaluated!");
			}
		}

		#endregion

		#region Nested type: PatternMatchFalseOperator

		[Serializable]
		private sealed class PatternMatchFalseOperator : Operator {
			public PatternMatchFalseOperator()
				: base("not like", 8) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2,
			                                 IGroupResolver group, IVariableResolver resolver,
			                                 IQueryContext context) {
				if (ob1.IsNull) {
					return ob1;
				}
				if (ob2.IsNull) {
					return ob2;
				}
				String val = ob1.CastTo(TType.StringType).ToStringValue();
				String pattern = ob2.CastTo(TType.StringType).ToStringValue();
				return TObject.GetBoolean(
					!PatternSearch.FullPatternMatch(pattern, val, '\\'));
			}
		}

		#endregion

		#region Nested type: PatternMatchTrueOperator

		[Serializable]
		private sealed class PatternMatchTrueOperator : Operator {
			public PatternMatchTrueOperator()
				: base("like", 8) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2,
			                                 IGroupResolver group, IVariableResolver resolver,
			                                 IQueryContext context) {
				if (ob1.IsNull) {
					return ob1;
				}
				if (ob2.IsNull) {
					return ob2;
				}
				String val = ob1.CastTo(TType.StringType).ToStringValue();
				String pattern = ob2.CastTo(TType.StringType).ToStringValue();

				TObject result = TObject.GetBoolean(
					PatternSearch.FullPatternMatch(pattern, val, '\\'));
				return result;
			}
		}

		#endregion

		#region Nested type: RegexOperator

		[Serializable]
		private sealed class RegexOperator : Operator {
			public RegexOperator()
				: base("regex", 8) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2,
			                                 IGroupResolver group, IVariableResolver resolver,
			                                 IQueryContext context) {
				if (ob1.IsNull) {
					return ob1;
				}
				if (ob2.IsNull) {
					return ob2;
				}
				String val = ob1.CastTo(TType.StringType).ToStringValue();
				String pattern = ob2.CastTo(TType.StringType).ToStringValue();
				return TObject.GetBoolean(PatternSearch.RegexMatch(
				                          	context.System, pattern, val));
			}
		}

		#endregion

		#region Nested type: SimpleOperator

		[Serializable]
		private sealed class SimpleOperator : Operator {
			public SimpleOperator(String str)
				: base(str) {
			}

			public SimpleOperator(String str, int prec)
				: base(str, prec) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2,
			                                 IGroupResolver group, IVariableResolver resolver,
			                                 IQueryContext context) {
				throw new ApplicationException("SimpleOperator should never be evaluated!");
			}
		}

		#endregion

		#region Nested type: SubtractOperator

		[Serializable]
		private sealed class SubtractOperator : Operator {
			public SubtractOperator()
				: base("-", 15) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2,
			                                 IGroupResolver group, IVariableResolver resolver,
			                                 IQueryContext context) {
				return ob1.Subtract(ob2);
			}
		} ;

		#endregion
	}
}