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
using System.Collections.Generic;
using System.Text;

namespace Deveel.Data {
	/// <summary>
	/// An operator for an expression.
	/// </summary>
	[Serializable]
	public abstract class Operator {
		// ---------- Statics ----------
		// ANY/ALL
		private static readonly Dictionary<string, Operator> AllMap = new Dictionary<string, Operator>();
		private static readonly Dictionary<string, Operator> AnyMap = new Dictionary<string, Operator>();

		// LOGICAL
		private static readonly AndOperator AndOp = new AndOperator();
		private static readonly OrOperator OrOp = new OrOperator();
		private static readonly Operator InOp;
		private static readonly Operator NinOp;
		private static readonly Operator NotOp = new SimpleOperator("not", 3);
		private static readonly EqualOperator EqOp = new EqualOperator();
		private static readonly NotEqualOperator NeqOp = new NotEqualOperator();
		private static readonly IsOperator IsOp = new IsOperator();
		private static readonly IsNotOperator IsnOp = new IsNotOperator();
		private static readonly GreaterOperator GOp = new GreaterOperator();
		private static readonly GreaterEqualOperator GeqOp = new GreaterEqualOperator();
		private static readonly LesserOperator LOp = new LesserOperator();
		private static readonly LesserEqualOperator LeqOp = new LesserEqualOperator();

		// ARITHMETICAL
		private static readonly AddOperator AddOp = new AddOperator();
		private static readonly SubtractOperator SubOp = new SubtractOperator();
		private static readonly ConcatOperator ConcatOp = new ConcatOperator();
		private static readonly DivideOperator DivOp = new DivideOperator();
		private static readonly ModulusOperator ModOp = new ModulusOperator();
		private static readonly MultiplyOperator MulOp = new MultiplyOperator();

		// STRING PATTERN
		private static readonly PatternMatchTrueOperator LikeOp = new PatternMatchTrueOperator();
		private static readonly SoundsLikeOperator SlikeOp = new SoundsLikeOperator();
		private static readonly PatternMatchFalseOperator NlikeOp = new PatternMatchFalseOperator();
		private static readonly RegexOperator RegexOp = new RegexOperator();

		private static readonly ParenOperator Par1Op = new ParenOperator("(");
		private static readonly ParenOperator Par2Op = new ParenOperator(")");

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
		private readonly OperatorSubType subType;

		static Operator() {
			// Populate the static ANY and ALL mapping
			AnyMap.Add("=", new AnyOperator("="));
			AnyMap.Add("<>", new AnyOperator("<>"));
			AnyMap.Add(">", new AnyOperator(">"));
			AnyMap.Add(">=", new AnyOperator(">="));
			AnyMap.Add("<", new AnyOperator("<"));
			AnyMap.Add("<=", new AnyOperator("<="));

			AllMap.Add("=", new AllOperator("="));
			AllMap.Add("<>", new AllOperator("<>"));
			AllMap.Add(">", new AllOperator(">"));
			AllMap.Add(">=", new AllOperator(">="));
			AllMap.Add("<", new AllOperator("<"));
			AllMap.Add("<=", new AllOperator("<="));

			// The IN and NOT IN operator are '= ANY' and '<> ALL' respectively.
			InOp = AnyMap["="];
			NinOp = AllMap["<>"];
		}

		protected Operator(string op)
			: this(op, 0, OperatorSubType.None) {
		}

		protected Operator(string op, int precedence)
			: this(op, precedence, OperatorSubType.None) {
		}

		protected Operator(string op, int precedence, OperatorSubType subType) {
			if (subType != OperatorSubType.None && 
				subType != OperatorSubType.Any && 
				subType != OperatorSubType.All)
				throw new ArgumentException("Invlid subType.", "subType");

			this.op = op;
			this.precedence = precedence;
			this.subType = subType;
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
				return (Equals(EqOp) ||
				        Equals(NeqOp) ||
				        Equals(GOp) ||
				        Equals(LOp) ||
				        Equals(GeqOp) ||
				        Equals(LeqOp) ||
				        Equals(IsOp) ||
				        Equals(IsnOp));
			}
		}

		/// <summary>
		/// Gets <b>true</b> if the operator is a mathematical operator,
		/// otherwise <b>false</b>.
		/// </summary>
		public bool IsMathematical {
			get {
				return (Equals(AddOp) ||
				        Equals(SubOp) ||
				        Equals(MulOp) ||
				        Equals(DivOp) ||
						Equals(ModOp) ||
				        Equals(ConcatOp));
			}
		}

		/// <summary>
		/// Gets <b>true</b> if the operator is a pattern operator,
		/// otherwise <b>false</b>.
		/// </summary>
		public bool IsPattern {
			get {
				return (Equals(LikeOp) ||
				        Equals(NlikeOp) ||
				        Equals(RegexOp));
			}
		}

		/// <summary>
		/// Gets <b>true</b> if the operator is a logical operator,
		/// otherwise <b>false</b>.
		/// </summary>
		public bool IsLogical {
			get {
				return (Equals(AndOp) ||
				        Equals(OrOp));
			}
		}

		/// <summary>
		/// Gets the <i>is not</i> conditional operator (<pre>IS NOT</pre>).
		/// </summary>
		public bool IsNot {
			get { return Equals(NotOp); }
		}

		/// <summary>
		/// Gets <b>true</b> if the operator is sub-query operator,
		/// otherwise <b>false</b>
		/// </summary>
		public bool IsSubQuery {
			get {
				return (subType != OperatorSubType.None ||
				        Equals(InOp) ||
				        Equals(NinOp));
			}
		}

		/// <summary>
		/// Returns true if this operator is not inversible.
		/// </summary>
		public bool IsNotInversible {
			get {
				// The REGEX op, and mathematical operators are not inversible.
				return Equals(RegexOp) || IsMathematical;
			}
		}

		/// <summary>
		/// Returns the sub query representation of this operator.
		/// </summary>
		private OperatorSubType SubQueryFormRepresentation {
			get { return subType; }
		}

		/// <summary>
		/// The type of object this Operator evaluates to.
		/// </summary>
		public TType ReturnTType {
			get {
				if (Equals(ConcatOp))
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

		/// <summary>
		/// Gets the operator used to evaluate the equality of
		/// two <see cref="TObject"/> passed as parameters.
		/// </summary>
		public static Operator Equal {
			get { return EqOp; }
		}

		/// <summary>
		/// Gets the operator used to evaluate the inequality of
		/// two <see cref="TObject"/> passed as parameters.
		/// </summary>
		public static Operator NotEqual {
			get { return NeqOp; }
		}

		/// <summary>
		/// Gets an <see cref="Operator"/> used to evaluate if a given 
		/// <see cref="TObject"/> is greater than another one.
		/// </summary>
		public static Operator Greater {
			get { return GOp; }
		}

		/// <summary>
		/// Gets an <see cref="Operator"/> used to evaluate if a given 
		/// <see cref="TObject"/> is smaller than another one.
		/// </summary>
		public static Operator Lesser {
			get { return LOp; }
		}

		/// <summary>
		/// Gets an <see cref="Operator"/> used to evaluate if a given 
		/// <see cref="TObject"/> is greater or equal than another one.
		/// </summary>
		public static Operator GreaterEqual {
			get { return GeqOp; }
		}

		/// <summary>
		/// Gets an <see cref="Operator"/> used to evaluate if a given 
		/// <see cref="TObject"/> is smaller or equal than another one.
		/// </summary>
		public static Operator LesserEqual {
			get { return LeqOp; }
		}

		/// <summary>
		/// Gets an <see cref="Operator"/> that is used to compute an 
		/// addition between two <see cref="TObject"/> given.
		/// </summary>
		/// <remarks>
		/// The addition of an argument to another can be of the following
		/// forms:
		/// <list type="bullet">
		///   <item>
		///     <term>Mathematic</term>
		///     <description>two numeric arguments</description>
		///   </item>
		///   <item>
		///     <term>Numeric to time</term>
		///     <description>the first argument is a <c>TIME</c> type
		///     and the second is a <c>NUMERIC</c> value representing
		///     the number of milliseconds to add, that will return
		///     an <c>TIME</c>;</description>
		///   </item>
		///   <item>
		///     <term>Interval to time</term>
		///     <description>an <c>INTERVAL</c> of time is added to the 
		///     first argument of type <c>DATE</c> resulting into another
		///    <c>TIME</c> type.</description>
		///   </item>
		///   <item>
		///     <term>String to string</term>
		///     <description>conctas two strings (like <see cref="Concat"/>
		///     operator)</description>
		///   </item>
		/// </list>
		/// </remarks>
		/// <seealso cref="Concat"/>
		/// <seealso cref="Substract"/>
		public static Operator Add {
			get { return AddOp; }
		}

		/// <summary>
		/// Gets an <see cref="Operator"/> that is used to compute
		/// a substraction between two arguments.
		/// </summary>
		/// <seealso cref="Add"/>
		public static Operator Substract {
			get { return SubOp; }
		}

		/// <summary>
		/// Gets an <see cref="Operator"/> instance that multiplies
		/// a first given argument by a second one.
		/// </summary>
		public static Operator Multiply {
			get { return MulOp; }
		}

		public static Operator Divide {
			get { return DivOp; }
		}

		public static Operator Modulo {
			get { return ModOp; }
		}

		public static Operator Concat {
			get { return ConcatOp; }
		}

		public static Operator Like {
			get { return LikeOp; }
		}

		public static Operator NotLike {
			get { return NlikeOp; }
		}

		public static Operator SoundsLike {
			get { return SlikeOp; }
		}

		public static Operator Regex {
			get { return RegexOp; }
		}

		public static Operator NotIn {
			get { return NinOp; }
		}

		public static Operator In {
			get { return InOp; }
		}

		public static Operator Not {
			get { return NotOp; }
		}

		public static Operator And {
			get { return AndOp; }
		}

		public static Operator Or {
			get { return OrOp; }
		}

		///<summary>
		/// Checks if the given operator string representation equals
		/// the current one.
		///</summary>
		///<param name="givenOp"></param>
		///<returns></returns>
		public bool Is(string givenOp) {
			return op.Equals(givenOp);
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
		/// Evaluates two <see cref="TObject"/> instances outside a context.
		/// </summary>
		/// <param name="obj1">First operand.</param>
		/// <param name="obj2">Second operand.</param>
		/// <returns>
		/// Returns a <see cref="TObject"/> as result of the evaluation.
		/// </returns>
		public TObject Evaluate(TObject obj1, TObject obj2) {
			return Evaluate(obj1, obj2, null, null, null);
		}

		/// <summary>
		/// Returns an Operator that is the reverse of this Operator.
		/// </summary>
		/// <remarks>
		/// This is used for reversing a conditional expression. eg. <c>9 &gt; id</c> 
		/// becomes <c>id &lt; 9</c>.
		/// </remarks>
		public Operator Reverse() {
			if (Equals(EqOp) || Equals(NeqOp) || Equals(IsOp) || Equals(IsnOp))
				return this;
			if (Equals(GOp))
				return LOp;
			if (Equals(LOp))
				return GOp;
			if (Equals(GeqOp))
				return LeqOp;
			if (Equals(LeqOp))
				return GeqOp;

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
				OperatorSubType invType;
				if (IsSubQueryForm(OperatorSubType.Any)) {
					invType = OperatorSubType.All;
				} else if (IsSubQueryForm(OperatorSubType.All)) {
					invType = OperatorSubType.Any;
				} else {
					throw new Exception("Can not handle sub-query form.");
				}

				Operator invOp = Get(op).Inverse();

				return invOp.GetSubQueryForm(invType);
			}
			if (Equals(EqOp))
				return NeqOp;
			if (Equals(NeqOp))
				return EqOp;
			if (Equals(GOp))
				return LeqOp;
			if (Equals(LOp))
				return GeqOp;
			if (Equals(GeqOp))
				return LOp;
			if (Equals(LeqOp))
				return GOp;
			if (Equals(AndOp))
				return OrOp;
			if (Equals(OrOp))
				return AndOp;
			if (Equals(LikeOp))
				return NlikeOp;
			if (Equals(NlikeOp))
				return LikeOp;
			if (Equals(IsOp))
				return IsnOp;
			if (Equals(IsnOp))
				return IsOp;

			throw new ApplicationException("Can't inverse operator '" + op + "'");
		}


		/// <summary>
		/// Given a parameter of either NONE, ANY, ALL or SINGLE, this returns true
		/// if this operator is of the given type.
		/// </summary>
		/// <param name="type"></param>
		/// <returns></returns>
		public bool IsSubQueryForm(OperatorSubType type) {
			return type == subType;
		}

		/// <summary>
		/// Returns the ANY or ALL form of this operator.
		/// </summary>
		/// <param name="type"></param>
		/// <returns></returns>
		public Operator GetSubQueryForm(OperatorSubType type) {
			Operator resultOp = null;
			if (type == OperatorSubType.Any) {
				AnyMap.TryGetValue(op, out resultOp);
			} else if (type == OperatorSubType.All) {
				AllMap.TryGetValue(op, out resultOp);
			} else if (type == OperatorSubType.None) {
				resultOp = Get(op);
			}

			if (resultOp == null)
				throw new ApplicationException("Couldn't change the form of operator '" + op + "'.");

			return resultOp;
		}

		/// <summary>
		/// Returns the ANY or ALL form of this operator.
		/// </summary>
		/// <param name="typeString"></param>
		/// <returns></returns>
		public Operator GetSubQueryForm(string typeString) {
			string s = typeString.ToUpper();
			if (s.Equals("SINGLE", StringComparison.InvariantCultureIgnoreCase) || 
				s.Equals("ANY", StringComparison.InvariantCultureIgnoreCase) ||
				s.Equals("SOME", StringComparison.InvariantCultureIgnoreCase))
				return GetSubQueryForm(OperatorSubType.Any);
			if (s.Equals("ALL", StringComparison.InvariantCultureIgnoreCase))
				return GetSubQueryForm(OperatorSubType.All);

			throw new ApplicationException("Do not understand subquery type '" + typeString + "'");
		}

		/// <inheritdoc/>
		public override string ToString() {
			StringBuilder buf = new StringBuilder();
			buf.Append(op);
			if (subType == OperatorSubType.Any) {
				buf.Append(" ANY");
			} else if (subType == OperatorSubType.All) {
				buf.Append(" ALL");
			}
			return buf.ToString();
		}

		/// <inheritdoc/>
		public override bool Equals(object obj) {
			Operator other = (Operator)obj;
			return op.Equals(other.op) && subType == other.subType;
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return op.GetHashCode() + subType.GetHashCode();
		}


		/// <summary>
		/// Returns an Operator with the given string.
		/// </summary>
		/// <param name="op"></param>
		/// <returns></returns>
		public static Operator Get(String op) {
			if (op.Equals("+"))
				return AddOp;
			if (op.Equals("-"))
				return SubOp;
			if (op.Equals("*"))
				return MulOp;
			if (op.Equals("/"))
				return DivOp;
			if (op.Equals("%"))
				return ModOp;
			if (op.Equals("||"))
				return ConcatOp;
			if (op.Equals("=") | op.Equals("=="))
				return EqOp;
			if (op.Equals("<>") | op.Equals("!="))
				return NeqOp;
			if (op.Equals(">"))
				return GOp;
			if (op.Equals("<"))
				return LOp;
			if (op.Equals(">="))
				return GeqOp;
			if (op.Equals("<="))
				return LeqOp;
			if (op.Equals("("))
				return Par1Op;
			if (op.Equals(")"))
				return Par2Op;

			// Operators that are words, convert to lower case...
			op = op.ToLower();
			if (op.Equals("is"))
				return IsOp;
			if (op.Equals("is not"))
				return IsnOp;
			if (op.Equals("like"))
				return LikeOp;
			if (op.Equals("not like"))
				return NlikeOp;
			if (op.Equals("sounds like"))
				return SlikeOp;
			if (op.Equals("regex"))
				return RegexOp;
			if (op.Equals("in"))
				return InOp;
			if (op.Equals("not in"))
				return NinOp;
			if (op.Equals("not"))
				return NotOp;
			if (op.Equals("and"))
				return AndOp;
			if (op.Equals("or"))
				return OrOp;


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
			public AllOperator(string op)
				: base(op, 8, OperatorSubType.All) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2, IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				if (ob2.TType is TQueryPlanType) {
					// The sub-query plan
					IQueryPlanNode plan = (IQueryPlanNode)ob2.Object;
					// Discover the correlated variables for this plan.
					IList<CorrelatedVariable> list = plan.DiscoverCorrelatedVariables(1, new List<CorrelatedVariable>());

					if (list.Count > 0) {
						// Set the correlated variables from the IVariableResolver
						for (int i = 0; i < list.Count; ++i) {
							list[i].SetFromResolver(resolver);
						}
						// Clear the cache in the context
						context.ClearCache();
					}

					// Evaluate the plan,
					Table t = plan.Evaluate(context);

					Operator revPlainOp = GetSubQueryForm(OperatorSubType.None).Reverse();
					return t.AllColumnMatchesValue(0, revPlainOp, ob1) ? TObject.BooleanTrue : TObject.BooleanFalse;
				} 
				if (ob2.TType is TArrayType) {
					Operator plainOp = GetSubQueryForm(OperatorSubType.None);
					Expression[] expList = (Expression[])ob2.Object;
					// Assume true unless otherwise found to be false or NULL.
					TObject retVal = TObject.BooleanTrue;
					for (int i = 0; i < expList.Length; ++i) {
						TObject expItem = expList[i].Evaluate(group, resolver, context);
						// If there is a null item, we return null if not otherwise found to
						// be false.
						if (expItem.IsNull) {
							retVal = TObject.BooleanNull;
						}
							// If it doesn't match return false
						else if (!IsTrue(plainOp.Evaluate(ob1, expItem, null, null, null))) {
							return TObject.BooleanFalse;
						}
					}
					// Otherwise return true or null.  If all match and no NULLs return
					// true.  If all match and there are NULLs then return NULL.
					return retVal;
				}
					
				throw new ApplicationException("Unknown RHS of ALL.");
			}
		}

		#endregion

		#region Nested type: AndOperator

		[Serializable]
		private sealed class AndOperator : Operator {
			public AndOperator()
				: base("and", 2) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2, IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				bool? b1 = ob1.ToNullableBoolean();
				bool? b2 = ob2.ToNullableBoolean();

				// If either ob1 or ob2 are null
				if (!b1.HasValue)
					return b2.HasValue ? (b2.Equals(false) ? TObject.BooleanFalse : TObject.BooleanNull) : TObject.BooleanNull;
				if (!b2.HasValue)
					return b1.Equals(false) ? TObject.BooleanFalse : TObject.BooleanNull;

				// If both true.
				return TObject.CreateBoolean(b1.Equals(true) & b2.Equals(true));
			}
		}

		#endregion

		#region Nested type: AnyOperator

		[Serializable]
		private sealed class AnyOperator : Operator {
			public AnyOperator(String op)
				: base(op, 8, OperatorSubType.Any) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2, IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				if (ob2.TType is TQueryPlanType) {
					// The sub-query plan
					IQueryPlanNode plan = (IQueryPlanNode)ob2.Object;
					// Discover the correlated variables for this plan.
					IList<CorrelatedVariable> list = plan.DiscoverCorrelatedVariables(1, new List<CorrelatedVariable>());

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
					Operator revPlainOp = GetSubQueryForm(OperatorSubType.None).Reverse();
					if (t.ColumnMatchesValue(0, revPlainOp, ob1)) {
						return TObject.BooleanTrue;
					}
					return TObject.BooleanFalse;
				} 
				if (ob2.TType is TArrayType) {
					Operator plainOp = GetSubQueryForm(OperatorSubType.None);
					Expression[] expList = (Expression[])ob2.Object;
					// Assume there are no matches
					TObject retVal = TObject.BooleanFalse;
					for (int i = 0; i < expList.Length; ++i) {
						TObject expItem = expList[i].Evaluate(group, resolver, context);
						// If null value, return null if there isn't otherwise a match found.
						if (expItem.IsNull) {
							retVal = TObject.BooleanNull;
						}
							// If there is a match, the ANY set test is true
						else if (IsTrue(plainOp.Evaluate(ob1, expItem, null, null, null))) {
							return TObject.BooleanTrue;
						}
					}
					// No matches, so return either false or NULL.  If there are no matches
					// and no nulls, return false.  If there are no matches and there are
					// nulls present, return null.
					return retVal;
				}
					
				throw new ApplicationException("Unknown RHS of ANY.");
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

		#region ModulusOperator

		[Serializable]
		private sealed class ModulusOperator : Operator {
			public ModulusOperator()
				: base("%", 20) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2, IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				return ob1.Modulus(ob2);
			}
		}

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

		#region SoundsLikeOperator

		private sealed class SoundsLikeOperator : Operator {
			public SoundsLikeOperator()
				: base("sounds like", 4) {
			}

			public override TObject Evaluate(TObject ob1, TObject ob2, IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				return ob1.SoundsLike(ob2);
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

			public override TObject Evaluate(TObject ob1, TObject ob2, IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				bool? b1 = ob1.ToNullableBoolean();
				bool? b2 = ob2.ToNullableBoolean();

				// If either ob1 or ob2 are null
				if (!b1.HasValue)
					return b2.HasValue ? (b2.Equals(true) ? TObject.BooleanTrue : TObject.BooleanNull) : TObject.BooleanNull;
				if (!b2.HasValue)
					return b1.Equals(true) ? TObject.BooleanTrue : TObject.BooleanNull;

				// If both true.
				return TObject.CreateBoolean(b1.Equals(true) | b2.Equals(true));
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
				return TObject.CreateBoolean(
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

				TObject result = TObject.CreateBoolean(
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
				if (ob1.IsNull)
					return ob1;
				if (ob2.IsNull)
					return ob2;

				string val = ob1.CastTo(TType.StringType).ToStringValue();
				string pattern = ob2.CastTo(TType.StringType).ToStringValue();
				return TObject.CreateBoolean(PatternSearch.RegexMatch(context.System, pattern, val));
			}
		}

		#endregion

		#region Nested type: SimpleOperator

		[Serializable]
		private sealed class SimpleOperator : Operator {
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