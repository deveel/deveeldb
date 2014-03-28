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

using Deveel.Data.QueryPlanning;
using Deveel.Data.Types;

namespace Deveel.Data {
	/// <summary>
	/// An operator for an expression.
	/// </summary>
	[Serializable]
	public abstract class Operator {
		// ---------- Statics ----------

		private static readonly Dictionary<string, Operator> AllMap = new Dictionary<string, Operator>(); 
		private static readonly Dictionary<string, Operator> AnyMap = new Dictionary<string, Operator>();

		private static readonly ParenOperator Par1Op = new ParenOperator("(");
		private static readonly ParenOperator Par2Op = new ParenOperator(")");

		// ---------- Member ----------

		/// <summary>
		/// A string that represents this operator.
		/// </summary>
		private readonly string op;

		/// <summary>
		/// The precedence of this operator.
		/// </summary>
		private readonly int precedence;

		/// <summary>
		/// If this is a set operator such as ANY or ALL then this is set with the flag type.
		/// </summary>
		private readonly OperatorSubType subType;

		/// <summary>
		/// Gets the operator used to evaluate the equality of
		/// two <see cref="TObject"/> passed as parameters.
		/// </summary>
		public static readonly Operator Equal = new EqualOperator();

		/// <summary>
		/// Gets the operator used to evaluate the inequality of
		/// two <see cref="TObject"/> passed as parameters.
		/// </summary>
		public static readonly Operator NotEqual = new NotEqualOperator();

		/// <summary>
		/// Gets an <see cref="Operator"/> used to evaluate if a given 
		/// <see cref="TObject"/> is greater than another one.
		/// </summary>
		public static readonly Operator Greater = new GreaterOperator();

		/// <summary>
		/// Gets an <see cref="Operator"/> used to evaluate if a given 
		/// <see cref="TObject"/> is smaller than another one.
		/// </summary>
		public static readonly Operator Lesser = new LesserOperator();

		/// <summary>
		/// Gets an <see cref="Operator"/> used to evaluate if a given 
		/// <see cref="TObject"/> is greater or equal than another one.
		/// </summary>
		public static readonly Operator GreaterEqual = new GreaterEqualOperator();

		/// <summary>
		/// Gets an <see cref="Operator"/> used to evaluate if a given 
		/// <see cref="TObject"/> is smaller or equal than another one.
		/// </summary>
		public static readonly Operator LesserEqual = new LesserEqualOperator();

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
		/// <seealso cref="Subtract"/>
		public static readonly Operator Add = new AddOperator();

		/// <summary>
		/// Gets an <see cref="Operator"/> that is used to compute
		/// a substraction between two arguments.
		/// </summary>
		/// <seealso cref="Add"/>
		public static readonly Operator Subtract = new SubtractOperator();

		/// <summary>
		/// Gets an <see cref="Operator"/> instance that multiplies
		/// a first given argument by a second one.
		/// </summary>
		public static readonly Operator Multiply = new MultiplyOperator();

		public static readonly Operator Divide = new DivideOperator();

		public static readonly Operator Modulo = new ModulusOperator();

		public static readonly Operator Concat = new ConcatOperator();

		public static readonly Operator Like = new PatternMatchTrueOperator();

		public static readonly Operator NotLike = new PatternMatchFalseOperator();

		public static readonly Operator SoundsLike = new SoundsLikeOperator();

		public static readonly Operator Regex = new RegexOperator();

		public static readonly Operator NotIn;

		public static readonly Operator In;

		public static readonly Operator Is = new IsOperator();

		public static readonly Operator IsNot = new IsNotOperator();

		public static readonly Operator Not = new SimpleOperator("not", 3);

		public static readonly Operator And = new AndOperator();

		public static readonly Operator Or = new OrOperator();

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
			In = AnyMap["="];
			NotIn = AllMap["<>"];
		}

		protected Operator(string op)
			: this(op, 0, OperatorSubType.None) {
		}

		protected Operator(string op, int precedence)
			: this(op, precedence, OperatorSubType.None) {
		}

		protected Operator(String op, int precedence, OperatorSubType subType) {
			if (subType != OperatorSubType.None && subType != OperatorSubType.Any && subType != OperatorSubType.All)
				throw new ArgumentException("Invalid sub type.", "subType");

			this.op = op;
			this.precedence = precedence;
			this.subType = subType;
		}


		/// <summary>
		/// Returns the string value of this operator.
		/// </summary>
		internal string StringRepresentation {
			get { return op; }
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
				return (Equals(Equal) ||
				        Equals(NotEqual) ||
				        Equals(Greater) ||
				        Equals(Lesser) ||
				        Equals(GreaterEqual) ||
				        Equals(LesserEqual) ||
				        Equals(Is) ||
				        Equals(IsNot));
			}
		}

		/// <summary>
		/// Gets <b>true</b> if the operator is a mathematical operator,
		/// otherwise <b>false</b>.
		/// </summary>
		public bool IsMathematical {
			get {
				return (Equals(Add) ||
				        Equals(Subtract) ||
				        Equals(Multiply) ||
				        Equals(Divide) ||
						Equals(Modulo) ||
				        Equals(Concat));
			}
		}

		/// <summary>
		/// Gets <b>true</b> if the operator is a pattern operator,
		/// otherwise <b>false</b>.
		/// </summary>
		public bool IsPattern {
			get {
				return (Equals(Like) ||
				        Equals(NotLike) ||
				        Equals(Regex));
			}
		}

		/// <summary>
		/// Gets <b>true</b> if the operator is a logical operator,
		/// otherwise <b>false</b>.
		/// </summary>
		public bool IsLogical {
			get {
				return (Equals(And) ||
				        Equals(Or));
			}
		}

		public bool IsNegation {
			get { return Equals(Not); }
		}

		/// <summary>
		/// Gets <b>true</b> if the operator is sub-query operator,
		/// otherwise <b>false</b>
		/// </summary>
		public bool IsSubQuery {
			get {
				return (subType != OperatorSubType.None ||
				        Equals(In) ||
				        Equals(NotIn));
			}
		}

		/// <summary>
		/// Returns true if this operator is not inversible.
		/// </summary>
		public bool IsNotInversible {
			get {
				// The REGEX op, and mathematical operators are not inversible.
				return Equals(Regex) || IsMathematical;
			}
		}

		/// <summary>
		/// The type of object this Operator evaluates to.
		/// </summary>
		public TType ReturnTType {
			get {
				if (Equals(Concat))
					return TType.StringType;
				if (IsMathematical)
					return TType.NumericType;
				return TType.BooleanType;
			}
		}

		///<summary>
		///</summary>
		///<param name="givenOp"></param>
		///<returns></returns>
		public bool IsEquivalent(string givenOp) {
			return givenOp.Equals(op);
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
		public abstract TObject Evaluate(TObject ob1, TObject ob2, IGroupResolver group, IVariableResolver resolver, IQueryContext context);

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
			if (Equals(Equal) || Equals(NotEqual) || Equals(Is) || Equals(IsNot))
				return this;
			if (Equals(Greater))
				return Lesser;
			if (Equals(Lesser))
				return Greater;
			if (Equals(GreaterEqual))
				return LesserEqual;
			if (Equals(LesserEqual))
				return GreaterEqual;

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
			if (Equals(Equal))
				return NotEqual;
			if (Equals(NotEqual))
				return Equal;
			if (Equals(Greater))
				return LesserEqual;
			if (Equals(Lesser))
				return GreaterEqual;
			if (Equals(GreaterEqual))
				return Lesser;
			if (Equals(LesserEqual))
				return Greater;
			if (Equals(And))
				return Or;
			if (Equals(Or))
				return And;
			if (Equals(Like))
				return NotLike;
			if (Equals(NotLike))
				return Like;
			if (Equals(Is))
				return IsNot;
			if (Equals(IsNot))
				return Is;

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
			if (s.Equals("SINGLE") || s.Equals("ANY") || s.Equals("SOME"))
				return GetSubQueryForm(OperatorSubType.Any);
			if (s.Equals("ALL"))
				return GetSubQueryForm(OperatorSubType.All);

			throw new ApplicationException("Do not understand subquery type '" + typeString + "'");
		}

		/// <inheritdoc/>
		public override string ToString() {
			StringBuilder sb = new StringBuilder();
			sb.Append(op);
			if (subType == OperatorSubType.Any) {
				sb.Append(" ANY");
			} else if (subType == OperatorSubType.All) {
				sb.Append(" ALL");
			}
			return sb.ToString();
		}

		/// <inheritdoc/>
		public override bool Equals(Object ob) {
			Operator oob = (Operator)ob;
			return op.Equals(oob.op) && subType == oob.subType;
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return op.GetHashCode() ^ subType.GetHashCode();
		}


		/// <summary>
		/// Returns an Operator with the given string.
		/// </summary>
		/// <param name="op"></param>
		/// <returns></returns>
		public static Operator Get(string op) {
			if (op.Equals("+"))
				return Add;
			if (op.Equals("-"))
				return Subtract;
			if (op.Equals("*"))
				return Multiply;
			if (op.Equals("/"))
				return Divide;
			if (op.Equals("%"))
				return Modulo;
			if (op.Equals("||"))
				return Concat;
			if (op.Equals("=") | op.Equals("=="))
				return Equal;
			if (op.Equals("<>") | op.Equals("!="))
				return NotEqual;
			if (op.Equals(">"))
				return Greater;
			if (op.Equals("<"))
				return Lesser;
			if (op.Equals(">="))
				return GreaterEqual;
			if (op.Equals("<="))
				return LesserEqual;
			if (op.Equals("("))
				return Par1Op;
			if (op.Equals(")"))
				return Par2Op;

			// Operators that are words, convert to lower case...
			op = op.ToLower();
			if (op.Equals("is", StringComparison.InvariantCultureIgnoreCase))
				return Is;
			if (op.Equals("is not", StringComparison.InvariantCultureIgnoreCase))
				return IsNot;
			if (op.Equals("like", StringComparison.InvariantCultureIgnoreCase))
				return Like;
			if (op.Equals("not like", StringComparison.InvariantCultureIgnoreCase))
				return NotLike;
			if (op.Equals("sounds like", StringComparison.InvariantCultureIgnoreCase))
				return SoundsLike;
			if (op.Equals("regex", StringComparison.InvariantCultureIgnoreCase))
				return Regex;
			if (op.Equals("in", StringComparison.InvariantCultureIgnoreCase))
				return In;
			if (op.Equals("not in", StringComparison.InvariantCultureIgnoreCase))
				return NotIn;
			if (op.Equals("not", StringComparison.InvariantCultureIgnoreCase))
				return Not;
			if (op.Equals("and", StringComparison.InvariantCultureIgnoreCase))
				return And;
			if (op.Equals("or", StringComparison.InvariantCultureIgnoreCase))
				return Or;


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

			public override TObject Evaluate(TObject ob1, TObject ob2, IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				return ob1.Add(ob2);
			}
		}

		#endregion

		#region Nested type: AllOperator

		[Serializable]
		private sealed class AllOperator : Operator {
			public AllOperator(String op)
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
						foreach (CorrelatedVariable variable in list) {
							variable.SetFromResolver(resolver);
						}
						// Clear the cache in the context
						context.ClearCache();
					}

					// Evaluate the plan,
					Table t = plan.Evaluate(context);

					Operator revPlainOp = GetSubQueryForm(OperatorSubType.None).Reverse();
					return t.AllColumnMatchesValue(0, revPlainOp, ob1);
				}
				if (ob2.TType is TArrayType) {
					Operator plainOp = GetSubQueryForm(OperatorSubType.None);
					Expression[] expList = (Expression[])ob2.Object;
					// Assume true unless otherwise found to be false or NULL.
					TObject retVal = TObject.BooleanTrue;
					foreach (Expression exp in expList) {
						TObject expItem = exp.Evaluate(group, resolver, context);
						// If there is a null item, we return null if not otherwise found to
						// be false.
						if (expItem.IsNull) {
							retVal = TObject.BooleanNull;
						} else if (!IsTrue(plainOp.Evaluate(ob1, expItem, null, null, null))) {
							// If it doesn't match return false
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
					return b2.HasValue && b2.Equals(false) ? TObject.BooleanFalse : TObject.BooleanNull;
				if (!b2.HasValue)
					return b1.Equals(false) ? TObject.BooleanFalse : TObject.BooleanNull;

				// If both true.
				return TObject.CreateBoolean(b1.Equals(true) && b2.Equals(true));
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
						foreach (CorrelatedVariable variable in list) {
							variable.SetFromResolver(resolver);
						}
						// Clear the cache in the context
						context.ClearCache();
					}

					// Evaluate the plan,
					Table t = plan.Evaluate(context);

					// The ANY operation
					Operator revPlainOp = GetSubQueryForm(OperatorSubType.None).Reverse();
					return t.ColumnMatchesValue(0, revPlainOp, ob1);
				}
				if (ob2.TType is TArrayType) {
					Operator plain_op = GetSubQueryForm(OperatorSubType.None);
					Expression[] expList = (Expression[])ob2.Object;
					// Assume there are no matches
					TObject retVal = TObject.BooleanFalse;
					foreach (Expression exp in expList) {
						TObject exp_item = exp.Evaluate(group, resolver, context);
						// If null value, return null if there isn't otherwise a match found.
						if (exp_item.IsNull) {
							retVal = TObject.BooleanNull;
						} else if (IsTrue(plain_op.Evaluate(ob1, exp_item, null, null, null))) {
							// If there is a match, the ANY set test is true
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

			public override TObject Evaluate(TObject ob1, TObject ob2,
			                                 IGroupResolver group, IVariableResolver resolver,
			                                 IQueryContext context) {
				bool? b1 = ob1.ToNullableBoolean();
				bool? b2 = ob2.ToNullableBoolean();

				// If either ob1 or ob2 are null
				if (!b1.HasValue)
					return b2.HasValue && b2.Value.Equals(true) ? TObject.BooleanTrue : TObject.BooleanNull;
				if (!b2.HasValue)
					return b1.Value.Equals(true) ? TObject.BooleanTrue : TObject.BooleanNull;

				// If both true.
				return TObject.CreateBoolean(b1.Equals(true) || b2.Equals(true));
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