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
using System.Linq;
using System.Linq.Expressions;
using System.Text;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Query;
using Deveel.Data.Types;

namespace Deveel.Data.Sql {
	/// <summary>
	/// An operator used to evaluate binary operations.
	/// </summary>
	public abstract class BinaryOperator {
		private static readonly Dictionary<BinaryOperatorType, BinaryOperator> AnyMap;
		private static readonly Dictionary<BinaryOperatorType, BinaryOperator> AllMap;

		private static readonly Dictionary<string, BinaryOperator> StringMap; 

		/// <summary>
		/// The binary operator that adds two numeric values.
		/// </summary>
		/// <seealso cref="BinaryOperatorType.Add"/>
		public static readonly BinaryOperator Add = new SimpleOperator(BinaryOperatorType.Add, (left, right) => left.Add(right));

		/// <summary>
		/// The binary operator that subtracts two numeric values.
		/// </summary>
		/// <seealso cref="BinaryOperatorType.Subtract"/>
		public static readonly BinaryOperator Subtract = new SimpleOperator(BinaryOperatorType.Subtract, (left, right) => left.Subtract(right));

		/// <summary>
		/// The binary operator that multiplies two numeric values.
		/// </summary>
		/// <seealso cref="BinaryOperatorType.Multiply"/>
		public static readonly BinaryOperator Multiply = new SimpleOperator(BinaryOperatorType.Multiply, (left, right) => left.Multiply(right));

		/// <summary>
		/// The binary operator that computes the modulo of two numeric values.
		/// </summary>
		/// <seealso cref="BinaryOperatorType.Modulo"/>
		public static readonly BinaryOperator Modulo = new SimpleOperator(BinaryOperatorType.Modulo, (left, right) => left.Modulus(right));

		/// <summary>
		/// The binary operator that divides two numeric values.
		/// </summary>
		/// <seealso cref="BinaryOperatorType.Divide"/>
		public static readonly BinaryOperator Divide = new SimpleOperator(BinaryOperatorType.Divide, (left, right) => left.Divide(right));

		/// <summary>
		/// The operator that assesses the equality of two values.
		/// </summary>
		/// <seealso cref="BinaryOperatorType.Equal"/>
		public static readonly BinaryOperator Equal = new SimpleOperator(BinaryOperatorType.Equal, (left, right) => left.IsEqualTo(right));

		/// <summary>
		/// The operator that assesses the inequality of two values.
		/// </summary>
		/// <seealso cref="BinaryOperatorType.NotEqual"/>
		public static readonly BinaryOperator NotEqual = new SimpleOperator(BinaryOperatorType.NotEqual, (left, right) => left.IsNotEqualTo(right));

		/// <summary>
		/// The operator that assesses the first of two values is greather than the second.
		/// </summary>
		/// <seealso cref="BinaryOperatorType.GreaterThan"/>
		public static readonly BinaryOperator GreaterThan = new SimpleOperator(BinaryOperatorType.GreaterThan, (left, right) => left.IsGreaterThan(right));

		/// <summary>
		/// The operator that assesses the first of two values is smaller than the second.
		/// </summary>
		/// <seealso cref="BinaryOperatorType.SmallerThan"/>
		public static readonly BinaryOperator SmallerThan = new SimpleOperator(BinaryOperatorType.SmallerThan, (left, right) => left.IsSmallerThan(right));

		/// <summary>
		/// The operator that assesses the first of two values is greater or equal than the second.
		/// </summary>
		/// <seealso cref="BinaryOperatorType.GreaterOrEqualThan"/>
		public static readonly BinaryOperator GreaterOrEqualThan =
			new SimpleOperator(BinaryOperatorType.GreaterOrEqualThan, (left, right) => left.IsGreterOrEqualThan(right));

		/// <summary>
		/// The operator that assesses the first of two values is smaller or equal than the second.
		/// </summary>
		/// <seealso cref="BinaryOperatorType.SmallerOrEqualThan"/>
		public static readonly BinaryOperator SmallerOrEqualThan =
			new SimpleOperator(BinaryOperatorType.SmallerOrEqualThan, (left, right) => left.IsSmallerOrEqualThan(right));

		/// <summary>
		/// The operator that assesses the equivalence of two values.
		/// </summary>
		/// <seealso cref="BinaryOperatorType.Is"/>
		public static readonly BinaryOperator Is = new SimpleOperator(BinaryOperatorType.Is, (left, right) => left.Is(right));

		/// <summary>
		/// The operator that assesses the non equivalence of two values.
		/// </summary>
		/// <seealso cref="BinaryOperatorType.IsNot"/>
		public static readonly BinaryOperator IsNot = new SimpleOperator(BinaryOperatorType.IsNot, (left, right) => left.IsNot(right));

		/// <summary>
		/// The operator that assesses the first string operand is contained into the second string operand.
		/// </summary>
		/// <seealso cref="BinaryOperatorType.Like"/>
		public static readonly BinaryOperator Like = new SimpleOperator(BinaryOperatorType.Like, (left, right) => left.IsLike(right));

		/// <summary>
		/// The operator that assesses the first string operand is not contained into the second string operand.
		/// </summary>
		/// <seealso cref="BinaryOperatorType.NotLike"/>
		public static readonly BinaryOperator NotLike = new SimpleOperator(BinaryOperatorType.NotLike, (left, right) => left.IsNotLike(right));

		public static readonly BinaryOperator And = new SimpleOperator(BinaryOperatorType.And, (left, right) => left.And(right));
		public static readonly BinaryOperator Or = new SimpleOperator(BinaryOperatorType.Or, (left, right) => left.Or(right));
		public static readonly BinaryOperator XOr = new SimpleOperator(BinaryOperatorType.XOr, (left, right) => left.XOr(right));

		public static readonly BinaryOperator In;
		public static readonly BinaryOperator NotIn;

		public static readonly BinaryOperator AnyEqual = new AnyOperator(BinaryOperatorType.Equal);
		public static readonly BinaryOperator AnyNotEqual = new AnyOperator(BinaryOperatorType.NotEqual);
		public static readonly BinaryOperator AnyGreaterThan = new AnyOperator(BinaryOperatorType.GreaterThan);
		public static readonly BinaryOperator AnySmallerThan = new AnyOperator(BinaryOperatorType.SmallerThan);
		public static readonly BinaryOperator AnyGreaterOrEqualThan = new AnyOperator(BinaryOperatorType.GreaterOrEqualThan);
		public static readonly BinaryOperator AnySmallerOrEqualThan = new AnyOperator(BinaryOperatorType.SmallerOrEqualThan);

		public static readonly BinaryOperator AllEqual = new AllOperator(BinaryOperatorType.Equal);
		public static readonly BinaryOperator AllNotEqual = new AllOperator(BinaryOperatorType.NotEqual);
		public static readonly BinaryOperator AllGreaterThan = new AllOperator(BinaryOperatorType.GreaterThan);
		public static readonly BinaryOperator AllSmallerThan = new AllOperator(BinaryOperatorType.SmallerThan);
		public static readonly BinaryOperator AllGreaterOrEqualThan = new AllOperator(BinaryOperatorType.GreaterOrEqualThan);
		public static readonly BinaryOperator AllSmallerOrEqualThan = new AllOperator(BinaryOperatorType.SmallerOrEqualThan);

		protected BinaryOperator(BinaryOperatorType operatorType) 
			: this(operatorType, OperatorSubType.None) {
		}

		internal BinaryOperator(BinaryOperatorType operatorType, OperatorSubType subType) {
			OperatorType = operatorType;
			this.SubQueryType = subType;
		}

		static BinaryOperator() {
			AllMap = new Dictionary<BinaryOperatorType, BinaryOperator> {
				{BinaryOperatorType.Equal, AllEqual},
				{BinaryOperatorType.NotEqual, AllNotEqual},
				{BinaryOperatorType.GreaterThan, AllGreaterThan},
				{BinaryOperatorType.SmallerThan, AllSmallerThan},
				{BinaryOperatorType.GreaterOrEqualThan, AllGreaterOrEqualThan},
				{BinaryOperatorType.SmallerOrEqualThan, AllSmallerOrEqualThan}
			};

			AnyMap = new Dictionary<BinaryOperatorType, BinaryOperator> {
				{BinaryOperatorType.Equal, AnyEqual},
				{BinaryOperatorType.NotEqual, AnyNotEqual},
				{BinaryOperatorType.GreaterThan, AnyGreaterThan},
				{BinaryOperatorType.SmallerThan, AnySmallerThan},
				{BinaryOperatorType.GreaterOrEqualThan, AnyGreaterOrEqualThan},
				{BinaryOperatorType.SmallerOrEqualThan, AnySmallerOrEqualThan}
			};

			In = AnyMap[BinaryOperatorType.Equal];
			NotIn = AllMap[BinaryOperatorType.NotEqual];

			StringMap = new Dictionary<string, BinaryOperator>(StringComparer.InvariantCultureIgnoreCase) {
				{"=", Equal},
				{"<>", NotEqual},
				{"!=", NotEqual},
				{">", GreaterThan},
				{"<", SmallerThan},
				{">=", GreaterOrEqualThan},
				{"<=", SmallerOrEqualThan},
				{"+", Add},
				{"||", Add},
				{"-", Subtract},
				{"*", Multiply},
				{"/", Divide},
				{"%", Modulo},
				{"is", Is},
				{"is not", IsNot},
				{"like", Like},
				{"not like", NotLike},
				{"in", In},
				{"not in", NotIn},
				{"and", And},
				{"or", Or},
				{"xor", XOr},

				{"= any", AnyEqual},
				{"<> any", AnyNotEqual},
				{"!= any", AnyNotEqual},
				{"> any", AnyGreaterThan},
				{"< any", AnySmallerThan},
				{">= any", AnyGreaterOrEqualThan},
				{"<= any", AnySmallerOrEqualThan},

				{"= all", AllEqual},
				{"<> all", AllNotEqual},
				{"!= all", AllNotEqual},
				{"> all", AllGreaterThan},
				{"< all", AllSmallerThan},
				{">= all", AllGreaterOrEqualThan},
				{"<= all", AllSmallerOrEqualThan}
			};
		}

		/// <summary>
		/// Gets the type of conditional operator.
		/// </summary>
		public BinaryOperatorType OperatorType { get; private set; }

		/// <summary>
		/// Gets a boolean value indicating if this is either a <c>ANY</c> or <c>ALL</c>
		/// subquery condition.
		/// </summary>
		public bool IsSubQuery {
			get { return SubQueryType != OperatorSubType.None; }
		}

		/// <summary>
		/// Gets the kind of sub-query this operator handle.
		/// </summary>
		public OperatorSubType SubQueryType { get; private set; }

		/// <summary>
		/// Gets a boolean value indicating if this is an arithmetic operator.
		/// </summary>
		/// <seealso cref="BinaryOperatorType.Add"/>
		/// <seealso cref="BinaryOperatorType.Multiply"/>
		/// <seealso cref="BinaryOperatorType.Modulo"/>
		/// <seealso cref="BinaryOperatorType.Divide"/>
		/// <seealso cref="BinaryOperatorType.Subtract"/>
		public bool IsArithmetic {
			get {
				return OperatorType == BinaryOperatorType.Divide ||
				       OperatorType == BinaryOperatorType.Multiply ||
				       OperatorType == BinaryOperatorType.Modulo ||
				       OperatorType == BinaryOperatorType.Add ||
				       OperatorType == BinaryOperatorType.Subtract;
			}
		}

		/// <summary>
		/// Gets a boolean value indicating if this operator represents
		/// a search pattern (eg. <c>LIKE</c>).
		/// </summary>
		public bool IsPattern {
			get {
				return OperatorType == BinaryOperatorType.Like ||
				       OperatorType == BinaryOperatorType.NotLike;
			}
		}

		/// <summary>
		/// Gets the assesed data type of the result of a binary
		/// operation between two objects.
		/// </summary>
		/// <value>
		///   Returns a <see cref="DataType"/> that represents the type
		///   of the result object from an operation
		/// </value>
		public DataType ResultType {
			get {
				if (IsArithmetic)
					return PrimitiveTypes.Numeric();

				return PrimitiveTypes.Boolean();
			}
		}

		/// <summary>
		/// Gets an inversed form of this condition.
		/// </summary>
		/// <returns>
		/// Returns an instance of <see cref="BinaryOperator"/> that is the
		/// inverse form of this condition.
		/// </returns>
		public BinaryOperator Inverse() {
			if (IsSubQuery) {
				OperatorSubType invType;
				if (SubQueryType == OperatorSubType.Any) {
					invType = OperatorSubType.All;
				} else if (SubQueryType == OperatorSubType.All) {
					invType = OperatorSubType.Any;
				} else {
					throw new Exception("Can not handle sub-query form.");
				}

				var invOp = Get(OperatorType).Inverse();

				return invOp.AsSubQuery(invType);
			}

			switch (OperatorType) {
				case BinaryOperatorType.Equal:
					return NotEqual;
				case BinaryOperatorType.NotEqual:
					return Equal;
				case BinaryOperatorType.GreaterThan:
					return SmallerOrEqualThan;
				case BinaryOperatorType.SmallerThan:
					return GreaterOrEqualThan;
				case BinaryOperatorType.GreaterOrEqualThan:
					return SmallerThan;
				case BinaryOperatorType.SmallerOrEqualThan:
					return GreaterThan;
				case BinaryOperatorType.And:
					return Or;
				case BinaryOperatorType.Or:
					return And;
				case BinaryOperatorType.Like:
					return NotLike;
				case BinaryOperatorType.NotLike:
					return Like;
				case BinaryOperatorType.Is:
					return IsNot;
				case BinaryOperatorType.IsNot:
					return Is;
			}

			throw new InvalidOperationException(String.Format("Cannot inverse operator '{0}'", OperatorType));
		}

		/// <summary>
		/// Gets an operator that is equivalent to the given type.
		/// </summary>
		/// <param name="operatorType">The type of the binary operator to return.</param>
		/// <returns>
		/// Returns an instance of <see cref="BinaryOperator"/> that is compatible
		/// with the given <paramref name="operatorType">type</paramref>.
		/// </returns>
		public static BinaryOperator Get(BinaryOperatorType operatorType) {
			switch (operatorType) {
				case BinaryOperatorType.Equal:
					return Equal;
				case BinaryOperatorType.NotEqual:
					return NotEqual;
				case BinaryOperatorType.Is:
					return Is;
				case BinaryOperatorType.IsNot:
					return IsNot;
				case BinaryOperatorType.Add:
					return Add;
				case BinaryOperatorType.Subtract:
					return Subtract;
				case BinaryOperatorType.Multiply:
					return Multiply;
				case BinaryOperatorType.Divide:
					return Divide;
				case BinaryOperatorType.Modulo:
					return Modulo;
				case BinaryOperatorType.GreaterThan:
					return GreaterThan;
				case BinaryOperatorType.SmallerThan:
					return SmallerThan;
				case BinaryOperatorType.GreaterOrEqualThan:
					return GreaterOrEqualThan;
				case BinaryOperatorType.SmallerOrEqualThan:
					return SmallerOrEqualThan;
				case BinaryOperatorType.And:
					return And;
				case BinaryOperatorType.Or:
					return Or;
				case BinaryOperatorType.Like:
					return Like;
				case BinaryOperatorType.NotLike:
					return NotLike;
				default:
					throw new ArgumentException();
			}
		}

		/// <summary>
		/// Creates a reversed version of this condition.
		/// </summary>
		/// <returns>
		/// Returns an instance of <see cref="BinaryOperator"/> that is the
		/// reversed version of this operator.
		/// </returns>
		/// <exception cref="ApplicationException">
		/// In case this operator is not conditional, hence it's not reversable.
		/// </exception>
		public BinaryOperator Reverse() {
			if (OperatorType == BinaryOperatorType.Equal || 
				OperatorType == BinaryOperatorType.NotEqual || 
				OperatorType == BinaryOperatorType.Is || 
				OperatorType == BinaryOperatorType.IsNot)
				return this;
			if (OperatorType == BinaryOperatorType.GreaterThan)
				return SmallerThan;
			if (OperatorType == BinaryOperatorType.SmallerThan)
				return GreaterThan;
			if (OperatorType == BinaryOperatorType.GreaterOrEqualThan)
				return SmallerOrEqualThan;
			if (OperatorType == BinaryOperatorType.SmallerOrEqualThan)
				return GreaterOrEqualThan;

			throw new InvalidOperationException("Cannot reverse a non conditional operator.");
		}

		/// <summary>
		/// When overridden by a derived type, this method computes an operation
		/// between two static values to obtain a result.
		/// </summary>
		/// <param name="left">The left hand side of the operation.</param>
		/// <param name="right">The right hand side of the operation.</param>
		/// <param name="context">The <see cref="EvaluateContext">context</see> used
		/// to compute the operation.</param>
		/// <returns>
		/// Returns an instance of <see cref="DataObject"/> that is the result of the
		/// operation between the two provided values, given the context.
		/// </returns>
		public abstract DataObject Evaluate(DataObject left, DataObject right, EvaluateContext context);

		/// <summary>
		/// Computes a static binary operation between two values to obtain a result.
		/// </summary>
		/// <param name="left">The left hand side of the operation.</param>
		/// <param name="right">The right hand side of the operation.</param>
		/// <returns>
		/// Returns an instance of <see cref="DataObject"/> that is the result of the
		/// operation between the two provided values.
		/// </returns>
		/// <seealso cref="Evaluate(DataObject, DataObject, EvaluateContext)"/>
		public DataObject Evaluate(DataObject left, DataObject right) {
			return Evaluate(left, right, null);
		}

		/// <summary>
		/// Forms a new operator as a sub-query operator given the specified form.
		/// </summary>
		/// <param name="type">The type of sub-query operator to form.</param>
		/// <returns>
		/// Returns an instance of <see cref="BinaryOperator"/> that is a form
		/// of the given sub-query operator type given.
		/// </returns>
		/// <exception cref="InvalidOperationException">
		/// If the <see cref="OperatorType">type</see> of this operator cannot
		/// be formed as sub-query operator.
		/// </exception>
		public BinaryOperator AsSubQuery(OperatorSubType type) {
			BinaryOperator resultOp = null;
			if (type == OperatorSubType.Any) {
				AnyMap.TryGetValue(OperatorType, out resultOp);
			} else if (type == OperatorSubType.All) {
				AllMap.TryGetValue(OperatorType, out resultOp);
			} else if (type == OperatorSubType.None) {
				resultOp = Get(OperatorType);
			}

			if (resultOp == null)
				throw new InvalidOperationException(String.Format("Could not change the form of operator '{0}'.", OperatorType));

			return resultOp;
		}

		/// <inheritdoc/>
		public override bool Equals(object obj) {
			var other = (BinaryOperator) obj;
			return OperatorType == other.OperatorType &&
			       SubQueryType == other.SubQueryType;
		}

		public override int GetHashCode() {
			return OperatorType.GetHashCode() ^ SubQueryType.GetHashCode();
		}

		/// <summary>
		/// Checks if this operator is equivalent to the given type.
		/// </summary>
		/// <remarks>
		/// <para>
		/// This method is particularly useful when the operator is a
		/// sub-query form and an higher-level operation must be computed.
		/// </para>
		/// </remarks>
		/// <param name="operatorType">The kind of operator tho verify equivalence.</param>
		/// <returns>
		/// Returns <c>true</c> if this binary operator is equivalent to the given
		/// <paramref name="operatorType"/> or <c>false</c> otherwise.
		/// </returns>
		public bool IsOfType(BinaryOperatorType operatorType) {
			return OperatorType == operatorType;
		}

		public override string ToString() {
			var pair = StringMap.FirstOrDefault(x => x.Value.OperatorType == OperatorType);

			var sb = new StringBuilder();
			sb.Append(pair.Key);

			if (SubQueryType == OperatorSubType.All) {
				sb.Append(" ALL");
			} else if (SubQueryType == OperatorSubType.Any) {
				sb.Append(" ANY");
			}

			return sb.ToString();
		}

		/// <summary>
		/// Parses the given string into a valid binary operator.
		/// </summary>
		/// <param name="s">The input string to parse.</param>
		/// <returns>
		/// Returns an instance of <see cref="BinaryOperator"/> equivalent
		/// to the input string given.
		/// </returns>
		/// <exception cref="FormatException">
		/// If the given string does not represent a valid operator.
		/// </exception>
		/// <seealso cref="ToString"/>
		/// <seealso cref="TryParse"/>
		public static BinaryOperator Parse(string s) {
			BinaryOperator result;
			if (!TryParse(s, out result))
				throw new FormatException();

			return result;
		}

		/// <summary>
		/// Attempts to parse a string given and return a valid binary operator.
		/// </summary>
		/// <param name="s">The input string to parse.</param>
		/// <param name="result">If the string input is valid, this is the output
		/// binary operator that will be returned.</param>
		/// <returns>
		/// Returns <b>true</b> if the string input can be resolved to a valid
		/// <see cref="BinaryOperator"/>, or <c>false</c> otherwise
		/// </returns>
		/// <seealso cref="ToString"/>
		public static bool TryParse(string s, out BinaryOperator result) {
			return StringMap.TryGetValue(s, out result);
		}

		#region SimpleOperator

		class SimpleOperator : BinaryOperator {
			private readonly Func<DataObject, DataObject, DataObject> binaryFunc;

			public SimpleOperator(BinaryOperatorType operatorType, Func<DataObject, DataObject, DataObject> binaryFunc) 
				: base(operatorType) {
				this.binaryFunc = binaryFunc;
			}

			public override DataObject Evaluate(DataObject left, DataObject right, EvaluateContext context) {
				return binaryFunc(left, right);
			}
		}

		#endregion

		#region AnyOperator

		class AnyOperator : BinaryOperator {
			public AnyOperator(BinaryOperatorType operatorType) 
				: base(operatorType, OperatorSubType.Any) {
			}

			public override DataObject Evaluate(DataObject left, DataObject right, EvaluateContext context) {
				//if (right.Type is QueryType) {
				//	// The sub-query plan
				//	var plan = (IQueryPlanNode)right.Value;
				//	// Discover the correlated variables for this plan.
				//	var list = plan.DiscoverQueryReferences(1, new List<QueryReference>());

				//	if (list.Count > 0) {
				//		// Set the correlated variables from the IVariableResolver
				//		foreach (var variable in list) {
				//			variable.SetFromResolver(context.VariableResolver);
				//		}

				//		// Clear the cache in the context
				//		context.QueryContext.ClearCachedTables();
				//	}

				//	// Evaluate the plan,
				//	var t = plan.Evaluate(context.QueryContext);

				//	// The ANY operation
				//	var revPlainOp = base.AsSubQuery(OperatorSubType.None).Reverse();
				//	return t.ColumnMatches(0, revPlainOp, left);
				//}
				//if (right.Type is ArrayType) {
				//	var plainOp = AsSubQuery(OperatorSubType.None);
				//	var expList = (SqlArray)right.Value;
				//	// Assume there are no matches
				//	var retVal = DataObject.BooleanFalse;
				//	foreach (var exp in expList) {
				//		var exp_item = exp.Evaluate(context);
				//		// If null value, return null if there isn't otherwise a match found.
				//		if (exp_item.IsNull) {
				//			retVal = DataObject.BooleanNull;
				//		} else if (IsTrue(plainOp.Evaluate(left, exp_item, null))) {
				//			// If there is a match, the ANY set test is true
				//			return DataObject.BooleanTrue;
				//		}
				//	}

				//	// No matches, so return either false or NULL.  If there are no matches
				//	// and no nulls, return false.  If there are no matches and there are
				//	// nulls present, return null.
				//	return retVal;
				//}

				//throw new ApplicationException("Unknown right has side of ANY operator.");

				throw new NotImplementedException();
			}
		}

		#endregion

		#region AllOperator

		class AllOperator : BinaryOperator {
			public AllOperator(BinaryOperatorType operatorType)
				: base(operatorType, OperatorSubType.All) {
			}

			public override DataObject Evaluate(DataObject left, DataObject right, EvaluateContext context) {
				throw new NotImplementedException();
			}
		}

		#endregion
	}
}