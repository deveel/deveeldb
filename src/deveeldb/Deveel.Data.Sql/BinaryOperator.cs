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
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Deveel.Data.Sql {
	/// <summary>
	/// A conditional operator used to compute binary operations
	/// of comparison on a set.
	/// </summary>
	public abstract class BinaryOperator {
		private readonly OperatorSubType subType;

		private static readonly Dictionary<BinaryOperatorType, BinaryOperator> AnyMap;
		private static readonly Dictionary<BinaryOperatorType, BinaryOperator> AllMap;

		private static readonly Dictionary<string, BinaryOperator> StringMap; 

		public static readonly BinaryOperator Add = new SimpleOperator(BinaryOperatorType.Add, (left, right) => left.Add(right));
		public static readonly BinaryOperator Subtract = new SimpleOperator(BinaryOperatorType.Subtract, (left, right) => left.Subtract(right));
		public static readonly BinaryOperator Multiply = new SimpleOperator(BinaryOperatorType.Multiply, (left, right) => left.Multiply(right));
		public static readonly BinaryOperator Modulo = new SimpleOperator(BinaryOperatorType.Modulo, (left, right) => left.Modulus(right));
		public static readonly BinaryOperator Divide = new SimpleOperator(BinaryOperatorType.Divide, (left, right) => left.Divide(right));
		public static readonly BinaryOperator Equal = new SimpleOperator(BinaryOperatorType.Equal, (left, right) => left.IsEqualTo(right));
		public static readonly BinaryOperator NotEqual = new SimpleOperator(BinaryOperatorType.NotEqual, (left, right) => left.IsNotEqualTo(right));
		public static readonly BinaryOperator GreaterThan = new SimpleOperator(BinaryOperatorType.GreaterThan, (left, right) => left.IsGreaterThan(right));
		public static readonly BinaryOperator SmallerThan = new SimpleOperator(BinaryOperatorType.SmallerThan, (left, right) => left.IsSmallerThan(right));

		public static readonly BinaryOperator GreaterOrEqualThan =
			new SimpleOperator(BinaryOperatorType.GreaterOrEqualThan, (left, right) => left.IsGreterOrEqualThan(right));
		public static readonly BinaryOperator SmallerOrEqualThan =
			new SimpleOperator(BinaryOperatorType.SmallerOrEqualThan, (left, right) => left.IsSmallerOrEqualThan(right));

		public static readonly BinaryOperator Is = new SimpleOperator(BinaryOperatorType.Is, (left, right) => left.Is(right));
		public static readonly BinaryOperator IsNot = new SimpleOperator(BinaryOperatorType.IsNot, (left, right) => left.IsNot(right));
		public static readonly BinaryOperator Like = new SimpleOperator(BinaryOperatorType.Like, (left, right) => left.IsLike(right));
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
			this.subType = subType;
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
				{"<=", AnySmallerOrEqualThan},

				{"= all", AllEqual},
				{"<> all", AllNotEqual},
				{"!=", AllNotEqual},
				{"> all", AllGreaterThan},
				{"< all", AllSmallerThan},
				{">=", AllGreaterOrEqualThan},
				{"<=", AllSmallerOrEqualThan}
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
			get { return subType != OperatorSubType.None; }
		}

		public bool IsArithmetic {
			get {
				return OperatorType == BinaryOperatorType.Divide ||
				       OperatorType == BinaryOperatorType.Multiply ||
				       OperatorType == BinaryOperatorType.Modulo ||
				       OperatorType == BinaryOperatorType.Add ||
				       OperatorType == BinaryOperatorType.Subtract;
			}
		}

		public bool IsPattern {
			get {
				return OperatorType == BinaryOperatorType.Like ||
				       OperatorType == BinaryOperatorType.NotLike;
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
				if (subType == OperatorSubType.Any) {
					invType = OperatorSubType.All;
				} else if (subType == OperatorSubType.All) {
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

		public abstract DataObject Evaluate(DataObject left, DataObject right, EvaluateContext context);

		public DataObject Evaluate(DataObject left, DataObject right) {
			return Evaluate(left, right, null);
		}

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
			       subType == other.subType;
		}

		public override int GetHashCode() {
			return OperatorType.GetHashCode() ^ subType.GetHashCode();
		}

		public bool IsOfType(BinaryOperatorType operatorType) {
			return OperatorType == operatorType;
		}

		public override string ToString() {
			var pair = StringMap.FirstOrDefault(x => x.Value.OperatorType == OperatorType);

			var sb = new StringBuilder();
			sb.Append(pair.Key);

			if (subType == OperatorSubType.All) {
				sb.Append(" ALL");
			} else if (subType == OperatorSubType.Any) {
				sb.Append(" ANY");
			}

			return sb.ToString();
		}

		public static BinaryOperator Parse(string s) {
			BinaryOperator result;
			if (!TryParse(s, out result))
				throw new FormatException();

			return result;
		}

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